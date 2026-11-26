// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use datafusion_catalog::TableProvider;
use datafusion_catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{internal_err, project_schema, Statistics};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::TableType;
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, StatisticsArgs,
};
use datafusion_session::Session;
use futures::{stream, TryStreamExt};
use paimon::catalog::Catalog;
use paimon::spec::{
    BigIntType, CoreOptions, DataField, DataType, Predicate, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::analyze_filters;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{datafusion_read_fields, PaimonTableProvider};
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};
use crate::table_loader::load_data_table_for_read;
use crate::DynamicOptions;

const FUNCTION_NAME: &str = "vector_search";

/// Table function implementation for `vector_search`.
pub struct VectorSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
    dynamic_options: DynamicOptions,
}

impl Debug for VectorSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl VectorSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self::new_with_dynamic_options(catalog, default_database, Default::default())
    }

    /// Creates a table function that applies the host session’s read options.
    pub fn new_with_dynamic_options(
        catalog: Arc<dyn Catalog>,
        default_database: &str,
        dynamic_options: DynamicOptions,
    ) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
            dynamic_options,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub(crate) enum VectorQuery {
    Single(Vec<f32>),
    Matrix(Vec<Vec<f32>>),
}

impl VectorQuery {
    pub(crate) fn parse(json: &str) -> Result<Self, serde_json::Error> {
        // Parse directly into floats. Untagged enum buffering turns decimal
        // numbers into a private map with serde_json's arbitrary_precision.
        serde_json::from_str::<Vec<f32>>(json)
            .map(Self::Single)
            .or_else(|_| serde_json::from_str::<Vec<Vec<f32>>>(json).map(Self::Matrix))
    }
}

impl TableFunctionImpl for VectorSearchFunction {
    fn call_with_args(&self, args: TableFunctionArgs<'_, '_>) -> DFResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if args.len() != 4 {
            return Err(datafusion_common::DataFusionError::Plan(
                "vector_search requires 4 arguments: (table_name, column_name, query_vector_json, limit)".to_string(),
            ));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let column_name = extract_string_literal(FUNCTION_NAME, &args[1], "column_name")?;
        let limit = extract_int_literal(FUNCTION_NAME, &args[3], "limit")?;

        if limit <= 0 {
            return Err(DataFusionError::Plan(
                "vector_search: limit must be positive".to_string(),
            ));
        }

        let identifier =
            parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = self.dynamic_options.read().unwrap().clone();
        let table = block_on_with_runtime(
            async move {
                let table = load_data_table_for_read(&catalog, &identifier, FUNCTION_NAME).await?;
                let table = if dynamic_options.is_empty() {
                    table
                } else {
                    table
                        .copy_with_time_travel(dynamic_options)
                        .await
                        .map_err(to_datafusion_error)?
                };
                Ok::<_, DataFusionError>(table)
            },
            "vector_search: catalog access thread panicked",
        )?;

        let inner = PaimonTableProvider::try_new(table)?;
        let query_vector_json =
            extract_string_literal(FUNCTION_NAME, &args[2], "query_vector_json")?;
        let query_vector = VectorQuery::parse(&query_vector_json).map_err(|e| {
            DataFusionError::Plan(format!(
                "vector_search: query_vector_json must be a JSON vector or matrix of floats, got '{}': {}",
                query_vector_json, e
            ))
        })?;

        if matches!(&query_vector, VectorQuery::Single(vector) if vector.is_empty()) {
            return Err(DataFusionError::Plan(
                "vector_search: query vector cannot be empty".to_string(),
            ));
        }

        Ok(Arc::new(VectorSearchTableProvider {
            inner,
            column_name,
            query_vector,
            limit: limit as usize,
        }))
    }
}

#[derive(Debug)]
struct VectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector: VectorQuery,
    limit: usize,
}

#[async_trait]
impl TableProvider for VectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema(), projection)?;
        let filter_analysis = analyze_filters(filters, self.inner.table().schema().fields(), true);
        if filter_analysis.requires_residual {
            return Err(DataFusionError::Plan(
                "vector_search cannot apply a partially translated scalar pre-filter".to_string(),
            ));
        }
        let pushed_predicate = filter_analysis.pushed_predicate;

        // An outer `LIMIT 0` needs no rows.
        if limit == Some(0) {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // The search runs with the table function's own top-k (`self.limit`) so the ANN
        // recall/search width is unchanged; the outer DataFusion `limit` only truncates
        // the already-ranked result before any rows are read (so a large top-k with a
        // small outer LIMIT doesn't read/materialize everything). All of this — search,
        // read and rank-order gather — runs at execution time in the exec's stream, so
        // planning / EXPLAIN stays cheap and the work is driven by the TaskContext.
        let mut exec = VectorSearchExec::new(
            self.inner.table().clone(),
            self.column_name.clone(),
            self.query_vector.clone(),
            self.limit,
            limit,
            projection.map(<[usize]>::to_vec),
            projected_schema,
        );
        if let Some(filter) = pushed_predicate {
            exec = exec.with_filter(filter);
        }
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let fields = self.inner.table().schema().fields();
        Ok(filters
            .iter()
            .map(|filter| {
                let analysis = analyze_filters(std::slice::from_ref(*filter), fields, true);
                if analysis.pushed_predicate.is_some() && !analysis.requires_residual {
                    // Keep DataFusion's residual filter as a correctness backstop
                    // while using the same predicate before vector Top-K.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// Execution-time plan for `vector_search`: runs the ANN search, reads the matching
/// rows, and gathers them into best-first relevance order when its stream is polled,
/// so planning (and `EXPLAIN`) stays cheap and the work runs under DataFusion's
/// `TaskContext`.
#[derive(Debug, Clone)]
struct VectorSearchExec {
    table: Table,
    column_name: String,
    query_vector: VectorQuery,
    /// The table function's own top-k — drives the ANN search width; never reduced.
    search_limit: usize,
    /// The outer DataFusion `LIMIT`, applied by truncating the ranked result.
    output_limit: Option<usize>,
    projection: Option<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    filter: Option<Predicate>,
    plan_properties: Arc<PlanProperties>,
}

impl VectorSearchExec {
    fn new(
        table: Table,
        column_name: String,
        query_vector: VectorQuery,
        search_limit: usize,
        output_limit: Option<usize>,
        projection: Option<Vec<usize>>,
        output_schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            column_name,
            query_vector,
            search_limit,
            output_limit,
            projection,
            output_schema,
            filter: None,
            plan_properties,
        }
    }

    fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(filter);
        self
    }

    async fn compute_batch(&self) -> DFResult<RecordBatch> {
        let snapshot = await_with_runtime(self.table.resolve_read_snapshot())
            .await
            .map_err(to_datafusion_error)?;
        let search_table = self.table.copy_with_pinned_snapshot(snapshot.as_ref());

        // Best-first row-ids from the index, searched at the full top-k so the ANN
        // recall is unchanged (data-evolution / global-index path; PK-vector tables are
        // unsupported here, as before).
        let mut search_result = await_with_runtime(async {
            let mut builder = search_table.new_vector_search_builder();
            builder
                .with_vector_column(&self.column_name)
                .with_limit(self.search_limit);
            match &self.query_vector {
                VectorQuery::Single(vector) => builder.with_query_vector(vector.clone()),
                VectorQuery::Matrix(vectors) => builder.with_multi_vector(vectors.clone()),
            };
            if let Some(filter) = &self.filter {
                builder.with_filter(filter.clone());
            }
            builder
                .execute()
                .await
                .map_err(to_datafusion_error)?
                .into_row_ids()
                .map_err(to_datafusion_error)
        })
        .await?;

        if search_result.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Apply the outer LIMIT by truncating the ranked result *before* reading, so a
        // large top-k with a small outer LIMIT only reads/materializes the rows it can
        // return (the search itself is unaffected).
        if let Some(n) = self.output_limit {
            if search_result.row_ids.len() > n {
                search_result.row_ids.truncate(n);
                search_result.scores.truncate(n);
            }
        }

        // Read the projected columns (+ internal `_ROW_ID`); the row-range scan yields
        // file order, realigned to relevance rank below.
        let read_fields = projected_read_fields(&search_table, self.projection.as_deref())?;
        let row_ranges = search_result.to_row_ranges().map_err(to_datafusion_error)?;
        let batches = await_with_runtime(async {
            let mut read_builder = search_table.new_read_builder();
            read_builder
                .with_read_type(read_fields)
                .with_row_ranges(row_ranges);
            let scan = read_builder.new_scan();
            let plan = scan.plan().await.map_err(to_datafusion_error)?;
            let table_read = read_builder.new_read().map_err(to_datafusion_error)?;
            let mut stream = table_read
                .to_arrow(plan.splits())
                .map_err(to_datafusion_error)?;
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = stream.try_next().await.map_err(to_datafusion_error)? {
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        })
        .await?;

        // Realign file-order rows to best-first rank and drop `_ROW_ID`.
        gather_rows_by_rank(&batches, &search_result.row_ids, &self.output_schema)
    }
}

impl DisplayAs for VectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VectorSearchExec: column={}, search_limit={}, output_limit={:?}, filter={:?}",
            self.column_name, self.search_limit, self.output_limit, self.filter
        )
    }
}

impl ExecutionPlan for VectorSearchExec {
    fn name(&self) -> &str {
        "VectorSearchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("VectorSearchExec is a leaf and takes no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "VectorSearchExec has a single partition, got partition {partition}"
            );
        }
        let exec = self.clone();
        let stream = stream::once(async move { exec.compute_batch().await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            Box::pin(stream),
        )))
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        }))
    }
}

/// Projected user columns (+ internal `_ROW_ID`, needed to realign rows to rank).
/// Errors if the table has no row tracking, since results then can't be ordered.
fn projected_read_fields(
    table: &paimon::table::Table,
    projection: Option<&[usize]>,
) -> DFResult<Vec<DataField>> {
    let base_fields = datafusion_read_fields(table);
    let mut read_fields: Vec<DataField> = match projection {
        Some(indices) => indices.iter().map(|&i| base_fields[i].clone()).collect(),
        None => base_fields,
    };
    if !read_fields
        .iter()
        .any(|field| field.name() == ROW_ID_FIELD_NAME)
    {
        if !CoreOptions::new(table.schema().options()).row_tracking_enabled() {
            return Err(DataFusionError::Plan(
                "vector_search: cannot order results by relevance because _ROW_ID is not available"
                    .to_string(),
            ));
        }
        read_fields.push(DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::with_nullable(true)),
        ));
    }
    Ok(read_fields)
}

/// Gather the file-order `batches` into `ranked_row_ids` order (rank == slice index),
/// producing `output_schema` (which excludes `_ROW_ID`). A permutation driven by the
/// index's existing ranking, not a re-sort.
fn gather_rows_by_rank(
    batches: &[RecordBatch],
    ranked_row_ids: &[u64],
    output_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    let input_schema = batches.first().map(|batch| batch.schema()).ok_or_else(|| {
        DataFusionError::Internal("vector_search: no rows materialized".to_string())
    })?;
    let combined = arrow_select::concat::concat_batches(&input_schema, batches)
        .map_err(DataFusionError::from)?;

    let row_id_index = combined.schema().index_of(ROW_ID_FIELD_NAME).map_err(|_| {
        DataFusionError::Internal(format!(
            "vector_search: materialized rows are missing the {ROW_ID_FIELD_NAME} column"
        ))
    })?;
    let row_ids = combined
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("vector_search: {ROW_ID_FIELD_NAME} must be Int64"))
        })?;

    // Map global row id -> physical position in the materialized batch.
    let mut position_of: HashMap<i64, u32> = HashMap::with_capacity(combined.num_rows());
    for row in 0..combined.num_rows() {
        if !row_ids.is_null(row) {
            position_of.insert(row_ids.value(row), row as u32);
        }
    }

    // Emit in rank order; extra scanned rows are ignored, but a missing scored id
    // fails loud rather than silently shrinking the top-k.
    let mut take_indices: Vec<u32> = Vec::with_capacity(ranked_row_ids.len());
    for &row_id in ranked_row_ids {
        let position = position_of.get(&(row_id as i64)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_search: scored row id {row_id} was not materialized; \
                 cannot return the requested top-k"
            ))
        })?;
        take_indices.push(*position);
    }
    let take_indices = UInt32Array::from(take_indices);
    let row_count = take_indices.len();

    let columns = output_schema
        .fields()
        .iter()
        .map(|field| -> DFResult<ArrayRef> {
            let index = combined.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "vector_search: materialized rows are missing expected column '{}'",
                    field.name()
                ))
            })?;
            let taken =
                arrow_select::take::take(combined.column(index).as_ref(), &take_indices, None)
                    .map_err(DataFusionError::from)?;
            // The Paimon read keeps its own arrow types (e.g. `Utf8`), but the provider
            // schema may differ (e.g. DataFusion's `Utf8View`); cast to match, as the
            // normal scan path does via `to_datafusion_batch`.
            if taken.data_type() == field.data_type() {
                Ok(taken)
            } else {
                cast(taken.as_ref(), field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Preserve the row count for a zero-column projection (e.g. `COUNT(*)`).
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(Arc::clone(output_schema), columns, &options)
        .map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float32Builder, Int32Array, LargeBinaryArray, ListBuilder, StringArray,
    };
    use datafusion_expr::lit;
    use paimon::catalog::Identifier;
    use paimon::spec::{
        ArrayType, BlobDescriptor, BlobType, DataType, FloatType, IntType, Schema as PaimonSchema,
        VarCharType,
    };
    use paimon::{CatalogOptions, FileSystemCatalog, Options};

    #[test]
    fn vector_query_accepts_decimal_vectors_and_matrices() {
        assert!(matches!(
            VectorQuery::parse("[1.0, -2.5]").unwrap(),
            VectorQuery::Single(vector) if vector == [1.0, -2.5]
        ));
        assert!(matches!(
            VectorQuery::parse("[[1.0, -2.5], [0.0, 3e-1]]").unwrap(),
            VectorQuery::Matrix(vectors) if vectors == [vec![1.0, -2.5], vec![0.0, 0.3]]
        ));
        assert!(VectorQuery::parse("[1.0, [2.5]]").is_err());
    }

    #[test]
    fn indexed_search_and_descriptor_scan_work_without_entered_runtime() {
        use arrow::datatypes::{DataType as ArrowDataType, Field};
        use datafusion_physical_plan::{collect, StatisticsContext};

        let warehouse = tempfile::tempdir().unwrap();
        let payload = warehouse.path().join("missing-payload.bin");
        let descriptor = BlobDescriptor::new(format!("file://{}", payload.display()), 0, 7);
        let descriptor_bytes = descriptor.serialize();
        let (catalog, identifier, snapshot_id) = crate::runtime::runtime().block_on(async {
            let mut options = Options::new();
            options.set(
                CatalogOptions::WAREHOUSE,
                warehouse.path().to_str().unwrap(),
            );
            let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
            catalog
                .create_database("db", false, Default::default())
                .await
                .unwrap();
            let identifier = Identifier::new("db", "vector_blob");
            let schema = PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(
                    "embedding",
                    DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
                )
                .column("picture", DataType::Blob(BlobType::new()))
                .column("content", DataType::VarChar(VarCharType::string_type()))
                .option("blob-descriptor-field", "picture")
                .option("bucket", "-1")
                .option("file.format", "parquet")
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true")
                .option("global-index.enabled", "true")
                .option("global-index.search-mode", "full")
                .build()
                .unwrap();
            catalog
                .create_table(&identifier, schema, false)
                .await
                .unwrap();
            let table = catalog.get_table(&identifier).await.unwrap();
            let builder = table.new_write_builder();
            let mut values = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(
                Field::new("element", ArrowDataType::Float32, true),
            ));
            values.values().append_slice(&[1.0, 0.0]);
            values.append(true);
            let batch = RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![7])) as ArrayRef),
                ("embedding", Arc::new(values.finish()) as ArrayRef),
                (
                    "picture",
                    Arc::new(LargeBinaryArray::from(vec![descriptor_bytes.as_slice()])) as ArrayRef,
                ),
                (
                    "content",
                    Arc::new(StringArray::from(vec!["paimon lake"])) as ArrayRef,
                ),
            ])
            .unwrap();
            let mut writer = builder.new_write().unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let mut vector_index = table.new_global_index_build_builder();
            vector_index
                .with_index_type("ivf-flat")
                .with_index_column("embedding")
                .with_options(std::collections::HashMap::from([
                    ("ivf-flat.dimension".into(), "2".into()),
                    ("ivf-flat.nlist".into(), "1".into()),
                    ("ivf-flat.distance.metric".into(), "l2".into()),
                ]));
            assert!(vector_index.execute().await.unwrap() > 0);
            let mut text_index = table.new_global_index_build_builder();
            text_index
                .with_index_type("full-text")
                .with_index_column("content");
            assert!(text_index.execute().await.unwrap() > 0);
            let snapshot_id = table.resolve_read_snapshot().await.unwrap().unwrap().id();
            (catalog, identifier, snapshot_id)
        });

        assert!(tokio::runtime::Handle::try_current().is_err());
        let dynamic_options = Arc::new(std::sync::RwLock::new(std::collections::HashMap::from([
            ("blob-as-descriptor".to_string(), "true".to_string()),
            (
                paimon::spec::SCAN_VERSION_OPTION.to_string(),
                snapshot_id.to_string(),
            ),
        ])));
        let catalog: Arc<dyn Catalog> = catalog;
        let session = crate::test_session::TestSession::new(Default::default());
        let vector = VectorSearchFunction::new_with_dynamic_options(
            Arc::clone(&catalog),
            "db",
            Arc::clone(&dynamic_options),
        );
        let vector_provider = vector
            .call_with_args(TableFunctionArgs::new(
                &[
                    lit(identifier.object()),
                    lit("embedding"),
                    lit("[1.0, 0.0]"),
                    lit(1_i64),
                ],
                &session,
            ))
            .unwrap();
        assert_eq!(
            vector_provider
                .downcast_ref::<VectorSearchTableProvider>()
                .unwrap()
                .inner
                .table()
                .travel_snapshot()
                .map(|snapshot| snapshot.id()),
            Some(snapshot_id)
        );

        let full_text = crate::FullTextSearchFunction::new_with_dynamic_options(
            Arc::clone(&catalog),
            "db",
            Arc::clone(&dynamic_options),
        );
        let full_text_provider = full_text
            .call_with_args(TableFunctionArgs::new(
                &[
                    lit(identifier.object()),
                    lit("content"),
                    lit("paimon"),
                    lit(1_i64),
                ],
                &session,
            ))
            .unwrap();

        use datafusion_functions::core::expr_fn::named_struct;
        use datafusion_functions_nested::expr_fn::make_array;
        let hybrid = crate::HybridSearchFunction::new_with_dynamic_options(
            Arc::clone(&catalog),
            "db",
            Arc::clone(&dynamic_options),
        );
        let hybrid_provider = hybrid
            .call_with_args(TableFunctionArgs::new(
                &[
                    lit(identifier.object()),
                    make_array(vec![named_struct(vec![
                        lit("field"),
                        lit("embedding"),
                        lit("query_vector"),
                        make_array(vec![lit(1.0_f32), lit(0.0_f32)]),
                    ])]),
                    make_array(vec![named_struct(vec![
                        lit("column"),
                        lit("content"),
                        lit("query"),
                        lit(r#"{"match":{"query":"paimon"}}"#),
                    ])]),
                    lit(1_i64),
                ],
                &session,
            ))
            .unwrap();

        let catalog =
            crate::PaimonCatalogProvider::new(catalog, dynamic_options, Default::default());
        use datafusion_catalog::CatalogProvider;
        let schema = catalog.schema("db").unwrap();
        assert!(schema.table_exist(identifier.object()));
        futures::executor::block_on(async {
            let table_provider = schema.table(identifier.object()).await.unwrap().unwrap();
            for provider in [
                table_provider,
                vector_provider,
                full_text_provider,
                hybrid_provider,
            ] {
                let plan = provider
                    .scan(&session, Some(&[0, 2]), &[], None)
                    .await
                    .unwrap();
                let statistics = StatisticsContext::new()
                    .compute(plan.as_ref(), &StatisticsArgs::new())
                    .unwrap();
                assert_eq!(
                    statistics.column_statistics.len(),
                    plan.schema().fields().len()
                );
                let batches = collect(plan, session.task_ctx()).await.unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                let id = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id.values(), &[7]);
                let blob = batches[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                assert_eq!(
                    BlobDescriptor::deserialize(blob.value(0)).unwrap(),
                    descriptor
                );
            }
        });
        assert!(!payload.exists());
    }
}
