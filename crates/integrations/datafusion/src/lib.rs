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

//! Read-only Apache Paimon providers and functions for DataFusion.

mod blob_descriptor_functions;
mod blob_reader;
mod blob_view;
mod catalog;
mod error;
mod filter_pushdown;
mod full_text_search;
mod hybrid_search;
mod physical_plan;
pub mod runtime;
mod table;
mod table_function_args;
mod table_loader;
mod vector_search;
mod vector_type;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Read options shared by a host session and its Paimon providers.
pub type DynamicOptions = Arc<RwLock<HashMap<String, String>>>;

pub use blob_descriptor_functions::blob_descriptor_functions;
pub use blob_reader::BlobReaderRegistry;
pub use blob_view::blob_view;
pub use catalog::{PaimonCatalogProvider, PaimonSchemaProvider};
pub use error::to_datafusion_error;
pub use full_text_search::FullTextSearchFunction;
pub use hybrid_search::HybridSearchFunction;
pub use physical_plan::PaimonTableScan;
pub use table::PaimonTableProvider;
pub use vector_search::VectorSearchFunction;
pub use vector_type::{parse_expr, parse_sql, PaimonTypePlanner};

#[cfg(test)]
#[path = "../testdata/warehouse/mod.rs"]
mod golden_warehouse;

#[cfg(test)]
mod test_session {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion_catalog::{CatalogProviderList, MemoryCatalogProviderList};
    use datafusion_common::{config::TableOptions, DFSchema, Result};
    use datafusion_execution::{config::SessionConfig, runtime_env::RuntimeEnv, TaskContext};
    use datafusion_expr::{
        execution_props::ExecutionProps,
        registry::{ExtensionTypeRegistryRef, MemoryExtensionTypeRegistry},
        AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
    };
    use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion_session::Session;

    pub(crate) struct TestSession {
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        execution_props: ExecutionProps,
        table_options: TableOptions,
        catalogs: Arc<MemoryCatalogProviderList>,
        extension_types: ExtensionTypeRegistryRef,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
    }

    impl TestSession {
        pub(crate) fn new(config: SessionConfig) -> Self {
            Self {
                config,
                runtime: Arc::new(RuntimeEnv::default()),
                execution_props: ExecutionProps::new(),
                table_options: TableOptions::default(),
                catalogs: Arc::new(MemoryCatalogProviderList::default()),
                extension_types: Arc::new(MemoryExtensionTypeRegistry::default()),
                scalar_functions: HashMap::new(),
                higher_order_functions: HashMap::new(),
                aggregate_functions: HashMap::new(),
                window_functions: HashMap::new(),
            }
        }
    }

    #[async_trait]
    impl Session for TestSession {
        fn session_id(&self) -> &str {
            "paimon-readonly-test"
        }

        fn config(&self) -> &SessionConfig {
            &self.config
        }

        fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
            self.catalogs.clone()
        }

        async fn create_physical_plan(&self, _: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
            datafusion_common::not_impl_err!("test session has no logical plan compiler")
        }

        fn create_physical_expr(&self, _: Expr, _: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
            datafusion_common::not_impl_err!("test session has no expression compiler")
        }

        fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
            &self.scalar_functions
        }

        fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
            &self.higher_order_functions
        }

        fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
            &self.aggregate_functions
        }

        fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
            &self.window_functions
        }

        fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
            &self.extension_types
        }

        fn runtime_env(&self) -> &Arc<RuntimeEnv> {
            &self.runtime
        }

        fn execution_props(&self) -> &ExecutionProps {
            &self.execution_props
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_options(&self) -> &TableOptions {
            &self.table_options
        }

        fn table_options_mut(&mut self) -> &mut TableOptions {
            &mut self.table_options
        }

        fn task_ctx(&self) -> Arc<TaskContext> {
            Arc::new(TaskContext::from(self as &dyn Session))
        }
    }
}
