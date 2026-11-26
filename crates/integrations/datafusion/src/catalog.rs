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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion_common::Result as DFResult;
use paimon::catalog::{Catalog, Identifier};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{build_table_definition, PaimonTableProvider};
use crate::table_loader::loaded_table_for_read;
use crate::{BlobReaderRegistry, DynamicOptions};

/// Exposes native Paimon tables through a read-only DataFusion catalog.
pub struct PaimonCatalogProvider {
    catalog: Arc<dyn Catalog>,
    dynamic_options: DynamicOptions,
    blob_reader_registry: BlobReaderRegistry,
    schema_force_view_types: bool,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl PaimonCatalogProvider {
    /// Creates a provider with shared read options and a descriptor reader registry.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        dynamic_options: DynamicOptions,
        blob_reader_registry: BlobReaderRegistry,
    ) -> Self {
        if let Some(filesystem) = catalog
            .as_any()
            .and_then(|catalog| catalog.downcast_ref::<paimon::FileSystemCatalog>())
        {
            blob_reader_registry
                .register_catalog_location(filesystem.warehouse(), filesystem.file_io().clone());
        }
        Self {
            catalog,
            dynamic_options,
            blob_reader_registry,
            schema_force_view_types: true,
        }
    }

    /// Selects whether scan schemas use Arrow view types where supported.
    pub fn with_schema_force_view_types(mut self, enabled: bool) -> Self {
        self.schema_force_view_types = enabled;
        self
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        block_on_with_runtime(
            async move {
                catalog.list_databases().await.unwrap_or_else(|error| {
                    log::error!("failed to list Paimon databases: {error}");
                    vec![]
                })
            },
            "paimon catalog access thread panicked",
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let catalog = Arc::clone(&self.catalog);
        let name = name.to_owned();
        let database = name.clone();
        let exists = block_on_with_runtime(
            async move { catalog.get_database(&database).await },
            "paimon catalog access thread panicked",
        );
        match exists {
            Ok(_) => Some(Arc::new(PaimonSchemaProvider {
                catalog: Arc::clone(&self.catalog),
                database: name,
                dynamic_options: Arc::clone(&self.dynamic_options),
                blob_reader_registry: self.blob_reader_registry.clone(),
                schema_force_view_types: self.schema_force_view_types,
            })),
            Err(paimon::Error::DatabaseNotExist { .. }) => None,
            Err(error) => {
                log::error!("failed to load Paimon database '{name}': {error}");
                None
            }
        }
    }
}

/// Exposes tables in one Paimon database and loads them on demand.
#[derive(Clone)]
pub struct PaimonSchemaProvider {
    catalog: Arc<dyn Catalog>,
    database: String,
    dynamic_options: DynamicOptions,
    blob_reader_registry: BlobReaderRegistry,
    schema_force_view_types: bool,
}

impl Debug for PaimonSchemaProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonSchemaProvider")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        block_on_with_runtime(
            async move {
                catalog
                    .list_tables(&database)
                    .await
                    .unwrap_or_else(|error| {
                        log::error!("failed to list Paimon tables in '{database}': {error}");
                        vec![]
                    })
            },
            "paimon catalog access thread panicked",
        )
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let identifier = Identifier::new(&self.database, name);
        let parsed = identifier
            .parsed_object_name()
            .map_err(to_datafusion_error)?;
        if parsed.system_table().is_some() {
            return Ok(None);
        }
        let base = Identifier::new(&self.database, parsed.table());
        let options = self.dynamic_options.read().unwrap().clone();
        await_with_runtime(async {
            let loaded = match self.catalog.load_table(&base).await {
                Ok(loaded) => loaded,
                Err(paimon::Error::TableNotExist { .. }) => return Ok(None),
                Err(error) => return Err(to_datafusion_error(error)),
            };
            let (table, _, _) = loaded_table_for_read(loaded, &identifier).await?;
            let definition = build_table_definition(&table).ok();
            let table = if options.is_empty() {
                table
            } else {
                table
                    .copy_with_time_travel(options)
                    .await
                    .map_err(to_datafusion_error)?
            };
            let provider = PaimonTableProvider::try_new_with_blob_reader_registry_and_definition(
                table,
                self.blob_reader_registry.clone(),
                definition,
            )?
            .with_schema_force_view_types(self.schema_force_view_types)?;
            Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
        })
        .await
    }

    fn table_exist(&self, name: &str) -> bool {
        let provider = self.clone();
        let name = name.to_owned();
        block_on_with_runtime(
            async move { provider.table(&name).await },
            "paimon catalog access thread panicked",
        )
        .is_ok_and(|table| table.is_some())
    }
}
