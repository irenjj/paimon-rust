# Paimon read-only compatibility spike

```yaml
upstream_repo: https://github.com/apache/paimon-rust.git
upstream_sha: ebcff9f1279233d3afe47c466530acfb6e07158c
upstream_version: 0.6.0
target_datafusion_path: /home/renjj/.paseo/worktrees/0k1e574a/df-lake-plan
target_datafusion_sha: 831a58bc15ba1371793585f05f6c7c6c4b8c1542
target_datafusion_version: 57.0.0-dev
```

## Workspace

The workspace builds `crates/paimon` and `crates/integrations/datafusion`.
It excludes `crates/paimon-rest-server`, `crates/integration_tests`,
`bindings/c`, `bindings/python`, and `benchmarks/tpcds`. The integration's
legacy SQLContext integration suites and examples are excluded from Cargo
target discovery. Library tests use memory, temporary directories, and an
embedded warehouse fixture unpacked into a temporary directory.

The integration compiles these modules alongside its `lib.rs` entry point:

```text
error.rs
runtime.rs
table_loader.rs
table_function_args.rs
filter_pushdown.rs
catalog.rs
table/mod.rs
physical_plan/{mod,scan,search_score}.rs
vector_search.rs
full_text_search.rs
hybrid_search.rs
blob_view.rs
blob_descriptor_functions.rs
blob_reader.rs
vector_type.rs
```

The removed modules covered SQLContext, DDL/DML, Spark compatibility,
stored procedures, read_files, system and object tables, external table engine
dispatch, persistent view planning and lateral vector search. Scan range
partitioning and execution memory accounting live in the retained physical plan
files.

## Local patches

- Use Arrow and Parquet 60.2.0, with DataFusion session, catalog, expression,
  execution, datasource and physical-plan crates from the pinned worktree.
  The active integration uses filesystem and memory storage features.
- Put ORC and Mosaic behind `format-orc` and `format-mosaic`. These features
  and Vortex are disabled in the validation build. Their upstream dependencies
  still pin Arrow 59 and can appear in the lockfile as inactive packages.
  Enabling the ORC and Mosaic features currently fails at Arrow 59/60 type
  boundaries. These formats require an upstream dependency upgrade.
- Adapt Arrow metadata, fixed-size list offsets, offset buffers and Parquet 60
  page index access. Keep row-group and page pruning conservative when index
  information is missing. Enable the LZO compression feature explicitly for
  the existing B-tree codec.
- Use slice projections, `TableFunctionImpl::call_with_args` and
  `ExecutionPlan::statistics_from_inputs`. Keep one statistics entry per
  projected field. Enter the fallback Tokio runtime while polling filesystem
  streams outside Tokio. Preserve the required legacy trait implementation of
  `with_new_children` as a shim to `replace_children`.
- Expose the read-only catalog and table providers directly. Hosts register
  `VectorSearchFunction`, `FullTextSearchFunction` and `HybridSearchFunction`
  under `vector_search`, `full_text_search` and `hybrid_search`. Hosts supply
  `named_struct` and `make_array` or `array` when planning hybrid route
  expressions. The three search functions and the catalog can share
  `DynamicOptions` for snapshot selection and `blob-as-descriptor`.
- Expose `blob_view` and `blob_descriptor_functions` as scalar UDF
  constructors, `BlobReaderRegistry` for descriptor routing, and
  `PaimonTypePlanner` plus the VECTOR parser entry points for host planning.
  The type planner handles the standalone VECTOR type.

The Git push URL is `no-push-upstream`. The path dependencies require the
specified checkout on the local machine.
