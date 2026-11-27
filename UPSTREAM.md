# Paimon read-only compatibility spike

```yaml
upstream_repo: https://github.com/apache/paimon-rust.git
upstream_sha: ebcff9f1279233d3afe47c466530acfb6e07158c
upstream_version: 0.6.0
target_datafusion_sha: 9f21b10e76b13f35c379376d09d51fbc5e8581f3
target_datafusion_version: 57.0.0-dev
```

## Workspace

The root workspace builds `crates/integrations/datafusion`, which depends on
`crates/paimon`. Cargo resolves every workspace member when it builds a
lockfile, including with `-p`. The core crate has its own manifest and lockfile
so it can build independently of the DataFusion preview. Invoke the core gates
from the repository root with `--manifest-path crates/paimon/Cargo.toml`.

The workspace excludes `crates/paimon-rest-server`, `crates/integration_tests`,
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

All upstream source files are retained. SQLContext, DDL/DML, Spark
compatibility, stored procedures, `read_files`, system and object tables,
external table engine dispatch, persistent view planning and lateral vector
search are gated behind the default-off `dml` feature. The 63 files restored
from upstream are byte-for-byte identical to the baseline. The default build
uses the modular range partitioning and execution memory accounting in the
physical plan files.

The gate preserves upstream code for rebases and future write support.
Enabling `dml` requires a separate migration of the legacy DataFusion APIs
and dependencies to the pinned modular build.

## Local patches

- Use Arrow and Parquet 60.2.0. Pin 13 DataFusion subcrates to `=57.0.0-dev`
  with direct defaults disabled. Enable `sql` on `datafusion-expr` for the
  VECTOR type hook and on `datafusion-functions-nested` for the matching
  planner API. The integration enables filesystem and memory storage on
  Paimon.
- Put ORC and Mosaic behind `format-orc` and `format-mosaic`. These features
  and Vortex are disabled in the validation build. Their upstream dependencies
  still pin Arrow 59 and can appear in the lockfile as inactive packages.
  Enabling the ORC and Mosaic features currently fails at Arrow 59/60 type
  boundaries. These formats require an upstream dependency upgrade.
- Run format-independent file-index, PK vector and full-text tests with small
  Parquet fixtures. Format-specific tests follow the corresponding feature
  flags.
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

## Reproduce the read-only gates

Run from the Paimon repository root in Bash. Set `DF_ROOT` to the pinned
DataFusion checkout. The same `--config` patches can be supplied by a Cargo
workspace that consumes `paimon-datafusion`.

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export DF_ROOT=/path/to/datafusion
test "$(git -C "$DF_ROOT" rev-parse HEAD)" = 9f21b10e76b13f35c379376d09d51fbc5e8581f3
test -z "$(git -C "$DF_ROOT" status --porcelain)"
unset PAIMON_TEST_WAREHOUSE

df_patch=()
for crate in common expr session catalog datasource datasource-parquet execution \
    physical-plan physical-expr physical-expr-adapter functions-nested \
    physical-expr-common functions; do
    df_patch+=(--config "patch.crates-io.datafusion-$crate.path=\"$DF_ROOT/datafusion/$crate\"")
done
core_args=(--manifest-path crates/paimon/Cargo.toml -p paimon --no-default-features \
    --features storage-memory,storage-fs)

cargo fmt --all -- --check
cargo clippy --locked "${core_args[@]}" --all-targets -- -D warnings
cargo test --locked "${core_args[@]}" --lib
cargo clippy --locked "${df_patch[@]}" -p paimon-datafusion --all-targets -- -D warnings
cargo test --locked "${df_patch[@]}" -p paimon-datafusion --lib
bash crates/integrations/datafusion/check-readonly-dependencies.sh "${df_patch[@]}"
```

The root `Cargo.lock` resolves with `--locked` against the pinned DataFusion
checkout. Cargo records the patched packages without their checkout paths.
The core lockfile resolves independently and needs no patches. The Git push
URL is `no-push-upstream`.

## Validation

Rust and Cargo 1.97.1 passed formatting and both strict `--all-targets`
Clippy runs. The library suites passed 5,010 core tests with eight manual
benchmarks ignored and 118 integration tests. Tests used local fixtures and
loopback-only networking for the existing HTTP mocks.

The core suite covers Parquet reads from JSON-restored `DataSplit` values,
descriptor range and lazy read behavior, and RRF and weighted ranking. The
integration suite exercises independent indexed vector and full-text reads,
hybrid reads, and descriptor scans with a missing payload file.

Both active dependency graphs contain Arrow and Parquet 60.2.0. The core
graph has zero DataFusion packages, and the integration graph resolves only
DataFusion subcrates at 57.0.0-dev. ORC, Mosaic and Vortex are inactive in
both graphs.
