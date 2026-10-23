# Contract-Driven Medallion Platform

A reusable, contract-driven medallion architecture built on **Dagster**, **PySpark**, and **Delta Lake**.
Data contracts written in YAML generate bronze/silver/gold assets, Dagster asset checks, Spark schemas,
validation gates, and synthetic test data — with no domain-specific framework code.

```
YAML contracts + defs.yaml
        │
        ▼
 MedallionFactory (Dagster component)
        │
        ├── bronze asset ── REST fetch ──► raw Delta table (+ ingestion metadata)
        ├── silver asset ── transformations ── contract validation ──► Delta table
        ├── gold asset   ── transformations ── contract validation ──► Delta table
        └── asset checks ── contract-derived (nullability, PK, FK, ranges, ...)
```

## Repository layout

```
dagster_pipelines/
├── packages/data-platform-common/        Platform library + MedallionFactory
│   └── src/data_platform_common/
│       ├── contracts/                    Pydantic contract models, loader, registry
│       ├── synthetic/                    Deterministic generator, regex engine, graph, batch
│       ├── validation/                   Record-level (pydantic) and frame-level (Spark) checks
│       ├── resources/                    REST, Spark, lakehouse, Trino, metadata resources
│       ├── io/                           Delta writer + DeltaLakeIOManager
│       ├── medallion/                    Pipeline model and bronze/silver/gold asset builders
│       ├── components/                   MedallionFactory component
│       ├── checks.py                     CheckSpec -> Dagster asset checks
│       ├── partitions.py                 Typed partition config + key propagation
│       ├── imports.py                    Trusted dotted-import resolution
│       └── transformations.py            json_to_dataframe, drop_columns_not_in_contract
├── packages/data-platform-datavault/     Standalone Data Vault 2.0 package
│   └── src/data_platform_datavault/
│       ├── model.py                      Specs, host adapter, resolved model
│       ├── hashing.py                    Deterministic MD5 hash keys and hashdiffs
│       └── components/                   DataVaultFactory + asset builders
├── services/synthetic-api/               FastAPI synthetic-data service
├── projects/example_pipelines/           Three medallion pipelines, six contracts, one vault
├── projects/test_project/                Minimal separate project proving reusability
├── tests/                                Integration and smoke tests
├── docker/                               Dockerfiles + Dagster and Trino config
├── docker-compose.yml                    MinIO + synthetic API + Dagster (+ optional Trino)
├── env.example                           Every environment variable with defaults
└── pyproject.toml                        Ruff / mypy / pytest tooling config
```

## Architecture

- **Every structure is data-driven.** Pipelines are declared as YAML documents; the component
  expands them into asset definitions at definition-load time.
- **Contracts are the single source of truth.** The same `DataContract` drives Spark schema
  derivation, type casting, validation, Dagster asset checks, and synthetic generation.
- **Validation is not duplicated.** A contract expands into `CheckSpec` objects that are executed
  both inline in the silver/gold assets (fail fast) and as Dagster asset checks (observability).
- **The components are reusable.** `projects/test_project` depends only on the published
  packages and declares a full medallion pipeline and vault with no project-local framework code.
- **Data Vault is separable.** All Data Vault code lives in `data-platform-datavault`; the only
  coupling to the host platform is the host-import block in `model.py`.
- **Resources are environment-driven.** Spark, storage, REST, Trino, and metadata configuration
  come from environment variables so the same code runs locally or in Compose/CI.

## Data contracts

Contracts live in YAML and are loaded via `load_contract_file`, the `ContractRegistry`, or a
trusted dotted callable (for example `data_contracts.get_example_silver_contract_path`).
Supported features:

| Feature | Example |
| --- | --- |
| Logical types | `string`, `integer`, `long`, `float`, `double`, `decimal`, `boolean`, `date`, `timestamp`, `array`, `struct`, `binary` |
| Required / nullable | `required: false`, `nullable: false` |
| Keys | `primary_key: true` per column or contract-level `primary_key`; `unique_keys` |
| Defaults / enums | `default: true`, `allowed_values: [alpha, beta]` |
| Constraints | `min_value`, `max_value`, `min_length`, `max_length`, `regex`, `min_items`, `max_items`, `precision`, `scale` |
| Nested data | `type: struct` with `fields`, `type: array` with `items` |
| Foreign keys | `references: {contract: example_store, columns: [store_id]}` |
| Synthetic hints | `distribution: sequential`, `prefix`, `start_date`, `null_probability` |
| Expectations | `row_count_min`, `not_null`, `unique`, `accepted_values`, `range`, `regex`, `min_length`, `max_length`, `expression` |

Contracts are self-validating at load time: duplicate columns, dangling keys, invalid regexes,
type-incompatible defaults/allowed values, decimal precision omissions, and inconsistent
sequential-vs-regex hints all fail before any pipeline runs.

## defs.yaml and the MedallionFactory

A pipeline is one YAML document. Multiple documents may live in one file (`---` separators), and
multiple files may live anywhere under the project's defs module:

```yaml
type: data_platform_common.components.medallion_factory.MedallionFactory
attributes:
  pipeline_name: example_entity
  source:
    type: rest
    resource_key: rest_client
    route: generate/example_entity
    table_name: example_entity
    response_path: rows            # navigate the JSON envelope to the record list
  contracts:
    silver: data_contracts.get_example_silver_contract_path
    gold: data_contracts.get_example_gold_contract_path
  merge_keys: [entity_id]
  silver_transformations:
    - data_platform_common.transformations.json_to_dataframe
    - example_pipelines.transformations.clean_data
  gold_transformations:
    - example_pipelines.transformations.add_score_band
    - data_platform_common.transformations.drop_columns_not_in_contract
  partitioning:
    mode: daily
    start: "2026-01-01"
    timezone: UTC
    partition_column: event_date
```

The component generates:

| Asset key | Behaviour |
| --- | --- |
| `[pipeline, bronze, table]` | Calls the REST resource, stores raw JSON records plus `_ingested_at`, `_source`, `_pipeline`, `_partition_key`. |
| `[pipeline, silver, table]` | Runs ordered transformations, casts to the silver contract schema, validates, writes Delta. |
| `[pipeline, gold, table]` | Runs ordered transformations, casts to the gold contract schema, validates, writes curated Delta. |

`projects/test_project` shows the smallest possible declaration (snapshot mode, two transforms
from the shared library, no project-local Python).

### Transformations

Transformations are dotted import paths resolved through the **trusted import resolver**. Two safety
rules apply while definitions load, not at materialization:

1. The dotted path must be inside a trusted prefix: `data_platform_common`, the packages of the
   project that owns the defs file (declared by `root_module` and `trusted_prefixes` in `dg.toml`,
   or discovered from `src/` in a source checkout), `DATA_PLATFORM_TRUSTED_PREFIXES`, or explicit
   `trusted_import_prefixes`.
2. The signature must accept the DataFrame first and any extra parameters must be keyword context
   from `{contract, partition, pipeline_name, spark, params, layer, metadata}` (or `**kwargs`).

This makes typos and unsafe imports fail at `dg load`, and lets contract-aware transforms such as
`drop_columns_not_in_contract(df, *, contract=...)` work without leaking framework logic.

## Asset checks

Each contract expands into Dagster asset checks: missing/unexpected columns, column types,
nullability, primary-key and unique-key constraints, allowed values, numeric ranges, string
lengths, regex patterns, empty datasets, foreign-key integrity, and every declared expectation.
Foreign-key checks declare `additional_deps` on the parent silver asset so ordering is guaranteed.

## Data Vault 2.0 factory

`DataVaultFactory` turns silver contracts into a Data Vault 2.0 model. It ships in
the standalone `data-platform-datavault` package and is a second reusable
component with the same wiring pattern as `MedallionFactory`:

```yaml
type: data_platform_datavault.components.data_vault_factory.DataVaultFactory
attributes:
  vault_name: retail
  sources:
    - name: example_store
      contract: data_contracts.get_example_store_silver_contract_path
      pipeline_name: example_store
    - name: example_order
      contract: data_contracts.get_example_order_silver_contract_path
      pipeline_name: example_order
  hubs:
    - name: store
      business_keys: [store_id]
      sources:
        - name: example_store
          columns: {store_id: store_id}
    - name: order
      business_keys: [order_id]
      sources:
        - name: example_order
          columns: {order_id: order_id}
  links:
    - name: store_order
      relationships:
        - {hub: order, columns: [order_id]}
        - {hub: store, columns: [store_id]}
      sources:
        - name: example_order
          columns: {order_id: order_id, store_id: store_id}
  satellites:
    - name: store_profile
      parent: store
      attributes: [store_name, region, event_date]
      load_ts_column: created_at
```

Hubs and links both accept one or more `sources`, each with a canonical-to-physical
column mapping. Their rows are unioned and deduplicated by the hash; the first
declared source wins duplicate key combinations, so `record_source` is stable.

Generated tables (asset keys `[vault, kind, table]`, Delta layer
`<layer>/<vault_name>`):

| Kind | Example | Columns |
| --- | --- | --- |
| Hub | `retail/hub/hub_store` | `hub_store_hk`, `store_id`, `load_ts`, `record_source` |
| Link | `retail/link/lnk_store_order` | `lnk_store_order_hk`, `hub_order_hk`, `hub_store_hk`, `load_ts`, `record_source` |
| Satellite | `retail/sat/sat_store_profile` | `hub_store_hk`, `load_ts`, `hashdiff`, `record_source`, tracked attributes |

### Link sources and column mappings

`relationships` defines the **canonical** link schema: the participating hubs
and the key columns whose order determines the hash. `sources` maps that schema
onto one or more physical silver tables:

```yaml
links:
  - name: store_order
    relationships:
      - {hub: order, columns: [order_id]}
      - {hub: store, columns: [store_id]}
    sources:
      - name: order_src
        columns: {order_id: order_id, store_id: store_id}
      - name: invoice_src
        columns: {order_id: invoice_order_ref, store_id: invoice_store_code}
```

Each mapping is `canonical column -> physical column`. Every source must map
every canonical column; sources that lack one need upstream consolidation. The
link reads all listed tables, aliases them to the canonical names, unions the
rows, and deduplicates by the link hash. If the same key combination appears in
several sources, the **first declared source wins** (deterministic priority), so
`record_source` is stable. Rows from all sources carry their own
`record_source`; `load_ts` is the run timestamp. No joins are performed.

Satellites inherit the physical source of their parent hub/link, which must
currently have exactly one source. `attributes` lists canonical output names;
`columns` optionally maps each one to a differently named source column. A
satellite on a multi-source parent is rejected until multi-source satellite
support is added.

Parameters that shape the output:

- Hash keys and hashdiffs are always **MD5**; **`hash_delimiter`**,
  **`normalize_key_values`**, and per-hub/link **`hash_prefix`** control the
  payload. Nulls hash as `^^`; values are trimmed/uppercased by default.
- **`load_ts_column`** uses a deterministic source timestamp instead of the run
  timestamp; **`attributes`** selects (or defaults to) the tracked payload;
  **`hashdiff_column`**, table names, and hash key names are overridable.
- **`layer`** and **`vault_name`** control both the asset namespace and the
  Delta path prefix.
- **`write_mode`** defaults to `merge`; vault merges are **insert-only**
  (`delta/update_matched: false`), so matched rows are never rewritten and every
  rerun is idempotent.

Vault tables are written with insert-only merges. No asset checks are generated
for vault assets; validation belongs to the upstream silver/gold contracts.

## Partitioning and full snapshots

`partitioning.mode` accepts:

| Mode | Dagster definition | Notes |
| --- | --- | --- |
| `unpartitioned` | none | single logical run; overwrite/merge per write mode |
| `full_snapshot` | none | explicit snapshot semantics: every run reprocesses everything |
| `static` | `StaticPartitionsDefinition` | keys are fixed in YAML |
| `daily` / `hourly` | time-window partitions | `start`/`end`/`timezone`; hourly `start` may be `YYYY-MM-DD` or `YYYY-MM-DD-HH:MM` |
| `dynamic` | `DynamicPartitionsDefinition` | partition name is `<pipeline>_partitions` |
| `multi` | `MultiPartitionsDefinition` | list of dimensions (`static`, `daily`, `hourly`, `dynamic`) |

Partition keys propagate everywhere:

- **Source requests**: `route`, `params` values, and request bodies support `{partition_key}`,
  `{start_date}`, `{end_date}`, and `{dim:<name>}` placeholders.
- **Execution metadata**: partition key, time window, and dimension keys are attached to
  materializations.
- **Storage**: `partition_column`/`partition_columns` become Delta `partitionBy` columns. Bronze
  uses the `_partition_key` column so raw ingestion is partitioned per run.

## Delta writes, merge, and idempotency

Medallion assets go through `DeltaLakeIOManager`; vault assets call
`DeltaTableWriter` directly from the asset body. Both use the same write options
and support:

| Mode | Behaviour |
| --- | --- |
| `merge` | dedupe incoming keys, `MERGE ... WHEN MATCHED UPDATE ALL / WHEN NOT MATCHED INSERT ALL` |
| `append` | plain append (explicitly not idempotent) |
| `overwrite` | full replace, `overwriteSchema` when schema evolution is enabled |
| `partition_overwrite` | dynamic partition overwrite of the partitions present in the batch |

Safety rules:

- **Merge without keys is rejected** at option construction (`MergeKeysRequiredError`), at asset
  definition time (pipeline validation), and again at write time if keys are missing from the frame.
  A requested merge never silently degrades to a full overwrite.
- **Reruns are idempotent**: default write modes are derived from the configuration —
  merge keys present → `merge`; partitioned → `partition_overwrite`; otherwise `overwrite`.
  Merging deduplicates keys within the batch before touching the table.
- **Schema evolution** is explicit (`schema_evolution: true` by default): merges set
  `spark.databricks.delta.schema.autoMerge.enabled` and use `withSchemaEvolution()` when available;
  full overwrites set `overwriteSchema`; partition overwrites use `mergeSchema` because Delta
  forbids `overwriteSchema` in dynamic partition overwrite mode.
- Every write records `delta/path`, `delta/write_mode`, `delta/rows`, merge keys, partition
  columns, and Delta operation metrics on the materialization.

## Resources and environment

Shared resources are constructed by `build_resources_from_env()` and wired into each project's
`definitions.py`:

| Resource | Purpose |
| --- | --- |
| `rest_client` | JSON REST access with timeouts, bounded retries/backoff, headers, optional API key |
| `spark` | local or Spark Connect (`remote_url`) sessions with Delta enabled |
| `lakehouse` | `local` or `s3` (MinIO) table path resolution + Hadoop S3A options |
| `trino` | optional query access; raises a clear error while disabled |
| `metadata` | optional external metadata webhook; a no-op when unconfigured |
| `delta_io_manager` | the Delta I/O manager used by medallion assets; vault assets write Delta directly |

See `env.example` for every variable and default. Notable ones:

```bash
DATA_PLATFORM_REST_BASE_URL=http://localhost:8000
DATA_PLATFORM_SPARK_MODE=local            # local | remote
DATA_PLATFORM_SPARK_MASTER=local[2]
DATA_PLATFORM_LAKEHOUSE_MODE=local        # local | s3
DATA_PLATFORM_LAKEHOUSE_LOCAL_ROOT=/tmp/dagster_pipelines/lakehouse
DATA_PLATFORM_S3_ENDPOINT_URL=http://localhost:9000
DATA_PLATFORM_TRINO_ENABLED=false
DATA_PLATFORM_METADATA_ENDPOINT=
```

## Synthetic data service

`services/synthetic-api` is a generic FastAPI service driven by the same contracts:

| Endpoint | Description |
| --- | --- |
| `GET /health` | status, version, contract count |
| `GET /contracts` | contract summaries |
| `GET /contracts/{name}` | full contract document |
| `POST/GET /generate/{name}` | deterministic rows for one contract |
| `POST /generate` | batch generation of a contract and all its dependencies |

Generation guarantees:

- **Deterministic**: per-contract seeds derived from the request seed; identical inputs produce
  identical rows across processes.
- **Contract-valid by default**: every batch is validated against the contract before it is
  returned (the same pydantic row model used in tests).
- **Complete type coverage**: nulls, uniqueness, primary keys, enums, numeric/string/date bounds,
  datetimes, arrays, nested structs, and regex constraints (via a small built-in regex generator).
- **Referential integrity**: the dependency graph detects missing dependencies and cycles, orders
  parents before children, and populates child foreign keys from generated parent key values.

```bash
curl -s localhost:8000/generate/example_entity?rows=3\&seed=7 | python -m json.tool
curl -s -X POST localhost:8000/generate -H 'content-type: application/json' \
  -d '{"contract": "example_order", "rows": 5, "seed": 1}' | python -m json.tool
```

## Running locally

```bash
# 1. Install (Python 3.12+, Java 21 for Spark)
bash scripts/bootstrap.sh
source .venv/bin/activate

# 2. Start the synthetic-data service (another terminal, or use Docker Compose)
uvicorn synthetic_api.app:app --host 0.0.0.0 --port 8000

# 3. Start Dagster
export DATA_PLATFORM_REST_BASE_URL=http://localhost:8000
dagster dev -m example_pipelines.definitions
# UI: http://localhost:3000
```

Or the whole stack (MinIO + API + Dagster) with Docker:

```bash
docker compose up --build
# Dagster UI:    http://localhost:3000
# Synthetic API: http://localhost:8000/docs
# MinIO console: http://localhost:9001  (minioadmin / minioadmin)
```

MinIO no longer publishes anonymous public images (Docker Hub repos deleted, Quay requires auth),
so the Compose file defaults to the community `bitnamilegacy/minio` images. Point
`MINIO_IMAGE`/`MINIO_CLIENT_IMAGE` at your own mirror if you prefer.

The S3 path is verified by an opt-in round-trip test:

```bash
docker compose up -d minio minio-init
RUN_DOCKER_TESTS=1 pytest tests/docker -m docker -q
```

Checks and tests:

```bash
ruff format --check .
ruff check .
mypy
ty check
pytest -m "not docker and not slow"     # fast unit + integration tests
pytest tests/smoke -m smoke             # end-to-end Spark/Delta materialization
```

## Adding a new dataset (contracts + YAML only)

1. **Write the silver contract** in your project's contracts directory, for example
   `projects/example_pipelines/src/data_contracts/yaml/example_invoice_silver.yaml`.
2. **Write the gold contract** (`example_invoice_gold.yaml`) with the curated columns.
3. **Expose path helpers** (optional but tidy) in the project's `data_contracts` package, or point
   `contracts.*` directly at file paths.
4. **Add a defs document** under the project's `defs/` package:

   ```yaml
   type: data_platform_common.components.medallion_factory.MedallionFactory
   attributes:
     pipeline_name: example_invoice
     source:
       type: rest
       resource_key: rest_client
       route: generate/example_invoice
       table_name: example_invoice
       response_path: rows
     contracts:
       silver: data_contracts.get_example_invoice_silver_contract_path
       gold: data_contracts.get_example_invoice_gold_contract_path
     merge_keys: [invoice_id]
     silver_transformations:
       - data_platform_common.transformations.json_to_dataframe
     gold_transformations:
       - data_platform_common.transformations.drop_columns_not_in_contract
     partitioning:
       mode: daily
       start: "2026-01-01"
       partition_column: event_date
   ```

5. **Optionally add transformations** as Python functions with a supported signature and reference
   them by dotted path.
6. **Add contracts to the synthetic service** (they are auto-discovered from the contracts
   directory) and expose a route in the upstream system.
7. **Run** `dagster dev` (or reload) — the new assets, checks, and schemas appear automatically.

No changes to `data_platform_common` are required, and asset keys are namespaced by
`pipeline_name`, so multiple datasets never collide.

## Reusability

`projects/test_project` is an independent Dagster project that installs the
published packages (`data-platform-common` and `data-platform-datavault`) and
declares both component types with no project-local framework code:

```yaml
type: data_platform_common.components.medallion_factory.MedallionFactory
attributes:
  pipeline_name: tiny_entity
  ...
  partitioning:
    mode: full_snapshot
```

```yaml
type: data_platform_datavault.components.data_vault_factory.DataVaultFactory
attributes:
  vault_name: tiny_retail
  sources:
    - name: tiny_store
      contract: test_project.contracts.get_tiny_store_silver_contract_path
      pipeline_name: tiny_store
    ...
  hubs:
    - name: store
      business_keys: [store_id]
      sources:
        - name: tiny_store
          columns: {store_id: store_id}
  links:
    - name: store_order
      relationships:
        - {hub: order, columns: [order_id]}
        - {hub: store, columns: [store_id]}
      sources:
        - name: tiny_order
          columns: {order_id: order_id, store_id: store_id}
  satellites:
    - {name: store_profile, parent: store, attributes: [store_name, region]}
```

Its tests load the definitions without Spark and assert the generated asset keys.

### Porting the Data Vault component to another project

The Data Vault code is a **standalone package** (`packages/data-platform-datavault`).
All host-package coupling is isolated in the host-import block at the top of
`src/data_platform_datavault/model.py`.

1. **Install the package** (wheel, editable checkout, or git reference):
   ```bash
   pip install /path/to/dagster_pipelines/packages/data-platform-datavault
   ```
   It depends on `data-platform-common` for the shared contract/IO layer, but has
   no dependency on `example_pipelines`.
2. **Retarget the host imports if your common package differs.** `model.py` imports
   the host symbols (`DataContract`, `ContractRegistry`, `parse_contract_document`,
   `build_trusted_prefixes`, `DeltaTableWriter`, `DeltaWriteOptions`, `WriteMode`,
   and `METADATA_COLUMN_PREFIX`). Point those imports at your
   equivalent modules — that block is the entire porting surface. If your
   common package uses the same module layout, only the top-level package name changes.
3. **Provide resources.** The vault needs `spark` and `lakehouse`; it writes
   Delta directly and does not use an IO manager or metadata resource.
   The simplest wiring is the shared environment builder in your `definitions.py`:
   ```python
   import dagster as dg
   from data_platform_common.resources import build_resources_from_env


   @dg.definitions
   def defs():
       return dg.Definitions.merge(
           dg.load_from_defs_folder(path_within_project=Path(__file__)),
           dg.Definitions(resources=build_resources_from_env()),
       )
   ```
   The vault uses the fixed resource names `spark` and `lakehouse`.
4. **Describe the silver contracts** in your project (YAML) and expose path helpers, e.g.
   `my_project.contracts.get_store_silver_contract_path`.
5. **Add a `defs/<anything>/defs.yaml`** using the full component path (see the example above).
   Do not put a `defs.yaml` at the `defs/` root if you also use subfolders — a root file
   short-circuits component discovery. Each component belongs in its own subdirectory.
6. **Make contract references resolvable.** Either use absolute/relative file paths, or set
   `tool.dg.project.root_module` (or `trusted_prefixes`) in your `dg.toml`/`pyproject.toml`
   so dotted helpers such as `my_project.contracts.get_*` are trusted. The component derives
   the trusted prefixes from the **consuming** project, not from this repo.
7. **Point `pipeline_name` at the pipeline that materializes each silver table.** The vault
   reads the Delta table at `<layer>/<table>` via the lakehouse resource and derives its
   source asset key as `[pipeline_name, layer, table]`.
8. **Reload definitions.** Hubs, links, satellites, and lineage are generated
   automatically; no Python changes are required.

The package also ships its own unit tests under
`packages/data-platform-datavault/tests`, so the extracted copy can be verified
independently of this repository.

## Verification status

Commands executed against this repository (Linux, Python 3.13.5, Java 21,
Spark 4.2.0, Delta 4.4.0):

| Command | Result |
| --- | --- |
| `ruff format --check .` | 74 files already formatted |
| `ruff check .` | all checks passed |
| `mypy` (root config, all five `src/` trees) | no issues in 50 source files |
| `pytest -m "not docker and not slow"` | 109 passed, 12 deselected |
| `pytest tests/smoke -m smoke` | 11 passed in 337.06s (Spark/Delta end-to-end, medallion + vault) |
| `docker compose config` | valid |
| `docker build -f docker/Dockerfile.api ...` | image built |
| `docker build -f docker/Dockerfile.dagster ...` | image built |
| `docker compose up -d` + `dagster asset list` in container | 14 assets listed, UI 200, API healthy |
| `RUN_DOCKER_TESTS=1 pytest tests/docker -m docker` | 1 passed in 35.61s (Delta merge round-trip through MinIO S3A) |

The smoke suite materializes the complete parent/child and daily-partitioned
pipelines and the Data Vault hubs/links/satellites against a live synthetic-data
service, validates the gold tables against their contracts, executes the
generated asset checks in real Dagster jobs, proves merge reruns are idempotent,
and verifies vault hash determinism and referential integrity.

## Known limitations

- **Spark artifacts**: local Delta sessions download `delta-spark` from Maven on first start
  (Ivy cache). Air-gapped environments must pre-seed the Ivy cache or configure
  `spark.jars.packages`/mirrors.
- **S3 mode requires Hadoop AWS bundles** (`hadoop-aws`, `aws-java-sdk-bundle`) via
  `DATA_PLATFORM_SPARK_EXTRA_PACKAGES`; the Docker image wires this up, but the packages download
  from Maven on first use.
- **MinIO images**: MinIO deleted its public Docker Hub repositories and now gates its Quay
  images behind authentication, so upstream `minio/minio` can no longer be pulled anonymously.
  `docker-compose.yml` therefore defaults to the community Bitnami legacy images
  (`bitnamilegacy/minio`, `bitnamilegacy/minio-client`), which bundle the same server and client,
  and exposes `MINIO_IMAGE` / `MINIO_CLIENT_IMAGE` overrides. The MinIO-backed Delta round-trip
  test (`tests/docker/test_minio_roundtrip.py`) runs against this stack.
- **Trino integration is optional and unexercised here**; the resource raises a clear error while
  disabled and the Compose profile provides a catalog for filesystem Delta tables.
- **Append mode is not idempotent** by design; use merge or partition overwrite for rerun safety.
- **`rlike` semantics**: regex checks use partial-match semantics (like Spark), while sequential
  synthetic hints are validated against the regex at load time.
- **Nested array element constraints** are enforced by record-level validation and synthetic
  generation; frame-level checks cover scalar leaves.
- **Metadata integration treats delivery failures as non-fatal** (logged, recorded return value).

## Troubleshooting

- **Java missing** → install `openjdk-21-jre-headless` and/or set `JAVA_HOME`.
- **`INVALID_EXTENSION` or Delta class errors** → ensure the session was built by
  `SparkResource` (delta extensions + `spark.jars.packages`).
- **`Path does not exist` for a parent table** → materialize the parent silver asset first; the
  foreign-key check declares dependencies, and `dg.materialize` orders them automatically.
- **406/405 from the source** → check `source.method` and `response_path` against the API.
