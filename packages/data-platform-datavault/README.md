# data-platform-datavault

Standalone Data Vault 2.0 package for Dagster: hubs, links, satellites, hashing,
and the `DataVaultFactory` component.

## Install

```bash
pip install ./packages/data-platform-datavault
```

The package depends on `data-platform-common` for the shared contract/IO layer.
It has no dependency on `example_pipelines` or any other project in this repo.

## Use

```yaml
type: data_platform_datavault.components.data_vault_factory.DataVaultFactory
attributes:
  vault_name: retail
  sources:
    - name: store
      contract: my_project.contracts.get_store_silver_path
      pipeline_name: store_pipeline
    - name: order
      contract: my_project.contracts.get_order_silver_path
      pipeline_name: order_pipeline
  hubs:
    - name: store
      business_keys: [store_id]
      sources:
        - name: store
          columns: {store_id: store_id}
    - name: order
      business_keys: [order_id]
      sources:
        - name: order
          columns: {order_id: order_id}
  links:
    - name: store_order
      relationships:
        - {hub: order, columns: [order_id]}
        - {hub: store, columns: [store_id]}
      sources:
        - name: order
          columns: {order_id: order_id, store_id: store_id}
  satellites:
    - {name: store_profile, parent: store, attributes: [store_name, region]}
    - {name: order_details, parent: order, attributes: [quantity]}
```

`relationships` defines the canonical key schema; `sources` maps it onto one or
more physical tables. Add another source with its own `columns` mapping to union
link instances from multiple feeds — the first declared source wins duplicate
key combinations.

Satellites inherit the source of their parent hub/link (currently the parent
must have exactly one source). `attributes` lists canonical output names;
`columns` optionally maps them to different source column names.

The component needs the resources `spark`, `lakehouse`, `metadata`, and
`delta_io_manager` (override with `resource_keys`).

## Porting to another common package

All host coupling lives in the host-import block at the top of
`src/data_platform_datavault/model.py`. To retarget the package to a different
(but similar) common library, change only those imports:

| Symbol | Purpose |
|---|---|
| `DataContract` | Contract model with `columns` / `column_names` |
| `ContractRegistry` | Resolves YAML and dotted contract references |
| `parse_contract_document` | Parses inline contract mappings (used by tests) |
| `ContractError` | Error raised for invalid vault models |
| `build_trusted_prefixes` | Trusted dotted-import resolution |
| `DeltaTableWriter` | Table existence checks and Delta access |
| `META_*` | IO-manager metadata keys (write contract) |
| `METADATA_COLUMN_PREFIX` | Prefix excluded from satellite attributes |

Everything else in the package is self-contained. Copy the directory or install
the wheel, retarget the host imports in `model.py`, and point `defs.yaml` at
`data_platform_datavault.components.data_vault_factory.DataVaultFactory`.

## Tests

Package-level model and hashing tests live in `tests/` and run without Spark:

```bash
pytest packages/data-platform-datavault/tests
```
