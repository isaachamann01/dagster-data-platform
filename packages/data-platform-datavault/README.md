# data-platform-datavault

Standalone Data Vault 2.0 package for Dagster: hubs, links, satellites, hashing,
generated asset checks, and the `DataVaultFactory` component.

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
    - {name: store, source: store, business_keys: [store_id]}
    - {name: order, source: order, business_keys: [order_id]}
  links:
    - name: store_order
      relationships:
        - {hub: order, columns: [order_id], source: order}
        - {hub: store, columns: [store_id], source: store}
  satellites:
    - {name: store_profile, source: store, parent: store, attributes: [store_name, region]}
    - {name: order_details, source: order, parent: order, attributes: [quantity]}
```

The component needs the resources `spark`, `lakehouse`, `metadata`, and
`delta_io_manager` (override with `resource_keys`).

## Porting to another common package

All host coupling lives in `src/data_platform_datavault/_common.py`. To retarget
the package to a different (but similar) common library, change only those
imports:

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
the wheel, retarget `_common.py`, and point `defs.yaml` at
`data_platform_datavault.components.data_vault_factory.DataVaultFactory`.

## Tests

Package-level model and hashing tests live in `tests/` and run without Spark:

```bash
pytest packages/data-platform-datavault/tests
```
