"""The DataVaultFactory Dagster component.

Turns a set of silver contracts into a Data Vault 2.0 model (hubs, links,
satellites) and generates one Delta-backed Dagster asset per table. Loadable from
``defs.yaml`` as::

    type: data_platform_datavault.components.data_vault_factory.DataVaultFactory
    attributes:
      vault_name: retail
      sources:
        - name: example_store
          contract: data_contracts.get_example_store_silver_contract_path
          pipeline_name: example_store
      hubs:
        - name: store
          source: example_store
          business_keys: [store_id]
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
          source: example_store
          parent: store
          attributes: [store_name, region, event_date]
          load_ts_column: created_at

Links define a canonical key schema in ``relationships`` and map it onto one or
more physical tables in ``sources``. Every source maps every canonical column;
the rows are unioned and deduplicated by the link hash, with the first declared
source winning duplicate key combinations::

      links:
        - name: store_order
          relationships:
            - {hub: order, columns: [order_id]}
            - {hub: store, columns: [store_id]}
          sources:
            - name: example_order
              columns: {order_id: order_id, store_id: store_id}
            - name: example_order_archive
              columns: {order_id: order_ref, store_id: store_code}
"""

from __future__ import annotations

import dagster as dg
from dagster.components import Component, ComponentLoadContext

from data_platform_datavault._common import ContractRegistry, build_trusted_prefixes
from data_platform_datavault.assets import build_vault_assets
from data_platform_datavault.config import DataVaultModelSpec
from data_platform_datavault.model import build_model


class DataVaultFactory(Component, DataVaultModelSpec):
    """Generate Data Vault 2.0 assets from silver contracts."""

    def _resource_keys(self) -> dict[str, str]:
        return {
            "spark": self.resource_keys.get("spark", "spark"),
            "lakehouse": self.resource_keys.get("lakehouse", "lakehouse"),
            "metadata": self.resource_keys.get("metadata", "metadata"),
            "io_manager": self.resource_keys.get("io_manager", "delta_io_manager"),
        }

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        prefixes = build_trusted_prefixes(
            getattr(context, "path", None), self.trusted_import_prefixes
        )
        registry = ContractRegistry(prefixes=prefixes)
        for extra_contract in self.contract_paths:
            registry.resolve(extra_contract)

        model = build_model(self, registry)
        keys = self._resource_keys()
        return dg.Definitions(assets=build_vault_assets(model, keys))


__all__ = ["DataVaultFactory"]
