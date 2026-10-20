"""The MedallionFactory Dagster component.

Loadable from ``defs.yaml`` as::

    type: data_platform_common.components.medallion_factory.MedallionFactory
    attributes:
      pipeline_name: example_entity
      source:
        type: rest
        resource_key: rest_client
        route: generate/example_entity
        table_name: example_entity
      contracts:
        silver: data_contracts.get_example_silver_contract_path
        gold: data_contracts.get_example_gold_contract_path
      merge_keys: [entity_id]
      silver_transformations:
        - data_platform_common.transformations.json_to_dataframe
        - example_pipelines.transformations.clean_data
      gold_transformations:
        - data_platform_common.transformations.drop_columns_not_in_contract
      partitioning:
        mode: daily
        start: 2026-01-01
        timezone: UTC
"""

from __future__ import annotations

from typing import Any

import dagster as dg
from dagster import Model, Resolvable
from dagster.components import Component, ComponentLoadContext
from pydantic import Field

from data_platform_common.checks import ForeignKeyParent, build_asset_checks
from data_platform_common.contracts.models import DataContract
from data_platform_common.contracts.registry import ContractRegistry
from data_platform_common.errors import ContractError
from data_platform_common.imports import build_transformations, build_trusted_prefixes
from data_platform_common.medallion.assets import MedallionPipeline, build_pipeline_assets
from data_platform_common.medallion.config import (
    ContractsSpec,
    ResourceKeysSpec,
    RestSourceSpec,
    WriteModesSpec,
)
from data_platform_common.partitions import PartitioningConfig

PARENT_LAYER = "silver"


class MedallionFactory(Component, Resolvable, Model):
    """Generate bronze/silver/gold assets and checks from one YAML declaration."""

    pipeline_name: str
    source: RestSourceSpec
    contracts: ContractsSpec
    merge_keys: list[str] = Field(default_factory=list)
    silver_transformations: list[str] = Field(default_factory=list)
    gold_transformations: list[str] = Field(default_factory=list)
    partitioning: PartitioningConfig = Field(default_factory=PartitioningConfig)
    write_modes: WriteModesSpec = Field(default_factory=WriteModesSpec)
    resource_keys: ResourceKeysSpec = Field(default_factory=ResourceKeysSpec)
    trusted_import_prefixes: list[str] = Field(default_factory=list)
    contract_paths: list[str] = Field(default_factory=list)
    extra_columns: list[str] = Field(default_factory=list)
    schema_evolution: bool = True

    # -- loading -------------------------------------------------------------

    def _trusted_prefixes(self, context: ComponentLoadContext) -> tuple[str, ...]:
        return build_trusted_prefixes(getattr(context, "path", None), self.trusted_import_prefixes)

    def _foreign_key_parents(
        self, registry: ContractRegistry, contracts: list[DataContract]
    ) -> dict[str, ForeignKeyParent]:
        parents: dict[str, ForeignKeyParent] = {}
        for contract in contracts:
            for foreign_key in contract.foreign_keys:
                parent_contract = registry.try_get(foreign_key.references.contract)
                if parent_contract is not None:
                    table = parent_contract.table_name
                    columns = (
                        list(foreign_key.references.columns) or parent_contract.primary_key_columns
                    )
                else:
                    # Cross-pipeline parents are resolved by convention: the
                    # parent pipeline shares the parent contract's name and the
                    # reference must name the parent columns explicitly.
                    table = foreign_key.references.contract
                    columns = list(foreign_key.references.columns)
                    if not columns:
                        raise ContractError(
                            f"foreign key {foreign_key.columns} references unknown contract "
                            f"'{foreign_key.references.contract}'; add it to contract_paths or "
                            "name the referenced columns explicitly"
                        )
                parents["+".join(foreign_key.columns)] = ForeignKeyParent(
                    table=table,
                    columns=columns,
                    layer=PARENT_LAYER,
                    asset_key=dg.AssetKey([foreign_key.references.contract, PARENT_LAYER, table]),
                )
        return parents

    # -- component API -------------------------------------------------------

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        prefixes = self._trusted_prefixes(context)
        registry = ContractRegistry(prefixes=prefixes)
        for extra_contract in self.contract_paths:
            registry.resolve(extra_contract)

        silver_contract = registry.resolve(self.contracts.silver)
        gold_contract = registry.resolve(self.contracts.gold) if self.contracts.gold else None

        pipeline = MedallionPipeline(
            pipeline_name=self.pipeline_name,
            source=self.source,
            silver_contract=silver_contract,
            gold_contract=gold_contract,
            merge_keys=list(self.merge_keys),
            silver_transformations=build_transformations(
                self.silver_transformations, prefixes=prefixes
            ),
            gold_transformations=build_transformations(
                self.gold_transformations, prefixes=prefixes
            ),
            partitioning=self.partitioning,
            write_modes=self.write_modes,
            resource_keys=self.resource_keys,
            foreign_key_parents=self._foreign_key_parents(
                registry, [contract for contract in (silver_contract, gold_contract) if contract]
            ),
            extra_columns=list(self.extra_columns),
            schema_evolution=self.schema_evolution,
        )

        assets = build_pipeline_assets(pipeline)
        check_resource_keys = {
            "spark": self.resource_keys.spark,
            "lakehouse": self.resource_keys.lakehouse,
        }
        checks: list[Any] = []
        for layer, contract in (("silver", silver_contract), ("gold", gold_contract)):
            if contract is None:
                continue
            checks.extend(
                build_asset_checks(
                    contract,
                    pipeline.asset_key(layer, contract.table_name),
                    layer=layer,
                    resource_keys=check_resource_keys,
                    foreign_key_parents=pipeline.foreign_key_parents,
                    extra_columns=self.extra_columns,
                )
            )
        return dg.Definitions(assets=assets, asset_checks=checks)


__all__ = ["MedallionFactory"]
