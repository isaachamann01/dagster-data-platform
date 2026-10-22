"""Dagster asset builders for Data Vault 2.0 hubs, links, and satellites.

Every vault table is a Delta asset written through the shared I/O manager with
an **insert-only merge** (matched rows are never updated), which keeps all
reruns idempotent while preserving first-seen load timestamps.

Note: this module intentionally avoids ``from __future__ import annotations`` so
Dagster decorators receive real annotation objects for ``context``.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_platform_datavault._common import (
    META_LAYER,
    META_MERGE_KEYS,
    META_PARTITION_BY,
    META_SCHEMA_EVOLUTION,
    META_TABLE,
    META_UPDATE_MATCHED,
    META_WRITE_MODE,
    DeltaTableWriter,
)
from data_platform_datavault.hashing import hash_expression
from data_platform_datavault.model import (
    DataVaultModel,
    HashConfig,
    ResolvedHub,
    ResolvedLink,
    ResolvedSatellite,
    ResolvedSource,
)


def read_source_table(spark: Any, lakehouse: Any, source: ResolvedSource) -> DataFrame:
    path = lakehouse.table_path(source.layer, source.table_name)
    if not DeltaTableWriter(spark).table_exists(path):
        raise ValueError(
            f"Silver source '{source.name}' not found at {path}; "
            "materialize the upstream silver assets first"
        )
    return spark.read.format("delta").load(path)


def _hash_column(config: HashConfig, *, alias: str) -> F.Column:
    return hash_expression(
        config.columns,
        delimiter=config.delimiter,
        prefix=config.prefix,
        normalize=config.normalize,
    ).alias(alias)


def _require_columns(frame: DataFrame, columns: tuple[str, ...] | list[str], table: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"vault table '{table}' requires source columns {missing}")


def _vault_metadata(model: DataVaultModel, table: str, merge_keys: list[str]) -> dict[str, Any]:
    return {
        META_LAYER: model.layer,
        META_TABLE: table,
        META_WRITE_MODE: model.specification.write_mode,
        META_MERGE_KEYS: merge_keys,
        META_PARTITION_BY: [],
        META_SCHEMA_EVOLUTION: True,
        META_UPDATE_MATCHED: False,
    }


def _now() -> datetime:
    return datetime.now(UTC)


def build_hub_asset(
    model: DataVaultModel, hub: ResolvedHub, keys: dict[str, str]
) -> dg.AssetsDefinition:
    spec = model.specification
    source_names = [source_mapping.source.name for source_mapping in hub.sources]
    source_columns = {
        source_mapping.source.name: dict(source_mapping.columns) for source_mapping in hub.sources
    }

    @dg.asset(
        key=hub.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[*hub.source_asset_keys],
        required_resource_keys={keys["spark"], keys["lakehouse"], keys["metadata"]},
        io_manager_key=keys["io_manager"],
        metadata={
            **_vault_metadata(model, hub.table, [hub.hash_key]),
            "datavault/kind": "hub",
            "datavault/sources": source_names,
            "datavault/source_columns": source_columns,
            "datavault/business_keys": list(hub.business_keys),
            "datavault/hash_key": hub.hash_key,
        },
        description=f"Data Vault hub {hub.table} (business keys: {list(hub.business_keys)})",
    )
    def hub_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
        metadata_resource = getattr(context.resources, keys["metadata"])
        lakehouse.configure_spark(spark)

        frames = []
        for priority, hub_source in enumerate(hub.sources):
            frame = read_source_table(spark, lakehouse, hub_source.source)
            _require_columns(frame, hub_source.physical_columns, hub.table)
            frames.append(
                frame.select(
                    *[
                        F.col(physical).alias(canonical)
                        for canonical, physical in hub_source.columns
                    ],
                    F.lit(hub_source.source.record_source).alias(spec.record_source_column_name),
                    F.lit(priority).alias("__source_priority"),
                )
            )
        unioned = frames[0]
        for extra in frames[1:]:
            unioned = unioned.unionByName(extra)

        keyed = unioned.select(
            _hash_column(hub.hash, alias=hub.hash_key),
            *[F.col(column) for column in hub.business_keys],
            F.col("__source_priority"),
            F.col(spec.record_source_column_name),
        )
        window = Window.partitionBy(hub.hash_key).orderBy(F.col("__source_priority"))
        deduped = keyed.withColumn("__source_row", F.row_number().over(window)).where(
            F.col("__source_row") == 1
        )
        result = deduped.select(
            hub.hash_key,
            *hub.business_keys,
            F.lit(_now()).alias(spec.load_timestamp_column_name),
            F.col(spec.record_source_column_name),
        )
        context.add_output_metadata(
            {
                "datavault/kind": "hub",
                "datavault/sources": source_names,
                "datavault/source_columns": source_columns,
                "datavault/business_keys": list(hub.business_keys),
                "datavault/hash_key": hub.hash_key,
            }
        )
        metadata_resource.record(
            "vault_hub_loaded",
            {"vault": model.vault_name, "table": hub.table, "sources": source_names},
        )
        return result

    return hub_asset


def build_link_asset(
    model: DataVaultModel, link: ResolvedLink, keys: dict[str, str]
) -> dg.AssetsDefinition:
    spec = model.specification
    source_names = [link_source.source.name for link_source in link.sources]
    source_columns = {
        link_source.source.name: dict(link_source.columns) for link_source in link.sources
    }

    @dg.asset(
        key=link.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[*link.source_asset_keys, *link.parent_asset_keys],
        required_resource_keys={keys["spark"], keys["lakehouse"], keys["metadata"]},
        io_manager_key=keys["io_manager"],
        metadata={
            **_vault_metadata(model, link.table, [link.hash_key]),
            "datavault/kind": "link",
            "datavault/sources": source_names,
            "datavault/source_columns": source_columns,
            "datavault/hubs": [relationship.hub_name for relationship in link.relationships],
            "datavault/hash_key": link.hash_key,
        },
        description=(
            "Data Vault link "
            f"{link.table} (hubs: {[relationship.hub_name for relationship in link.relationships]})"
        ),
    )
    def link_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
        metadata_resource = getattr(context.resources, keys["metadata"])
        lakehouse.configure_spark(spark)

        frames = []
        for priority, link_source in enumerate(link.sources):
            frame = read_source_table(spark, lakehouse, link_source.source)
            _require_columns(frame, link_source.physical_columns, link.table)
            frames.append(
                frame.select(
                    *[
                        F.col(physical).alias(canonical)
                        for canonical, physical in link_source.columns
                    ],
                    F.lit(link_source.source.record_source).alias(spec.record_source_column_name),
                    F.lit(priority).alias("__source_priority"),
                )
            )
        unioned = frames[0]
        for extra in frames[1:]:
            unioned = unioned.unionByName(extra)

        keyed = unioned.select(
            _hash_column(link.hash, alias=link.hash_key),
            *[
                _hash_column(relationship.hub_hash, alias=relationship.hub_hash_key)
                for relationship in link.relationships
            ],
            F.col("__source_priority"),
            F.col(spec.record_source_column_name),
        )
        # Deterministic dedupe: the first declared source wins for duplicate
        # key combinations.
        window = Window.partitionBy(link.hash_key).orderBy(F.col("__source_priority"))
        deduped = keyed.withColumn("__source_row", F.row_number().over(window)).where(
            F.col("__source_row") == 1
        )
        result = deduped.select(
            link.hash_key,
            *[relationship.hub_hash_key for relationship in link.relationships],
            F.lit(_now()).alias(spec.load_timestamp_column_name),
            F.col(spec.record_source_column_name),
        )
        context.add_output_metadata(
            {
                "datavault/kind": "link",
                "datavault/sources": source_names,
                "datavault/source_columns": source_columns,
                "datavault/hubs": [relationship.hub_name for relationship in link.relationships],
                "datavault/hash_key": link.hash_key,
            }
        )
        metadata_resource.record(
            "vault_link_loaded",
            {
                "vault": model.vault_name,
                "table": link.table,
                "sources": source_names,
            },
        )
        return result

    return link_asset


def build_satellite_asset(
    model: DataVaultModel, satellite: ResolvedSatellite, keys: dict[str, str]
) -> dg.AssetsDefinition:
    spec = model.specification

    @dg.asset(
        key=satellite.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[satellite.source.asset_key, satellite.parent_asset_key],
        required_resource_keys={keys["spark"], keys["lakehouse"], keys["metadata"]},
        io_manager_key=keys["io_manager"],
        metadata={
            **_vault_metadata(
                model, satellite.table, [satellite.parent_hash_key, satellite.hashdiff_column]
            ),
            "datavault/kind": "satellite",
            "datavault/source": satellite.source.name,
            "datavault/parent": f"{satellite.parent_kind}:{satellite.parent_name}",
            "datavault/attributes": list(satellite.attributes),
            "datavault/hashdiff": satellite.hashdiff_column,
        },
        description=(
            f"Data Vault satellite {satellite.table} on {satellite.parent_kind} "
            f"'{satellite.parent_name}'"
        ),
    )
    def satellite_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
        metadata_resource = getattr(context.resources, keys["metadata"])
        lakehouse.configure_spark(spark)

        frame = read_source_table(spark, lakehouse, satellite.source)
        required = [
            *[physical for _, physical in satellite.parent_source_columns],
            *[physical for _, physical in satellite.attribute_source_columns],
        ]
        if satellite.load_ts_column:
            required.append(satellite.load_ts_column)
        _require_columns(frame, required, satellite.table)

        projections = [
            *[
                F.col(physical).alias(canonical)
                for canonical, physical in satellite.parent_source_columns
            ],
            *[
                F.col(physical).alias(canonical)
                for canonical, physical in satellite.attribute_source_columns
            ],
        ]
        if satellite.load_ts_column:
            projections.append(F.col(satellite.load_ts_column).alias("__load_ts"))
        frame = frame.select(*projections)

        load_ts = F.to_timestamp(F.col("__load_ts")) if satellite.load_ts_column else F.lit(_now())
        result = frame.select(
            _hash_column(satellite.parent_hash, alias=satellite.parent_hash_key),
            load_ts.alias(spec.load_timestamp_column_name),
            hash_expression(
                satellite.attributes,
                delimiter=spec.hash_delimiter,
                normalize=spec.normalize_key_values,
            ).alias(satellite.hashdiff_column),
            F.lit(satellite.source.record_source).alias(spec.record_source_column_name),
            *[F.col(attribute) for attribute in satellite.attributes],
        ).dropDuplicates([satellite.parent_hash_key, satellite.hashdiff_column])
        context.add_output_metadata(
            {
                "datavault/kind": "satellite",
                "datavault/source": satellite.source.name,
                "datavault/parent": f"{satellite.parent_kind}:{satellite.parent_name}",
                "datavault/parent_source_columns": dict(satellite.parent_source_columns),
                "datavault/attributes": list(satellite.attributes),
                "datavault/hashdiff": satellite.hashdiff_column,
            }
        )
        metadata_resource.record(
            "vault_satellite_loaded",
            {"vault": model.vault_name, "table": satellite.table, "source": satellite.source.name},
        )
        return result

    return satellite_asset


def build_vault_assets(model: DataVaultModel, keys: dict[str, str]) -> list[dg.AssetsDefinition]:
    """Build one asset per hub, link, and satellite in the model."""

    assets: list[dg.AssetsDefinition] = []
    for hub in model.hubs.values():
        assets.append(build_hub_asset(model, hub, keys))
    for link in model.links.values():
        assets.append(build_link_asset(model, link, keys))
    for satellite in model.satellites.values():
        assets.append(build_satellite_asset(model, satellite, keys))
    return assets
