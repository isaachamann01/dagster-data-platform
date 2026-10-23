"""The DataVaultFactory Dagster component.

Loads Data Vault specs from ``defs.yaml`` and generates Delta-backed assets for
hubs, links, and satellites. See the package README for the YAML reference.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster.components import Component, ComponentLoadContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_platform_datavault.hashing import hash_expression
from data_platform_datavault.model import (
    ContractRegistry,
    DataVaultModel,
    DataVaultModelSpec,
    DeltaTableWriter,
    DeltaWriteOptions,
    HashConfig,
    ResolvedHub,
    ResolvedLink,
    ResolvedSatellite,
    ResolvedSource,
    ResolvedSourceMapping,
    WriteMode,
    build_model,
    build_trusted_prefixes,
)

NOOP_IO_MANAGER_KEY = "vault_noop_io_manager"
RESOURCE_KEYS = {"spark": "spark", "lakehouse": "lakehouse"}


class _NoOpIOManager(dg.IOManager):
    """Vault assets write Delta directly; this only satisfies Dagster's output plumbing."""

    def handle_output(self, context: dg.OutputContext, obj: object) -> None:
        pass

    def load_input(self, context: dg.InputContext) -> None:
        return None


NOOP_IO_MANAGER = _NoOpIOManager()


class DataVaultFactory(Component, DataVaultModelSpec):
    """Generate Data Vault 2.0 assets from silver contracts."""

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        prefixes = build_trusted_prefixes(
            getattr(context, "path", None), self.trusted_import_prefixes
        )
        registry = ContractRegistry(prefixes=prefixes)
        for extra_contract in self.contract_paths:
            registry.resolve(extra_contract)

        model = build_model(self, registry)
        return dg.Definitions(
            assets=build_vault_assets(model, RESOURCE_KEYS),
            resources={NOOP_IO_MANAGER_KEY: NOOP_IO_MANAGER},
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


def _union_sources(
    spark: Any,
    lakehouse: Any,
    sources: tuple[ResolvedSourceMapping, ...],
    *,
    table: str,
    record_source_column: str,
) -> DataFrame:
    """Read, alias, and union every source; the first source has priority."""

    frames = []
    for priority, mapping in enumerate(sources):
        frame = read_source_table(spark, lakehouse, mapping.source)
        _require_columns(frame, [physical for _, physical in mapping.columns], table)
        frames.append(
            frame.select(
                *[F.col(physical).alias(canonical) for canonical, physical in mapping.columns],
                F.lit(mapping.source.record_source).alias(record_source_column),
                F.lit(priority).alias("__source_priority"),
            )
        )
    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame)
    return unioned


def _dedupe_first_source(frame: DataFrame, hash_key: str) -> DataFrame:
    """Keep the first declared source for each hash key."""

    window = Window.partitionBy(hash_key).orderBy(F.col("__source_priority"))
    return frame.withColumn("__source_row", F.row_number().over(window)).where(
        F.col("__source_row") == 1
    )


def _write_table(
    context: dg.AssetExecutionContext,
    spark: Any,
    lakehouse: Any,
    model: DataVaultModel,
    table: str,
    frame: DataFrame,
    *,
    merge_keys: list[str],
) -> None:
    """Write one vault table directly through the lakehouse Delta writer."""

    result = DeltaTableWriter(spark).write(
        frame,
        DeltaWriteOptions(
            path=lakehouse.table_path(model.layer, table),
            mode=WriteMode(model.specification.write_mode),
            merge_keys=tuple(merge_keys),
            partition_by=(),
            schema_evolution=True,
            update_matched=False,
        ),
    )
    context.add_output_metadata(
        {
            "delta/path": result.path,
            "delta/write_mode": result.mode,
            "delta/rows": result.rows_written,
            "delta/schema": result.schema_json,
            "delta/merge_keys": list(result.merge_keys),
            "delta/partition_by": list(result.partition_by),
            "delta/table_existed": result.table_existed,
            **{f"delta/metrics/{key}": value for key, value in result.metrics.items()},
        }
    )


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

    asset_metadata = {
        "datavault/kind": "hub",
        "datavault/sources": source_names,
        "datavault/source_columns": source_columns,
        "datavault/business_keys": list(hub.business_keys),
        "datavault/hash_key": hub.hash_key,
    }

    @dg.asset(
        key=hub.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[*hub.source_asset_keys],
        required_resource_keys={keys["spark"], keys["lakehouse"]},
        io_manager_key=NOOP_IO_MANAGER_KEY,
        metadata=asset_metadata,
        description=f"Data Vault hub {hub.table} (business keys: {list(hub.business_keys)})",
    )
    def hub_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
        lakehouse.configure_spark(spark)

        unioned = _union_sources(
            spark,
            lakehouse,
            hub.sources,
            table=hub.table,
            record_source_column=spec.record_source_column_name,
        )
        keyed = unioned.select(
            _hash_column(hub.hash, alias=hub.hash_key),
            *[F.col(column) for column in hub.business_keys],
            F.col("__source_priority"),
            F.col(spec.record_source_column_name),
        )
        result = _dedupe_first_source(keyed, hub.hash_key).select(
            hub.hash_key,
            *hub.business_keys,
            F.lit(_now()).alias(spec.load_timestamp_column_name),
            F.col(spec.record_source_column_name),
        )
        _write_table(context, spark, lakehouse, model, hub.table, result, merge_keys=[hub.hash_key])
        context.add_output_metadata(asset_metadata)
        return None

    return hub_asset


def build_link_asset(
    model: DataVaultModel, link: ResolvedLink, keys: dict[str, str]
) -> dg.AssetsDefinition:
    spec = model.specification
    source_names = [link_source.source.name for link_source in link.sources]
    source_columns = {
        link_source.source.name: dict(link_source.columns) for link_source in link.sources
    }

    asset_metadata = {
        "datavault/kind": "link",
        "datavault/sources": source_names,
        "datavault/source_columns": source_columns,
        "datavault/hubs": [relationship.hub_name for relationship in link.relationships],
        "datavault/hash_key": link.hash_key,
    }

    @dg.asset(
        key=link.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[*link.source_asset_keys, *link.parent_asset_keys],
        required_resource_keys={keys["spark"], keys["lakehouse"]},
        io_manager_key=NOOP_IO_MANAGER_KEY,
        metadata=asset_metadata,
        description=(
            "Data Vault link "
            f"{link.table} (hubs: {[relationship.hub_name for relationship in link.relationships]})"
        ),
    )
    def link_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
        lakehouse.configure_spark(spark)

        unioned = _union_sources(
            spark,
            lakehouse,
            link.sources,
            table=link.table,
            record_source_column=spec.record_source_column_name,
        )
        keyed = unioned.select(
            _hash_column(link.hash, alias=link.hash_key),
            *[
                _hash_column(relationship.hub_hash, alias=relationship.hub_hash_key)
                for relationship in link.relationships
            ],
            F.col("__source_priority"),
            F.col(spec.record_source_column_name),
        )
        result = _dedupe_first_source(keyed, link.hash_key).select(
            link.hash_key,
            *[relationship.hub_hash_key for relationship in link.relationships],
            F.lit(_now()).alias(spec.load_timestamp_column_name),
            F.col(spec.record_source_column_name),
        )
        _write_table(
            context, spark, lakehouse, model, link.table, result, merge_keys=[link.hash_key]
        )
        context.add_output_metadata(asset_metadata)
        return None

    return link_asset


def build_satellite_asset(
    model: DataVaultModel, satellite: ResolvedSatellite, keys: dict[str, str]
) -> dg.AssetsDefinition:
    spec = model.specification
    asset_metadata = {
        "datavault/kind": "satellite",
        "datavault/source": satellite.source.name,
        "datavault/parent": f"{satellite.parent_kind}:{satellite.parent_name}",
        "datavault/parent_source_columns": dict(satellite.parent_source_columns),
        "datavault/attributes": list(satellite.attributes),
        "datavault/hashdiff": satellite.hashdiff_column,
    }

    @dg.asset(
        key=satellite.asset_key,
        group_name=model.vault_name,
        kinds={"spark", "delta", "datavault"},
        deps=[satellite.source.asset_key, satellite.parent_asset_key],
        required_resource_keys={keys["spark"], keys["lakehouse"]},
        io_manager_key=NOOP_IO_MANAGER_KEY,
        metadata=asset_metadata,
        description=(
            f"Data Vault satellite {satellite.table} on {satellite.parent_kind} "
            f"'{satellite.parent_name}'"
        ),
    )
    def satellite_asset(context: dg.AssetExecutionContext) -> Any:
        spark = getattr(context.resources, keys["spark"]).get_session()
        lakehouse = getattr(context.resources, keys["lakehouse"])
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
        _write_table(
            context,
            spark,
            lakehouse,
            model,
            satellite.table,
            result,
            merge_keys=[satellite.parent_hash_key, satellite.hashdiff_column],
        )
        context.add_output_metadata(asset_metadata)
        return None

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


__all__ = ["DataVaultFactory", "build_vault_assets"]
