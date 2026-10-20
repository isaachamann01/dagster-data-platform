"""Asset construction for a single contract-driven medallion pipeline.

Every component declaration produces up to three assets:

- ``[pipeline, bronze, table]``: REST retrieval plus ingestion metadata
- ``[pipeline, silver, table]``: parsed, transformed, contract-validated Delta table
- ``[pipeline, gold, table]``: curated, transformed, contract-validated Delta table

Asset keys are namespaced by pipeline name so multiple independently configured
pipelines can coexist in one code location without collisions.

Note: this module intentionally avoids ``from __future__ import annotations`` so
Dagster decorators receive real annotation objects for ``context``.
"""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from data_platform_common.checks import ForeignKeyParent, build_foreign_key_resolver
from data_platform_common.contracts.models import DataContract
from data_platform_common.errors import ContractError, ContractValidationError
from data_platform_common.imports import Transformation
from data_platform_common.io.delta import WriteMode
from data_platform_common.io.manager import (
    META_LAYER,
    META_MERGE_KEYS,
    META_PARTITION_BY,
    META_SCHEMA_EVOLUTION,
    META_TABLE,
    META_WRITE_MODE,
)
from data_platform_common.medallion.config import (
    ResourceKeysSpec,
    RestSourceSpec,
    WriteModesSpec,
)
from data_platform_common.partitions import (
    PartitioningConfig,
    build_partitions_definition,
    partition_context,
)
from data_platform_common.spark_types import METADATA_COLUMN_PREFIX, cast_dataframe
from data_platform_common.validation.frame import ValidationContext, run_frame_validation

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("_raw", T.StringType(), nullable=False),
        T.StructField("_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_source", T.StringType(), nullable=False),
        T.StructField("_pipeline", T.StringType(), nullable=False),
        T.StructField("_partition_key", T.StringType(), nullable=True),
    ]
)


@dataclass
class MedallionPipeline:
    """All resolved inputs needed to build one pipeline's Dagster objects."""

    pipeline_name: str
    source: RestSourceSpec
    silver_contract: DataContract
    gold_contract: DataContract | None
    merge_keys: list[str]
    silver_transformations: list[Transformation]
    gold_transformations: list[Transformation]
    partitioning: PartitioningConfig
    write_modes: WriteModesSpec
    resource_keys: ResourceKeysSpec
    foreign_key_parents: Mapping[str, ForeignKeyParent] = field(default_factory=dict)
    extra_columns: list[str] = field(default_factory=list)
    schema_evolution: bool = True

    # -- names --------------------------------------------------------------

    @property
    def bronze_table(self) -> str:
        return self.source.table_name or self.pipeline_name

    @property
    def silver_table(self) -> str:
        return self.silver_contract.table_name

    @property
    def gold_table(self) -> str | None:
        return self.gold_contract.table_name if self.gold_contract else None

    @property
    def partitions_def(self) -> dg.PartitionsDefinition | None:
        return build_partitions_definition(self.partitioning, pipeline_name=self.pipeline_name)

    def asset_key(self, layer: str, table: str) -> dg.AssetKey:
        return dg.AssetKey([self.pipeline_name, layer, table])

    @property
    def bronze_key(self) -> dg.AssetKey:
        return self.asset_key("bronze", self.bronze_table)

    @property
    def silver_key(self) -> dg.AssetKey:
        return self.asset_key("silver", self.silver_table)

    @property
    def gold_key(self) -> dg.AssetKey | None:
        return self.asset_key("gold", self.gold_table) if self.gold_table else None

    # -- write behaviour -----------------------------------------------------

    def effective_merge_keys(self, layer: str) -> list[str]:
        if layer == "gold" and self.gold_contract is not None and self.merge_keys:
            return list(self.merge_keys)
        if layer == "silver":
            return list(self.merge_keys)
        return []

    def resolve_write_mode(self, layer: str) -> WriteMode:
        explicit = getattr(self.write_modes, layer, None)
        if explicit:
            return WriteMode(explicit)
        if layer == "bronze":
            return (
                WriteMode.PARTITION_OVERWRITE
                if self.partitioning.partition_columns
                else WriteMode.OVERWRITE
            )
        if self.effective_merge_keys(layer):
            return WriteMode.MERGE
        if self.partitioning.partition_columns:
            return WriteMode.PARTITION_OVERWRITE
        return WriteMode.OVERWRITE

    def table_metadata(self, layer: str, table: str) -> dict[str, Any]:
        if layer == "bronze":
            # Raw ingestion is partitioned by the resolved partition key column
            # (when partitioned) rather than by contract columns.
            partition_by = ["_partition_key"] if self.partitioning.is_partitioned else []
        else:
            partition_by = list(self.partitioning.partition_columns)
        metadata: dict[str, Any] = {
            META_LAYER: layer,
            META_TABLE: table,
            META_WRITE_MODE: self.resolve_write_mode(layer).value,
            META_MERGE_KEYS: list(self.effective_merge_keys(layer)),
            META_PARTITION_BY: partition_by,
            META_SCHEMA_EVOLUTION: self.schema_evolution,
        }
        return metadata

    def validate_configuration(self) -> None:
        """Fail fast on invalid contract/config combinations at load time."""

        contract = self.gold_contract or self.silver_contract
        for key in self.merge_keys:
            if contract.get_column(key) is None:
                raise ContractError(
                    f"pipeline '{self.pipeline_name}': merge key '{key}' is not a column of "
                    f"contract '{contract.name}'"
                )
        target = self.gold_contract or self.silver_contract
        for column in self.partitioning.partition_columns:
            if target.get_column(column) is None:
                raise ContractError(
                    f"pipeline '{self.pipeline_name}': partition column '{column}' is not a "
                    f"column of contract '{target.name}'"
                )
        if self.partitioning.is_partitioned and not self.partitioning.partition_columns:
            # Logical partitioning (metadata/paths) is allowed, but surface the
            # decision so it is never accidental.
            pass

    def parent_asset_keys(self) -> list[dg.AssetKey]:
        return [
            parent.asset_key
            for parent in self.foreign_key_parents.values()
            if parent.asset_key is not None
        ]


def _extract_records(payload: Any, response_path: str | None) -> list[dict[str, Any]]:
    records: Any = payload
    if response_path:
        for part in response_path.split("."):
            if isinstance(records, dict) and part in records:
                records = records[part]
            else:
                raise ContractError(
                    f"response_path '{response_path}' does not exist in the payload"
                )
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        raise ContractError(
            f"Expected the REST payload to be a list of records, got {type(records).__name__}"
        )
    for record in records:
        if not isinstance(record, dict):
            raise ContractError(
                f"Expected each record to be a JSON object, got {type(record).__name__}"
            )
    return records


def _validation_failure(outcomes: list[Any], contract: DataContract) -> None:
    failures = [outcome for outcome in outcomes if outcome.failed and outcome.severity == "error"]
    if failures:
        raise ContractValidationError(contract.name, [outcome.as_failure() for outcome in failures])


def _resolve_foreign_keys(
    pipeline: MedallionPipeline, spark: Any, lakehouse: Any
) -> Callable[[Any], DataFrame] | None:
    if not pipeline.foreign_key_parents:
        return None
    return build_foreign_key_resolver(
        pipeline.foreign_key_parents, spark=spark, lakehouse=lakehouse
    )


def _build_bronze(pipeline: MedallionPipeline) -> dg.AssetsDefinition:
    keys = pipeline.resource_keys

    @dg.asset(
        key=pipeline.bronze_key,
        group_name=pipeline.pipeline_name,
        kinds={"rest", "delta"},
        partitions_def=pipeline.partitions_def,
        required_resource_keys={keys.rest, keys.spark, keys.lakehouse, keys.metadata},
        io_manager_key=keys.io_manager,
        metadata=pipeline.table_metadata("bronze", pipeline.bronze_table),
        description=(
            f"Raw REST ingestion for pipeline '{pipeline.pipeline_name}' from route "
            f"'{pipeline.source.route}'"
        ),
    )
    def bronze(context: dg.AssetExecutionContext) -> Any:
        partition = partition_context(context, pipeline.partitioning)
        rest = getattr(context.resources, keys.rest)
        spark_resource = getattr(context.resources, keys.spark)
        lakehouse = getattr(context.resources, keys.lakehouse)
        metadata_resource = getattr(context.resources, keys.metadata)

        route = partition.render(pipeline.source.route)
        params = {key: partition.render(value) for key, value in pipeline.source.params.items()}
        payload = rest.fetch_json(
            route,
            params=params,
            method=pipeline.source.method,
            json_body=pipeline.source.request_body,
        )
        records = _extract_records(payload, pipeline.source.response_path)
        ingested_at = datetime.now(UTC)

        rows = [
            {
                "_raw": json.dumps(record, default=str),
                "_ingested_at": ingested_at,
                "_source": route,
                "_pipeline": pipeline.pipeline_name,
                "_partition_key": partition.key,
            }
            for record in records
        ]
        spark = spark_resource.get_session()
        lakehouse.configure_spark(spark)
        frame = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

        context.add_output_metadata(
            {
                "source/route": route,
                "source/records": len(rows),
                **partition.metadata(),
            }
        )
        metadata_resource.record(
            "bronze_ingested",
            {
                "pipeline": pipeline.pipeline_name,
                "route": route,
                "records": len(rows),
                "partition": partition.key,
            },
        )
        return frame

    return bronze


def _apply_transformations(
    frame: DataFrame,
    transformations: list[Transformation],
    *,
    contract: DataContract,
    layer: str,
    partition: Any,
    spark: Any,
    pipeline_name: str,
    extra: Mapping[str, Any] | None = None,
) -> DataFrame:
    result = frame
    for transformation in transformations:
        result = transformation.apply(
            result,
            contract=contract,
            partition=partition,
            layer=layer,
            spark=spark,
            pipeline_name=pipeline_name,
            metadata=extra or {},
        )
    return result


def _build_silver(pipeline: MedallionPipeline) -> dg.AssetsDefinition:
    keys = pipeline.resource_keys
    contract = pipeline.silver_contract

    @dg.asset(
        key=pipeline.silver_key,
        group_name=pipeline.pipeline_name,
        kinds={"spark", "delta"},
        partitions_def=pipeline.partitions_def,
        ins={"bronze": dg.AssetIn(key=pipeline.bronze_key)},
        deps=pipeline.parent_asset_keys() or None,
        required_resource_keys={keys.spark, keys.lakehouse, keys.metadata},
        io_manager_key=keys.io_manager,
        metadata=pipeline.table_metadata("silver", pipeline.silver_table),
        description=f"Silver Delta table for pipeline '{pipeline.pipeline_name}'",
    )
    def silver(context: dg.AssetExecutionContext, bronze: Any) -> Any:
        partition = partition_context(context, pipeline.partitioning)
        spark_resource = getattr(context.resources, keys.spark)
        lakehouse = getattr(context.resources, keys.lakehouse)
        spark = spark_resource.get_session()
        lakehouse.configure_spark(spark)

        frame = _apply_transformations(
            bronze,
            pipeline.silver_transformations,
            contract=contract,
            layer="silver",
            partition=partition,
            spark=spark,
            pipeline_name=pipeline.pipeline_name,
        )
        frame = cast_dataframe(frame, contract)
        outcomes = run_frame_validation(
            frame,
            contract,
            context=ValidationContext(
                contract=contract,
                foreign_key_resolver=_resolve_foreign_keys(pipeline, spark, lakehouse),
                extra_columns=frozenset(pipeline.extra_columns),
            ),
        )
        _validation_failure(outcomes, contract)
        context.add_output_metadata(
            {
                "contract": contract.name,
                "contract/checks": len(outcomes),
                "contract/failed": len([outcome for outcome in outcomes if outcome.failed]),
                **partition.metadata(),
            }
        )
        return frame

    return silver


def _build_gold(pipeline: MedallionPipeline) -> dg.AssetsDefinition:
    assert pipeline.gold_contract is not None
    keys = pipeline.resource_keys
    contract = pipeline.gold_contract

    @dg.asset(
        key=pipeline.gold_key,
        group_name=pipeline.pipeline_name,
        kinds={"spark", "delta"},
        partitions_def=pipeline.partitions_def,
        ins={"silver": dg.AssetIn(key=pipeline.silver_key)},
        deps=pipeline.parent_asset_keys() or None,
        required_resource_keys={keys.spark, keys.lakehouse, keys.metadata},
        io_manager_key=keys.io_manager,
        metadata=pipeline.table_metadata("gold", contract.table_name),
        description=f"Gold curated Delta table for pipeline '{pipeline.pipeline_name}'",
    )
    def gold(context: dg.AssetExecutionContext, silver: Any) -> Any:
        partition = partition_context(context, pipeline.partitioning)
        spark_resource = getattr(context.resources, keys.spark)
        lakehouse = getattr(context.resources, keys.lakehouse)
        spark = spark_resource.get_session()
        lakehouse.configure_spark(spark)

        frame = _apply_transformations(
            silver,
            pipeline.gold_transformations,
            contract=contract,
            layer="gold",
            partition=partition,
            spark=spark,
            pipeline_name=pipeline.pipeline_name,
        )
        frame = cast_dataframe(frame, contract)
        outcomes = run_frame_validation(
            frame,
            contract,
            context=ValidationContext(
                contract=contract,
                foreign_key_resolver=_resolve_foreign_keys(pipeline, spark, lakehouse),
                extra_columns=frozenset(pipeline.extra_columns),
            ),
        )
        _validation_failure(outcomes, contract)
        context.add_output_metadata(
            {
                "contract": contract.name,
                "contract/checks": len(outcomes),
                "contract/failed": len([outcome for outcome in outcomes if outcome.failed]),
                **partition.metadata(),
            }
        )
        return frame

    return gold


def build_pipeline_assets(pipeline: MedallionPipeline) -> list[dg.AssetsDefinition]:
    """Build the bronze/silver/gold assets for one pipeline definition."""

    pipeline.validate_configuration()
    assets = [_build_bronze(pipeline), _build_silver(pipeline)]
    if pipeline.gold_contract is not None:
        assets.append(_build_gold(pipeline))
    return assets


__all__ = [
    "BRONZE_SCHEMA",
    "MEDALLION_METADATA_PREFIX",
    "MedallionPipeline",
    "build_pipeline_assets",
]

MEDALLION_METADATA_PREFIX = METADATA_COLUMN_PREFIX
