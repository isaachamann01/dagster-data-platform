"""Dagster IO manager that persists Spark DataFrames as Delta tables.

Write behaviour comes from asset definition metadata set by the MedallionFactory
(``delta/table``, ``delta/write_mode``, ``delta/merge_keys``, ...), so the same
IO manager serves bronze, silver, and gold assets.
"""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableIOManager, InputContext, OutputContext

from data_platform_common.io.delta import DeltaTableWriter, DeltaWriteOptions, WriteMode
from data_platform_common.resources.lakehouse import LakehouseResource
from data_platform_common.resources.spark import SparkResource

META_LAYER = "delta/layer"
META_TABLE = "delta/table"
META_PATH = "delta/path"
META_WRITE_MODE = "delta/write_mode"
META_MERGE_KEYS = "delta/merge_keys"
META_PARTITION_BY = "delta/partition_by"
META_SCHEMA_EVOLUTION = "delta/schema_evolution"
META_UPDATE_MATCHED = "delta/update_matched"


def _metadata_of(context: OutputContext) -> dict[str, Any]:
    return dict(context.definition_metadata or {})


class DeltaLakeIOManager(ConfigurableIOManager):
    """Reads and writes Spark DataFrames against Delta Lake storage."""

    spark: SparkResource
    lakehouse: LakehouseResource

    def _path_for(self, metadata: dict[str, Any]) -> str:
        if META_PATH in metadata:
            return str(metadata[META_PATH])
        layer = str(metadata.get(META_LAYER, "silver"))
        table = metadata.get(META_TABLE)
        if not table:
            raise ValueError(
                f"Asset is missing '{META_TABLE}' metadata; cannot resolve a Delta path"
            )
        return self.lakehouse.table_path(layer, str(table))

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        from pyspark.sql import DataFrame

        if not isinstance(obj, DataFrame):
            raise TypeError(
                f"DeltaLakeIOManager expected a pyspark DataFrame, got {type(obj).__name__}"
            )
        metadata = _metadata_of(context)
        path = self._path_for(metadata)
        mode = WriteMode(str(metadata.get(META_WRITE_MODE, WriteMode.OVERWRITE.value)))
        merge_keys = tuple(str(key) for key in metadata.get(META_MERGE_KEYS, ()) or ())
        partition_by = tuple(str(key) for key in metadata.get(META_PARTITION_BY, ()) or ())
        schema_evolution = bool(metadata.get(META_SCHEMA_EVOLUTION, True))
        update_matched = bool(metadata.get(META_UPDATE_MATCHED, True))

        spark = self.spark.get_session()
        self.lakehouse.configure_spark(spark)
        writer = DeltaTableWriter(spark)
        result = writer.write(
            obj,
            DeltaWriteOptions(
                path=path,
                mode=mode,
                merge_keys=merge_keys,
                partition_by=partition_by,
                schema_evolution=schema_evolution,
                update_matched=update_matched,
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
        metadata_context = getattr(context, "resources", None)
        metadata_resource = (
            metadata_context.metadata
            if metadata_context is not None and hasattr(metadata_context, "metadata")
            else None
        )
        if (
            metadata_resource is not None
            and getattr(metadata_resource, "is_configured", lambda: False)()
        ):
            metadata_resource.record(
                "table_written",
                {
                    "asset": str(context.asset_key),
                    "path": result.path,
                    "mode": result.mode,
                    "rows": result.rows_written,
                },
            )

    def load_input(self, context: InputContext) -> Any:
        upstream = context.upstream_output
        if upstream is None:
            raise ValueError("load_input requires an upstream output context")
        metadata = dict(upstream.definition_metadata or {})
        path = self._path_for(metadata)
        spark = self.spark.get_session()
        self.lakehouse.configure_spark(spark)
        return spark.read.format("delta").load(path)
