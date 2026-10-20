"""Delta Lake write engine with merge, append, overwrite, and partition overwrite."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from pyspark.sql import DataFrame, SparkSession

from data_platform_common.errors import MergeKeysRequiredError


class WriteMode(StrEnum):
    """Supported Delta write modes."""

    MERGE = "merge"
    APPEND = "append"
    OVERWRITE = "overwrite"
    PARTITION_OVERWRITE = "partition_overwrite"


@dataclass(frozen=True)
class DeltaWriteOptions:
    """Everything the writer needs to safely persist one dataset."""

    path: str
    mode: WriteMode = WriteMode.OVERWRITE
    merge_keys: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    schema_evolution: bool = True
    # Insert-only merges (hubs/links/satellites) leave matched rows untouched.
    update_matched: bool = True

    def __post_init__(self) -> None:
        if self.mode is WriteMode.MERGE:
            if not self.merge_keys:
                raise MergeKeysRequiredError(
                    f"merge mode requires merge keys for {self.path!r}; refusing to fall back "
                    "to overwrite because that would silently replace the whole table"
                )
            if any(not key.strip() for key in self.merge_keys):
                raise MergeKeysRequiredError("merge keys must be non-empty column names")
        if self.mode is WriteMode.PARTITION_OVERWRITE and not self.partition_by:
            raise MergeKeysRequiredError(
                "partition_overwrite requires at least one partition column"
            )


@dataclass
class WriteResult:
    """Metadata about one completed write."""

    path: str
    mode: str
    rows_written: int
    schema_json: str
    merge_keys: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    metrics: dict[str, str] = field(default_factory=dict)
    table_existed: bool = False


class DeltaTableWriter:
    """Writes Spark DataFrames to Delta tables."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def table_exists(self, path: str) -> bool:
        from delta.tables import DeltaTable

        try:
            return DeltaTable.isDeltaTable(self._spark, path)
        except Exception:  # noqa: BLE001 - unreadable path counts as "not a delta table"
            return False

    def write(self, df: DataFrame, options: DeltaWriteOptions) -> WriteResult:
        present = set(df.columns)
        missing_keys = [key for key in options.merge_keys if key not in present]
        if missing_keys:
            raise MergeKeysRequiredError(
                f"merge keys {missing_keys} are not columns of the incoming frame; "
                f"available: {sorted(present)}"
            )
        missing_partitions = [column for column in options.partition_by if column not in present]
        if missing_partitions:
            raise ValueError(f"partition columns {missing_partitions} are not columns of the frame")

        existed = self.table_exists(options.path)
        if options.mode is WriteMode.MERGE:
            metrics = self._merge(df, options, existed)
        elif options.mode is WriteMode.APPEND:
            metrics = self._append(df, options)
        elif options.mode is WriteMode.OVERWRITE:
            metrics = self._overwrite(df, options)
        elif options.mode is WriteMode.PARTITION_OVERWRITE:
            metrics = self._overwrite_partitions(df, options)
        else:  # pragma: no cover - exhaustive enum
            raise ValueError(f"Unsupported write mode: {options.mode}")

        schema_json = self._spark.read.format("delta").load(options.path).schema.json()
        return WriteResult(
            path=options.path,
            mode=options.mode.value,
            rows_written=df.count(),
            schema_json=schema_json,
            merge_keys=options.merge_keys,
            partition_by=options.partition_by,
            metrics=metrics,
            table_existed=existed,
        )

    # -- modes --------------------------------------------------------------

    def _write_kwargs(self, options: DeltaWriteOptions) -> dict[str, str]:
        kwargs: dict[str, str] = {}
        if options.schema_evolution:
            kwargs["overwriteSchema"] = "true"
        return kwargs

    def _append(self, df: DataFrame, options: DeltaWriteOptions) -> dict[str, str]:
        writer = df.write.format("delta").mode("append")
        if options.partition_by:
            writer = writer.partitionBy(*options.partition_by)
        writer.save(options.path)
        return {}

    def _overwrite(self, df: DataFrame, options: DeltaWriteOptions) -> dict[str, str]:
        writer = df.write.format("delta").mode("overwrite")
        for key, value in self._write_kwargs(options).items():
            writer = writer.option(key, value)
        if options.partition_by:
            writer = writer.partitionBy(*options.partition_by)
        writer.save(options.path)
        return {}

    def _overwrite_partitions(self, df: DataFrame, options: DeltaWriteOptions) -> dict[str, str]:
        # Dynamic partition overwrite replaces only the partitions present in the
        # incoming frame, which keeps reruns idempotent without touching history.
        # Delta rejects `overwriteSchema` in this mode, so schema evolution uses
        # `mergeSchema`, which adds new columns to existing partitions as nulls.
        self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            writer = df.write.format("delta").mode("overwrite")
            if options.schema_evolution:
                writer = writer.option("mergeSchema", "true")
            writer = writer.partitionBy(*options.partition_by)
            writer.save(options.path)
        finally:
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        return {}

    def _merge(
        self, df: DataFrame, options: DeltaWriteOptions, table_existed: bool
    ) -> dict[str, str]:
        from delta.tables import DeltaTable

        keys = list(options.merge_keys)
        # Deduplicate inside the incoming batch first: merging duplicate keys in a
        # single pass can produce multiple updates/inserts for the same target row.
        source = df.dropDuplicates(keys)

        if not table_existed:
            writer = source.write.format("delta").mode("errorifexists")
            if options.partition_by:
                writer = writer.partitionBy(*options.partition_by)
            writer.save(options.path)
            return {"created": "true"}

        if options.schema_evolution:
            self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        try:
            target = DeltaTable.forPath(self._spark, options.path)
            condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)
            builder = target.alias("target").merge(source.alias("source"), condition)
            if options.schema_evolution:
                with_schema_evolution = getattr(builder, "withSchemaEvolution", None)
                if callable(with_schema_evolution):
                    builder = with_schema_evolution()
            if options.update_matched:
                builder = builder.whenMatchedUpdateAll()
            builder = builder.whenNotMatchedInsertAll()
            raw_metrics = builder.execute()
        finally:
            if options.schema_evolution:
                self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

        metrics: dict[str, str] = {}
        if isinstance(raw_metrics, dict):
            metrics = {str(key): str(value) for key, value in raw_metrics.items()}
        return metrics


def write_dataframe(
    df: DataFrame,
    *,
    path: str,
    mode: WriteMode | str = WriteMode.OVERWRITE,
    merge_keys: list[str] | tuple[str, ...] = (),
    partition_by: list[str] | tuple[str, ...] = (),
    schema_evolution: bool = True,
) -> WriteResult:
    """One-shot helper used by tests and scripts."""

    spark = df.sparkSession
    return DeltaTableWriter(spark).write(
        df,
        DeltaWriteOptions(
            path=path,
            mode=WriteMode(mode),
            merge_keys=tuple(merge_keys),
            partition_by=tuple(partition_by),
            schema_evolution=schema_evolution,
        ),
    )
