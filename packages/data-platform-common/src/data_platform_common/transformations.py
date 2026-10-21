"""Built-in contract-aware transformations for the medallion layers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_platform_common.contracts.models import DataContract
from data_platform_common.spark_types import METADATA_COLUMN_PREFIX

RAW_COLUMN = "_raw"


def json_to_dataframe(df: DataFrame) -> DataFrame:
    """Parse the bronze ``_raw`` JSON payload column into typed columns.

    Infers the schema from a bounded sample of raw payloads, then parses every
    row with ``from_json``. Ingestion metadata columns are preserved.
    """

    if RAW_COLUMN not in df.columns:
        return df
    spark = df.sparkSession
    sample = [row[0] for row in df.select(RAW_COLUMN).limit(1000).collect() if row[0] is not None]
    metadata_columns = [
        column
        for column in df.columns
        if column.startswith(METADATA_COLUMN_PREFIX) and column != RAW_COLUMN
    ]
    if not sample:
        return df.drop(RAW_COLUMN)
    inferred = spark.read.json(spark.sparkContext.parallelize(sample)).schema
    parsed = df.withColumn("_parsed", F.from_json(F.col(RAW_COLUMN), inferred))
    projections = [F.col("_parsed.*"), *[F.col(column) for column in metadata_columns]]
    result = parsed.select(*projections)
    # Flatten nested columns produced by `_parsed.*`.
    for field in result.schema.fields:
        if field.name == "_parsed":
            raise AssertionError("unexpected nested parse column")
    return result


def drop_columns_not_in_contract(
    df: DataFrame, *, contract: DataContract | None = None
) -> DataFrame:
    """Keep only contract columns (plus metadata columns). Requires ``contract``."""

    if contract is None:
        raise ValueError("drop_columns_not_in_contract requires the 'contract' context argument")
    keep = [
        column
        for column in df.columns
        if column in contract.column_map or column.startswith(METADATA_COLUMN_PREFIX)
    ]
    return df.select(*keep)
