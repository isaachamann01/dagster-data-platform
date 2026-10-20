"""Example transformations referenced from defs.yaml by dotted import path."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_data(df: DataFrame) -> DataFrame:
    """Trim names, null out-of-range scores, and deduplicate on the primary key."""

    result = df
    if "name" in result.columns:
        result = result.withColumn("name", F.trim(F.col("name")))
    if "score" in result.columns:
        result = result.withColumn(
            "score",
            F.when((F.col("score") < 0) | (F.col("score") > 100), F.lit(None)).otherwise(
                F.col("score")
            ),
        )
    if "entity_id" in result.columns:
        result = result.dropDuplicates(["entity_id"])
    return result


def add_score_band(df: DataFrame) -> DataFrame:
    """Derive a categorical score band for the gold layer."""

    if "score" not in df.columns:
        return df
    band = (
        F.when(F.col("score").isNull(), F.lit(None))
        .when(F.col("score") >= 80, F.lit("high"))
        .when(F.col("score") >= 50, F.lit("medium"))
        .otherwise(F.lit("low"))
    )
    return df.withColumn("score_band", band)


def add_order_total(df: DataFrame) -> DataFrame:
    """Derive the order total from quantity and unit price."""

    if "quantity" not in df.columns or "unit_price" not in df.columns:
        return df
    return df.withColumn(
        "total_price",
        (F.col("quantity").cast("double") * F.col("unit_price").cast("double")).cast("double"),
    )


def titlecase_store_name(df: DataFrame) -> DataFrame:
    """Example of a pipeline-local transform constrained to the store contract."""

    if "store_name" not in df.columns:
        return df
    return df.withColumn("store_name", F.initcap(F.trim(F.col("store_name"))))
