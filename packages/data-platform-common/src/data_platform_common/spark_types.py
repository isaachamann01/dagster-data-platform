"""Spark schema derivation and contract-driven type casting."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform_common.contracts.models import ColumnContract, DataContract, LogicalType

METADATA_COLUMN_PREFIX = "_"


def spark_type_for(column: ColumnContract) -> DataType:
    """Map a contract column to its canonical Spark type."""

    match column.type:
        case LogicalType.STRING:
            return StringType()
        case LogicalType.BINARY:
            return BinaryType()
        case LogicalType.BOOLEAN:
            return BooleanType()
        case LogicalType.INTEGER:
            return IntegerType()
        case LogicalType.LONG:
            return LongType()
        case LogicalType.FLOAT:
            return FloatType()
        case LogicalType.DOUBLE:
            return DoubleType()
        case LogicalType.DECIMAL:
            assert column.constraints.precision is not None and column.constraints.scale is not None
            return DecimalType(column.constraints.precision, column.constraints.scale)
        case LogicalType.DATE:
            return DateType()
        case LogicalType.TIMESTAMP:
            return TimestampType()
        case LogicalType.ARRAY:
            assert column.items is not None
            return ArrayType(spark_type_for(column.items), containsNull=True)
        case LogicalType.STRUCT:
            assert column.fields is not None
            return StructType(
                [
                    StructField(field.name, spark_type_for(field), field.nullable)
                    for field in column.fields
                ]
            )
    raise ValueError(f"Unsupported logical type: {column.type}")


def spark_schema_for(contract: DataContract) -> StructType:
    """The full Spark schema for a contract."""

    return StructType(
        [
            StructField(column.name, spark_type_for(column), column.nullable)
            for column in contract.columns
        ]
    )


def _cast_expression(column: ColumnContract) -> F.Column:
    name = column.name
    source = F.col(name)
    target = spark_type_for(column)

    match column.type:
        case LogicalType.DATE:
            return F.coalesce(source.cast("date"), F.to_date(F.to_timestamp(source)))
        case LogicalType.TIMESTAMP:
            return F.coalesce(source.cast("timestamp"), F.to_timestamp(source))
        case LogicalType.ARRAY | LogicalType.STRUCT:
            assert isinstance(target, (T.ArrayType, T.StructType))
            return F.when(source.isNull(), F.lit(None)).otherwise(
                F.from_json(F.to_json(source), target)
            )
        case LogicalType.DECIMAL:
            return source.cast(target)
        case _:
            return source.cast(target)


def cast_dataframe(
    df: DataFrame, contract: DataContract, *, include_missing: bool = True
) -> DataFrame:
    """Cast a DataFrame to a contract's schema.

    Present columns are cast to their canonical Spark type. Missing nullable
    columns are added as typed nulls; missing columns with defaults are filled
    with their default value; missing non-nullable columns without a default are
    left absent so contract checks can flag them.
    """

    projections: list[F.Column] = []
    present = set(df.columns)
    for column in contract.columns:
        if column.name in present:
            projections.append(_cast_expression(column).alias(column.name))
            continue
        if not include_missing:
            continue
        if column.default is not None:
            projections.append(
                F.lit(column.default).cast(spark_type_for(column)).alias(column.name)
            )
        elif column.nullable or not column.required:
            projections.append(F.lit(None).cast(spark_type_for(column)).alias(column.name))
        # else: required + non-nullable + no default -> left missing for checks

    extras = [name for name in df.columns if name not in contract.column_map]
    for name in extras:
        projections.append(F.col(name).alias(name))
    return df.select(*projections)


def is_metadata_column(name: str) -> bool:
    return name.startswith(METADATA_COLUMN_PREFIX)
