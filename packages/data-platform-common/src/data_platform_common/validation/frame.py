"""Contract-driven Spark DataFrame validation.

Every contract expands into a list of :class:`CheckSpec` objects. The same
specs drive both the validation gate in silver/gold assets and the Dagster
asset checks, so the two can never drift apart.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform_common.contracts.models import (
    ColumnContract,
    DataContract,
    Expectation,
    ExpectationType,
    ForeignKey,
    LogicalType,
)
from data_platform_common.spark_types import is_metadata_column, spark_type_for
from data_platform_common.validation.results import SEVERITY_ERROR, SEVERITY_WARN, CheckOutcome

CheckEvaluator = Callable[[DataFrame, "ValidationContext"], CheckOutcome]


@dataclass
class ValidationContext:
    """Everything a check needs beyond the frame itself."""

    contract: DataContract
    foreign_key_resolver: Callable[[ForeignKey], DataFrame] | None = None
    extra_columns: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class CheckSpec:
    """A named, evaluatable contract check."""

    name: str
    description: str
    severity: str
    evaluate: CheckEvaluator

    def run(self, df: DataFrame, context: ValidationContext) -> CheckOutcome:
        outcome = self.evaluate(df, context)
        if outcome.severity != self.severity:
            return CheckOutcome(
                name=outcome.name,
                passed=outcome.passed,
                message=outcome.message,
                severity=self.severity,
                metadata=outcome.metadata,
            )
        return outcome


def _display(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return repr(value)


def _sample_rows(
    df: DataFrame, condition: F.Column, columns: Sequence[str], limit: int = 5
) -> list[list[Any]]:
    rows = df.where(condition).select(*columns).limit(limit).collect()
    return [[row[column] for column in columns] for row in rows]


def _count(df: DataFrame, condition: F.Column) -> int:
    return df.where(condition).count()


def _any_of(conditions: list[F.Column]) -> F.Column:
    """Logical OR across Spark column conditions (PySpark has no `or_` helper)."""

    result = conditions[0]
    for condition in conditions[1:]:
        result = result | condition
    return result


def _literal(column: ColumnContract, value: Any) -> F.Column:
    if isinstance(value, str) and column.type in (LogicalType.DATE, LogicalType.TIMESTAMP):
        spark_type = "date" if column.type is LogicalType.DATE else "timestamp"
        return F.lit(value).cast(spark_type)
    return F.lit(value)


def _pass(name: str, message: str, **metadata: Any) -> CheckOutcome:
    return CheckOutcome(name=name, passed=True, message=message, metadata=metadata)


def _fail(name: str, message: str, **metadata: Any) -> CheckOutcome:
    return CheckOutcome(name=name, passed=False, message=message, metadata=metadata)


# --------------------------------------------------------------------------- #
# Individual checks
# --------------------------------------------------------------------------- #


def _missing_columns_check(contract: DataContract) -> CheckEvaluator:
    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = "missing_columns"
        required = [column.name for column in contract.columns if column.required]
        missing = [column for column in required if column not in df.columns]
        if missing:
            return _fail(name, f"missing required columns: {missing}", missing_columns=missing)
        return _pass(name, "all required columns present")

    return evaluate


def _unexpected_columns_check(contract: DataContract) -> CheckEvaluator:
    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = "unexpected_columns"
        allowed = set(contract.column_names) | set(context.extra_columns)
        unexpected = [
            column
            for column in df.columns
            if column not in allowed and not is_metadata_column(column)
        ]
        if unexpected:
            return _fail(name, f"unexpected columns: {unexpected}", unexpected_columns=unexpected)
        return _pass(name, "no unexpected columns")

    return evaluate


def _types_compatible(expected: T.DataType, actual: T.DataType) -> bool:
    if isinstance(expected, T.StructType) and isinstance(actual, T.StructType):
        expected_fields = {field.name: field.dataType for field in expected.fields}
        actual_fields = {field.name: field.dataType for field in actual.fields}
        for name, expected_type in expected_fields.items():
            actual_type = actual_fields.get(name)
            if actual_type is None or not _types_compatible(expected_type, actual_type):
                return False
        return True
    if isinstance(expected, T.ArrayType) and isinstance(actual, T.ArrayType):
        return _types_compatible(expected.elementType, actual.elementType)
    if isinstance(expected, T.DecimalType) and isinstance(actual, T.DecimalType):
        return expected.precision == actual.precision and expected.scale == actual.scale
    if isinstance(expected, (T.LongType, T.IntegerType)) and isinstance(
        actual, (T.LongType, T.IntegerType)
    ):
        return True
    if isinstance(expected, (T.DoubleType, T.FloatType)) and isinstance(
        actual, (T.DoubleType, T.FloatType)
    ):
        return True
    return type(expected) is type(actual)


def _column_types_check(contract: DataContract) -> CheckEvaluator:
    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = "column_types"
        mismatches: dict[str, str] = {}
        for column in contract.columns:
            if column.name not in df.columns:
                continue
            expected = spark_type_for(column)
            actual = df.schema[column.name].dataType
            if not _types_compatible(expected, actual):
                mismatches[column.name] = (
                    f"expected {expected.simpleString()}, got {actual.simpleString()}"
                )
        if mismatches:
            detail = "; ".join(f"{column}: {reason}" for column, reason in mismatches.items())
            return _fail(name, f"column type mismatches: {detail}", mismatches=mismatches)
        return _pass(name, "all column types match the contract")

    return evaluate


def _nullability_check(contract: DataContract) -> CheckEvaluator:
    non_nullable = [
        (path, column) for path, column in contract.iter_leaf_columns() if not column.nullable
    ]

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = "nullability"
        violations: dict[str, int] = {}
        for path, _column in non_nullable:
            if path not in df.columns:
                continue
            count = _count(df, F.col(path).isNull())
            if count:
                violations[path] = count
        if violations:
            detail = ", ".join(f"{column}: {count} nulls" for column, count in violations.items())
            return _fail(
                name, f"non-nullable columns contain nulls: {detail}", violations=violations
            )
        return _pass(name, "no nulls in non-nullable columns")

    return evaluate


def _not_empty_check(_contract: DataContract) -> CheckEvaluator:
    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = "not_empty"
        empty = df.limit(1).count() == 0
        if empty:
            return _fail(name, "dataset is empty")
        return _pass(name, "dataset is not empty")

    return evaluate


def _key_uniqueness_check(name: str, keys: Sequence[str], description: str) -> CheckEvaluator:
    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        present = [key for key in keys if key in df.columns]
        if len(present) != len(keys):
            missing = sorted(set(keys) - set(present))
            return _fail(name, f"cannot evaluate uniqueness, missing key columns: {missing}")
        duplicates = df.groupBy(*keys).count().where(F.col("count") > 1)
        duplicate_count = duplicates.count()
        if duplicate_count:
            samples = _sample_rows(duplicates, F.lit(True), [*keys, "count"])
            return _fail(
                name,
                f"{duplicate_count} duplicate key combination(s), e.g. "
                + "; ".join("(" + ", ".join(_display(v) for v in row) + ")" for row in samples),
                duplicate_groups=duplicate_count,
            )
        return _pass(name, f"{description} is unique")

    return evaluate


def _allowed_values_check(path: str, column: ColumnContract) -> CheckEvaluator:
    allowed = list(column.allowed_values or [])

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = f"allowed_values__{path}"
        if path not in df.columns:
            return _pass(name, "column not present; skipped")
        condition = F.col(path).isNotNull() & ~F.col(path).isin(*allowed)
        invalid_count = _count(df, condition)
        if invalid_count:
            samples = _sample_rows(df, condition, [path])
            return _fail(
                name,
                f"{invalid_count} value(s) outside allowed values {allowed}: "
                + ", ".join(_display(row[0]) for row in samples),
                invalid_count=invalid_count,
                allowed_values=allowed,
            )
        return _pass(name, f"all values within {allowed}")

    return evaluate


def _range_check(path: str, column: ColumnContract) -> CheckEvaluator:
    minimum = column.constraints.min_value
    maximum = column.constraints.max_value

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = f"range__{path}"
        if path not in df.columns:
            return _pass(name, "column not present; skipped")
        conditions = []
        if minimum is not None:
            conditions.append(F.col(path) < _literal(column, minimum))
        if maximum is not None:
            conditions.append(F.col(path) > _literal(column, maximum))
        condition = F.col(path).isNotNull() & _any_of(conditions)
        invalid_count = _count(df, condition)
        if invalid_count:
            samples = _sample_rows(df, condition, [path])
            bounds = f"min={minimum}, max={maximum}"
            return _fail(
                name,
                f"{invalid_count} value(s) outside [{bounds}]: "
                + ", ".join(_display(row[0]) for row in samples),
                invalid_count=invalid_count,
            )
        return _pass(name, f"values within [{minimum}, {maximum}]")

    return evaluate


def _length_check(path: str, column: ColumnContract) -> CheckEvaluator:
    minimum = column.constraints.min_length
    maximum = column.constraints.max_length

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = f"length__{path}"
        if path not in df.columns:
            return _pass(name, "column not present; skipped")
        conditions = []
        if minimum is not None:
            conditions.append(F.length(F.col(path)) < minimum)
        if maximum is not None:
            conditions.append(F.length(F.col(path)) > maximum)
        condition = F.col(path).isNotNull() & _any_of(conditions)
        invalid_count = _count(df, condition)
        if invalid_count:
            samples = _sample_rows(df, condition, [path])
            return _fail(
                name,
                f"{invalid_count} value(s) outside length bounds "
                f"[{minimum}, {maximum}]: " + ", ".join(_display(row[0]) for row in samples),
                invalid_count=invalid_count,
            )
        return _pass(name, f"string lengths within [{minimum}, {maximum}]")

    return evaluate


def _regex_check(path: str, column: ColumnContract) -> CheckEvaluator:
    pattern = column.constraints.regex

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        name = f"regex__{path}"
        if path not in df.columns or pattern is None:
            return _pass(name, "column not present; skipped")
        condition = F.col(path).isNotNull() & ~F.col(path).rlike(pattern)
        invalid_count = _count(df, condition)
        if invalid_count:
            samples = _sample_rows(df, condition, [path])
            return _fail(
                name,
                f"{invalid_count} value(s) not matching /{pattern}/: "
                + ", ".join(_display(row[0]) for row in samples),
                invalid_count=invalid_count,
                regex=pattern,
            )
        return _pass(name, f"all values match /{pattern}/")

    return evaluate


def _foreign_key_check(foreign_key: ForeignKey) -> CheckEvaluator:
    keys = list(foreign_key.columns)
    name = f"foreign_key__{'+'.join(keys)}"

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        if context.foreign_key_resolver is None:
            return _pass(name, "no foreign-key resolver configured; skipped")
        missing_child = [key for key in keys if key not in df.columns]
        if missing_child:
            return _fail(name, f"child key columns missing: {missing_child}")
        # The resolver returns the parent frame already projected onto the child
        # key column names, so referencing contracts with different column names
        # (or empty references meaning "parent primary key") works uniformly.
        parent = context.foreign_key_resolver(foreign_key)
        missing_parent = [key for key in keys if key not in parent.columns]
        if missing_parent:
            return _fail(name, f"resolved parent frame is missing key columns: {missing_parent}")
        parent_keys = parent.select(*keys).distinct()
        child_keys = df.select(*keys).where(F.col(keys[0]).isNotNull()).distinct()
        orphans = child_keys.join(parent_keys, on=keys, how="left_anti")
        orphan_count = orphans.count()
        if orphan_count:
            samples = orphans.limit(5).collect()
            return _fail(
                name,
                f"{orphan_count} orphan key(s) not present in "
                f"'{foreign_key.references.contract}': "
                + "; ".join(
                    "(" + ", ".join(_display(row[key]) for key in keys) + ")" for row in samples
                ),
                orphan_count=orphan_count,
                parent_contract=foreign_key.references.contract,
            )
        return _pass(name, f"all values exist in '{foreign_key.references.contract}'")

    return evaluate


def _expectation_check(expectation: Expectation) -> CheckEvaluator:
    name = f"expectation__{expectation.name}"
    columns = list(expectation.columns)

    def evaluate(df: DataFrame, context: ValidationContext) -> CheckOutcome:
        if expectation.type is ExpectationType.ROW_COUNT_MIN:
            minimum = int(expectation.params["min"])
            count = df.count()
            if count < minimum:
                return _fail(
                    name, f"row count {count} < required minimum {minimum}", row_count=count
                )
            return _pass(name, f"row count {count} >= {minimum}")

        if expectation.type is ExpectationType.EXPRESSION:
            expression = str(expectation.params["expression"])
            try:
                condition = ~F.expr(expression)
                invalid_count = _count(df, condition)
            except Exception as error:  # noqa: BLE001 - surfaced as a check failure
                return _fail(name, f"could not evaluate expression '{expression}': {error}")
            if invalid_count:
                return _fail(name, f"{invalid_count} row(s) violate expression '{expression}'")
            return _pass(name, f"all rows satisfy '{expression}'")

        missing = [column for column in columns if column not in df.columns]
        if missing:
            return _fail(name, f"columns missing: {missing}")

        if expectation.type is ExpectationType.NOT_NULL:
            violations = {column: _count(df, F.col(column).isNull()) for column in columns}
            violations = {column: count for column, count in violations.items() if count}
            if violations:
                return _fail(name, f"null violations: {violations}", violations=violations)
            return _pass(name, "no null values")

        if expectation.type is ExpectationType.UNIQUE:
            duplicates = df.groupBy(*columns).count().where(F.col("count") > 1)
            count = duplicates.count()
            if count:
                return _fail(name, f"{count} duplicate value group(s)", duplicate_groups=count)
            return _pass(name, "values are unique")

        if expectation.type is ExpectationType.ACCEPTED_VALUES:
            values = list(expectation.params["values"])
            condition = F.col(columns[0]).isNotNull() & ~F.col(columns[0]).isin(*values)
            invalid_count = _count(df, condition)
            if invalid_count:
                return _fail(
                    name, f"{invalid_count} value(s) outside {values}", invalid_count=invalid_count
                )
            return _pass(name, f"values within {values}")

        if expectation.type is ExpectationType.RANGE:
            conditions = []
            if "min" in expectation.params:
                conditions.append(F.col(columns[0]) < F.lit(expectation.params["min"]))
            if "max" in expectation.params:
                conditions.append(F.col(columns[0]) > F.lit(expectation.params["max"]))
            condition = F.col(columns[0]).isNotNull() & _any_of(conditions)
            invalid_count = _count(df, condition)
            if invalid_count:
                return _fail(
                    name,
                    f"{invalid_count} value(s) outside configured range",
                    invalid_count=invalid_count,
                )
            return _pass(name, "values within range")

        if expectation.type is ExpectationType.REGEX:
            pattern = str(expectation.params["pattern"])
            condition = F.col(columns[0]).isNotNull() & ~F.col(columns[0]).rlike(pattern)
            invalid_count = _count(df, condition)
            if invalid_count:
                return _fail(
                    name,
                    f"{invalid_count} value(s) not matching /{pattern}/",
                    invalid_count=invalid_count,
                )
            return _pass(name, f"values match /{pattern}/")

        if expectation.type in (ExpectationType.MIN_LENGTH, ExpectationType.MAX_LENGTH):
            bound = int(expectation.params.get("length", expectation.params.get("value", 0)))
            if expectation.type is ExpectationType.MIN_LENGTH:
                condition = F.col(columns[0]).isNotNull() & (F.length(F.col(columns[0])) < bound)
                text = f"shorter than {bound}"
            else:
                condition = F.col(columns[0]).isNotNull() & (F.length(F.col(columns[0])) > bound)
                text = f"longer than {bound}"
            invalid_count = _count(df, condition)
            if invalid_count:
                return _fail(name, f"{invalid_count} value(s) {text}", invalid_count=invalid_count)
            return _pass(name, f"no values {text}")

        raise AssertionError(f"Unhandled expectation type: {expectation.type}")

    return evaluate


# --------------------------------------------------------------------------- #
# Spec construction and execution
# --------------------------------------------------------------------------- #


def iter_check_specs(contract: DataContract) -> list[CheckSpec]:
    """Expand a contract into every check that applies to it."""

    specs: list[CheckSpec] = [
        CheckSpec(
            "missing_columns",
            f"Required columns of {contract.name} exist",
            SEVERITY_ERROR,
            _missing_columns_check(contract),
        ),
        CheckSpec(
            "unexpected_columns",
            f"{contract.name} has no undeclared columns",
            SEVERITY_ERROR,
            _unexpected_columns_check(contract),
        ),
        CheckSpec(
            "column_types",
            f"Columns of {contract.name} match contract types",
            SEVERITY_ERROR,
            _column_types_check(contract),
        ),
        CheckSpec(
            "nullability",
            f"Non-nullable columns of {contract.name} contain no nulls",
            SEVERITY_ERROR,
            _nullability_check(contract),
        ),
        CheckSpec(
            "not_empty",
            f"{contract.name} dataset is not empty",
            SEVERITY_ERROR,
            _not_empty_check(contract),
        ),
    ]
    if contract.primary_key:
        specs.append(
            CheckSpec(
                "primary_key_unique",
                f"Primary key of {contract.name} is unique",
                SEVERITY_ERROR,
                _key_uniqueness_check("primary_key_unique", contract.primary_key, "primary key"),
            )
        )
    for index, key in enumerate(contract.unique_keys):
        specs.append(
            CheckSpec(
                f"unique__{'+'.join(key)}",
                f"Unique key #{index + 1} of {contract.name} is unique",
                SEVERITY_ERROR,
                _key_uniqueness_check(f"unique__{'+'.join(key)}", key, "unique key"),
            )
        )
    for path, column in contract.iter_leaf_columns():
        if column.allowed_values is not None:
            specs.append(
                CheckSpec(
                    f"allowed_values__{path}",
                    f"'{path}' only contains allowed values",
                    SEVERITY_ERROR,
                    _allowed_values_check(path, column),
                )
            )
        if column.constraints.min_value is not None or column.constraints.max_value is not None:
            specs.append(
                CheckSpec(
                    f"range__{path}",
                    f"'{path}' respects numeric bounds",
                    SEVERITY_ERROR,
                    _range_check(path, column),
                )
            )
        if column.constraints.min_length is not None or column.constraints.max_length is not None:
            specs.append(
                CheckSpec(
                    f"length__{path}",
                    f"'{path}' respects string length bounds",
                    SEVERITY_ERROR,
                    _length_check(path, column),
                )
            )
        if column.constraints.regex is not None:
            specs.append(
                CheckSpec(
                    f"regex__{path}",
                    f"'{path}' matches its regex",
                    SEVERITY_ERROR,
                    _regex_check(path, column),
                )
            )
    for foreign_key in contract.foreign_keys:
        specs.append(
            CheckSpec(
                f"foreign_key__{'+'.join(foreign_key.columns)}",
                f"Foreign key to '{foreign_key.references.contract}' is satisfied",
                SEVERITY_ERROR,
                _foreign_key_check(foreign_key),
            )
        )
    for expectation in contract.expectations:
        severity = SEVERITY_WARN if expectation.severity == "warn" else SEVERITY_ERROR
        specs.append(
            CheckSpec(
                f"expectation__{expectation.name}",
                f"Expectation '{expectation.name}' holds",
                severity,
                _expectation_check(expectation),
            )
        )
    seen: set[str] = set()
    for spec in specs:
        if spec.name in seen:
            raise ValueError(f"Duplicate check name '{spec.name}' in contract '{contract.name}'")
        seen.add(spec.name)
    return specs


def run_frame_validation(
    df: DataFrame,
    contract: DataContract,
    *,
    context: ValidationContext | None = None,
    only: Iterable[str] | None = None,
) -> list[CheckOutcome]:
    """Run all (or a filtered subset of) checks for a contract."""

    ctx = context or ValidationContext(contract=contract)
    selected = set(only) if only is not None else None
    outcomes: list[CheckOutcome] = []
    for spec in iter_check_specs(contract):
        if selected is not None and spec.name not in selected:
            continue
        outcomes.append(spec.run(df, ctx))
    return outcomes
