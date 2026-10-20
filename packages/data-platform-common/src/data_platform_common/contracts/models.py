"""Pydantic v2 models for the YAML data contract format.

A contract describes a table: its columns and logical types, key structure,
referential relationships, quality expectations, and synthetic generation
hints. The same contract drives Spark schemas, row/frame validation, Dagster
asset checks, and synthetic data generation.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class LogicalType(StrEnum):
    """Logical column types supported by contracts."""

    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    DATE = "date"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    STRUCT = "struct"
    BINARY = "binary"


NUMERIC_TYPES = frozenset(
    {
        LogicalType.INTEGER,
        LogicalType.LONG,
        LogicalType.FLOAT,
        LogicalType.DOUBLE,
        LogicalType.DECIMAL,
    }
)
INTEGER_TYPES = frozenset({LogicalType.INTEGER, LogicalType.LONG})
TEMPORAL_TYPES = frozenset({LogicalType.DATE, LogicalType.TIMESTAMP})
STRING_TYPES = frozenset({LogicalType.STRING, LogicalType.BINARY})


class BaseContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ColumnConstraints(BaseContractModel):
    """Optional, type-specific value constraints for a column."""

    min_value: float | None = None
    max_value: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    regex: str | None = None
    min_items: int | None = None
    max_items: int | None = None
    precision: int | None = None
    scale: int | None = None

    @model_validator(mode="after")
    def _validate_bounds(self) -> ColumnConstraints:
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValueError("min_value must be <= max_value")
        if (
            self.min_length is not None
            and self.max_length is not None
            and self.min_length > self.max_length
        ):
            raise ValueError("min_length must be <= max_length")
        if (
            self.min_items is not None
            and self.max_items is not None
            and self.min_items > self.max_items
        ):
            raise ValueError("min_items must be <= max_items")
        for name in ("min_length", "max_length", "min_items", "max_items", "precision", "scale"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be >= 0")
        if self.regex is not None:
            try:
                re.compile(self.regex)
            except re.error as error:  # pragma: no cover - message only
                raise ValueError(f"invalid regex '{self.regex}': {error}") from error
        if (self.precision is None) != (self.scale is None):
            raise ValueError("precision and scale must be provided together")
        if self.precision is not None and self.scale is not None and self.scale > self.precision:
            raise ValueError("scale must be <= precision")
        return self


class SyntheticHints(BaseContractModel):
    """Hints that steer synthetic generation for one column."""

    distribution: Literal["uniform", "normal", "sequential"] = "uniform"
    null_probability: float | None = None
    min_value: float | None = None
    max_value: float | None = None
    mean: float | None = None
    stddev: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    choices: list[Any] | None = None
    start_date: date | None = None
    end_date: date | None = None
    prefix: str | None = None
    sequence_start: int = 1

    @field_validator("null_probability")
    @classmethod
    def _check_probability(cls, value: float | None) -> float | None:
        if value is not None and not 0.0 <= value <= 1.0:
            raise ValueError("null_probability must be between 0 and 1")
        return value

    @field_validator("choices")
    @classmethod
    def _check_choices(cls, value: list[Any] | None) -> list[Any] | None:
        if value is not None and not value:
            raise ValueError("choices must not be empty")
        return value


class ForeignKeyRef(BaseContractModel):
    """A reference to a parent contract and (optionally) its key columns.

    An empty ``columns`` list means "the parent contract's primary key".
    """

    contract: str
    columns: list[str] = Field(default_factory=list)

    @field_validator("columns")
    @classmethod
    def _check_columns(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value):
            raise ValueError("foreign key reference columns must be unique")
        return value


class ColumnContract(BaseContractModel):
    """One column (or nested field) of a table contract."""

    name: str
    type: LogicalType
    description: str | None = None
    required: bool = True
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Any | None = None
    allowed_values: list[Any] | None = None
    constraints: ColumnConstraints = Field(default_factory=ColumnConstraints)
    fields: list[ColumnContract] | None = None
    items: ColumnContract | None = None
    references: ForeignKeyRef | None = None
    synthetic: SyntheticHints | None = None

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not NAME_PATTERN.match(value):
            raise ValueError(
                f"column name '{value}' must match {NAME_PATTERN.pattern} "
                "(metadata columns are prefixed with '_' and cannot be declared)"
            )
        return value

    @field_validator("allowed_values")
    @classmethod
    def _check_allowed_values(cls, value: list[Any] | None) -> list[Any] | None:
        if value is not None:
            if not value:
                raise ValueError("allowed_values must not be empty")
            seen = [repr(item) for item in value]
            if len(set(seen)) != len(seen):
                raise ValueError("allowed_values must be unique")
        return value

    @model_validator(mode="after")
    def _validate_shape(self) -> ColumnContract:
        if self.type is LogicalType.STRUCT:
            if not self.fields:
                raise ValueError(f"struct column '{self.name}' requires non-empty fields")
            if self.items is not None:
                raise ValueError(f"struct column '{self.name}' cannot declare items")
            names = [field.name for field in self.fields]
            if len(set(names)) != len(names):
                raise ValueError(f"struct column '{self.name}' has duplicate field names")
        elif self.type is LogicalType.ARRAY:
            if self.items is None:
                raise ValueError(f"array column '{self.name}' requires an items definition")
            if self.fields is not None:
                raise ValueError(f"array column '{self.name}' cannot declare fields")
        elif self.fields is not None or self.items is not None:
            raise ValueError(
                f"only array/struct columns may declare fields/items (column '{self.name}')"
            )

        if self.type is LogicalType.DECIMAL and (
            self.constraints.precision is None or self.constraints.scale is None
        ):
            raise ValueError(f"decimal column '{self.name}' requires precision and scale")

        if (
            self.constraints.regex is not None
            and self.synthetic is not None
            and self.synthetic.distribution == "sequential"
        ):
            sample = f"{self.synthetic.prefix or ''}{1:06d}"
            if re.search(self.constraints.regex, sample) is None:
                raise ValueError(
                    f"sequential synthetic values for '{self.name}' (e.g. {sample!r}) do not "
                    f"match regex {self.constraints.regex!r}"
                )

        if self.default is not None:
            if not value_matches_type(self, self.default):
                raise ValueError(
                    f"default {self.default!r} does not match type "
                    f"{self.type.value} for '{self.name}'"
                )
            if self.allowed_values is not None and self.default not in self.allowed_values:
                raise ValueError(
                    f"default {self.default!r} is not in allowed_values for '{self.name}'"
                )
        if self.allowed_values is not None:
            invalid = [
                value for value in self.allowed_values if not value_matches_type(self, value)
            ]
            if invalid:
                raise ValueError(
                    f"allowed_values for '{self.name}' contain values incompatible with "
                    f"{self.type.value}: {invalid}"
                )
        return self


class ForeignKey(BaseContractModel):
    """A multi-column foreign key from this contract to a parent contract."""

    columns: list[str] = Field(min_length=1)
    references: ForeignKeyRef

    @field_validator("columns")
    @classmethod
    def _check_columns(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value):
            raise ValueError("foreign key columns must be unique")
        return value


class ExpectationType(StrEnum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    RANGE = "range"
    REGEX = "regex"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    ROW_COUNT_MIN = "row_count_min"
    EXPRESSION = "expression"


class Expectation(BaseContractModel):
    """An explicit data-quality expectation beyond column constraints."""

    name: str
    type: ExpectationType
    columns: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    severity: Literal["warn", "error"] = "error"

    @model_validator(mode="after")
    def _validate_shape(self) -> Expectation:
        if (
            self.type
            in {
                ExpectationType.NOT_NULL,
                ExpectationType.UNIQUE,
                ExpectationType.ACCEPTED_VALUES,
                ExpectationType.RANGE,
                ExpectationType.REGEX,
                ExpectationType.MIN_LENGTH,
                ExpectationType.MAX_LENGTH,
            }
            and not self.columns
        ):
            raise ValueError(
                f"expectation '{self.name}' of type {self.type.value} requires columns"
            )
        if self.type is ExpectationType.EXPRESSION and not self.params.get("expression"):
            raise ValueError(f"expectation '{self.name}' requires params.expression")
        if self.type is ExpectationType.ROW_COUNT_MIN and "min" not in self.params:
            raise ValueError(f"expectation '{self.name}' requires params.min")
        if self.type is ExpectationType.RANGE and not ({"min", "max"} & set(self.params)):
            raise ValueError(f"expectation '{self.name}' requires params.min and/or params.max")
        if self.type is ExpectationType.ACCEPTED_VALUES and not self.params.get("values"):
            raise ValueError(f"expectation '{self.name}' requires params.values")
        return self


class ContractSyntheticHints(BaseContractModel):
    """Contract-level synthetic generation defaults."""

    default_rows: int = Field(default=10, ge=0)
    null_probability: float | None = None
    seed: int | None = None

    @field_validator("null_probability")
    @classmethod
    def _check_probability(cls, value: float | None) -> float | None:
        if value is not None and not 0.0 <= value <= 1.0:
            raise ValueError("null_probability must be between 0 and 1")
        return value


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _coerce_temporal(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            if "T" in text or ":" in text:
                return datetime.fromisoformat(text)
            return date.fromisoformat(text)
        except ValueError:
            return None
    return None


def value_matches_type(column: ColumnContract, value: Any) -> bool:
    """Loose type check used for contract self-validation.

    Deliberately permissive: it rejects clearly wrong literals while accepting
    strings for temporal types and dicts/lists for nested types.
    """

    if value is None:
        return column.nullable
    match column.type:
        case LogicalType.STRING:
            return isinstance(value, str)
        case LogicalType.BINARY:
            return isinstance(value, (bytes, bytearray, str))
        case LogicalType.BOOLEAN:
            return isinstance(value, bool)
        case LogicalType.INTEGER | LogicalType.LONG:
            return isinstance(value, int) and not isinstance(value, bool)
        case LogicalType.FLOAT | LogicalType.DOUBLE:
            return _is_number(value)
        case LogicalType.DECIMAL:
            return _is_number(value)
        case LogicalType.DATE | LogicalType.TIMESTAMP:
            return _coerce_temporal(value) is not None
        case LogicalType.ARRAY:
            return isinstance(value, list)
        case LogicalType.STRUCT:
            return isinstance(value, dict)
    raise AssertionError(f"Unhandled logical type: {column.type}")


class DataContract(BaseContractModel):
    """A complete table contract."""

    name: str
    version: str = "1.0"
    table: str | None = None
    layer: Literal["bronze", "silver", "gold"] | None = None
    description: str | None = None
    owner: str | None = None
    columns: list[ColumnContract] = Field(min_length=1)
    primary_key: list[str] = Field(default_factory=list)
    unique_keys: list[list[str]] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    expectations: list[Expectation] = Field(default_factory=list)
    synthetic: ContractSyntheticHints = Field(default_factory=ContractSyntheticHints)

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not NAME_PATTERN.match(value):
            raise ValueError(f"contract name '{value}' must match {NAME_PATTERN.pattern}")
        return value

    @model_validator(mode="after")
    def _validate_contract(self) -> DataContract:
        names = [column.name for column in self.columns]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate column names: {duplicates}")

        derived_pk = [column.name for column in self.columns if column.primary_key]
        merged_pk = list(dict.fromkeys([*self.primary_key, *derived_pk]))
        for name in merged_pk:
            column = self.get_column(name)
            if column is None:
                raise ValueError(f"primary key column '{name}' is not defined")
            column.nullable = False
            if column.type in {LogicalType.ARRAY, LogicalType.STRUCT}:
                raise ValueError(f"primary key column '{name}' cannot be an array/struct")
            column.unique = True
        self.primary_key = merged_pk

        for key in self.unique_keys:
            for name in key:
                if self.get_column(name) is None:
                    raise ValueError(f"unique key column '{name}' is not defined")
        for column in self.columns:
            if (
                column.unique
                and column.name not in self.primary_key
                and [column.name] not in self.unique_keys
            ):
                self.unique_keys.append([column.name])

        merged_fks = list(self.foreign_keys)
        for column in self.columns:
            if column.references is not None:
                merged_fks.append(ForeignKey(columns=[column.name], references=column.references))
        deduped: dict[tuple[tuple[str, ...], str, tuple[str, ...]], ForeignKey] = {}
        for fk in merged_fks:
            for name in fk.columns:
                if self.get_column(name) is None:
                    raise ValueError(f"foreign key column '{name}' is not defined")
            fk_key = (tuple(fk.columns), fk.references.contract, tuple(fk.references.columns))
            deduped[fk_key] = fk
        self.foreign_keys = list(deduped.values())

        for expectation in self.expectations:
            for name in expectation.columns:
                if self.get_column(name) is None:
                    raise ValueError(
                        f"expectation '{expectation.name}' references undefined column '{name}'"
                    )
            if expectation.type is ExpectationType.RANGE:
                for bound in ("min", "max"):
                    if bound in expectation.params and not _is_number(expectation.params[bound]):
                        raise ValueError(
                            f"expectation '{expectation.name}' params.{bound} must be numeric"
                        )
            if expectation.type is ExpectationType.ROW_COUNT_MIN and not isinstance(
                expectation.params["min"], int
            ):
                raise ValueError(f"expectation '{expectation.name}' params.min must be an integer")
        return self

    @property
    def table_name(self) -> str:
        return self.table or self.name

    @property
    def column_map(self) -> dict[str, ColumnContract]:
        return {column.name: column for column in self.columns}

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]

    @property
    def primary_key_columns(self) -> list[str]:
        return list(self.primary_key)

    def get_column(self, path: str) -> ColumnContract | None:
        """Look up a column by name, supporting ``parent.child`` dotted paths."""

        parts = path.split(".")
        current: ColumnContract | None = None
        columns = self.columns
        for index, part in enumerate(parts):
            current = next((column for column in columns if column.name == part), None)
            if current is None:
                return None
            if index < len(parts) - 1:
                if current.type is LogicalType.STRUCT and current.fields:
                    columns = current.fields
                else:
                    return None
        return current

    def iter_leaf_columns(self, prefix: str = "") -> list[tuple[str, ColumnContract]]:
        """All scalar leaf columns with dotted paths for nested structs."""

        leaves: list[tuple[str, ColumnContract]] = []
        for column in self.columns:
            path = f"{prefix}{column.name}"
            if column.type is LogicalType.STRUCT and column.fields:
                leaves.extend(self._iter_fields(column.fields, f"{path}."))
            elif column.type is LogicalType.ARRAY and column.items is not None:
                continue
            else:
                leaves.append((path, column))
        return leaves

    @staticmethod
    def _iter_fields(fields: list[ColumnContract], prefix: str) -> list[tuple[str, ColumnContract]]:
        leaves: list[tuple[str, ColumnContract]] = []
        for field in fields:
            path = f"{prefix}{field.name}"
            if field.type is LogicalType.STRUCT and field.fields:
                leaves.extend(DataContract._iter_fields(field.fields, f"{path}."))
            elif field.type is LogicalType.ARRAY:
                continue
            else:
                leaves.append((path, field))
        return leaves


ColumnContract.model_rebuild()
