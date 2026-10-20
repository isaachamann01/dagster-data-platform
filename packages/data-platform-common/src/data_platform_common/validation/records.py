"""Pydantic v2 row validation derived from a data contract.

Builds a dynamic model per contract, including nested structs and arrays, then
validates record batches. This is the record-level counterpart to the Spark
frame checks and is used by tests and the synthetic generator self-validation.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from functools import partial
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    create_model,
)

from data_platform_common.contracts.models import ColumnContract, DataContract, LogicalType


def _allowed_check(value: Any, *, allowed: list[Any]) -> Any:
    if value is None:
        return value
    for candidate in allowed:
        if candidate == value:
            return value
    raise ValueError(f"value {value!r} is not one of {allowed!r}")


def _annotation_for(column: ColumnContract) -> Any:
    constraints = column.constraints
    base: Any
    match column.type:
        case LogicalType.STRING:
            base = Annotated[
                str,
                StringConstraints(
                    min_length=constraints.min_length,
                    max_length=constraints.max_length,
                    pattern=constraints.regex,
                ),
            ]
        case LogicalType.BINARY:
            base = bytes
        case LogicalType.BOOLEAN:
            base = bool
        case LogicalType.INTEGER | LogicalType.LONG:
            base = Annotated[int, Field(ge=constraints.min_value, le=constraints.max_value)]
        case LogicalType.FLOAT | LogicalType.DOUBLE:
            base = Annotated[float, Field(ge=constraints.min_value, le=constraints.max_value)]
        case LogicalType.DECIMAL:
            assert constraints.precision is not None and constraints.scale is not None
            base = Annotated[
                Decimal,
                Field(
                    max_digits=constraints.precision,
                    decimal_places=constraints.scale,
                    ge=Decimal(str(constraints.min_value))
                    if constraints.min_value is not None
                    else None,
                    le=Decimal(str(constraints.max_value))
                    if constraints.max_value is not None
                    else None,
                ),
            ]
        case LogicalType.DATE:
            base = date
        case LogicalType.TIMESTAMP:
            base = datetime
        case LogicalType.ARRAY:
            assert column.items is not None
            item = _annotation_for(column.items)
            base = Annotated[
                list[item],  # type: ignore[valid-type]
                Field(
                    min_length=constraints.min_items,
                    max_length=constraints.max_items,
                ),
            ]
        case LogicalType.STRUCT:
            assert column.fields is not None
            base = _build_struct_model(column.name, column.fields)
        case _:  # pragma: no cover - exhaustive
            raise ValueError(f"Unsupported logical type: {column.type}")

    if column.allowed_values is not None:
        base = Annotated[
            base, AfterValidator(partial(_allowed_check, allowed=column.allowed_values))
        ]
    return base


def _build_struct_model(name: str, fields: list[ColumnContract]) -> type[BaseModel]:
    definitions: dict[str, tuple[Any, Any]] = {}
    for field_spec in fields:
        annotation: Any = _annotation_for(field_spec)
        default: Any = ...
        if not field_spec.required or field_spec.nullable:
            default = field_spec.default
            annotation = annotation | None
        definitions[field_spec.name] = (annotation, default)
    return create_model(  # type: ignore[call-overload]
        f"{name}_struct",
        __config__=ConfigDict(extra="forbid"),
        **definitions,
    )


def build_row_model(contract: DataContract) -> type[BaseModel]:
    """Build a dynamic pydantic model that validates one row of a contract."""

    definitions: dict[str, tuple[Any, Any]] = {}
    for column in contract.columns:
        annotation: Any = _annotation_for(column)
        default: Any = ...
        if not column.required or column.nullable:
            default = column.default
            annotation = annotation | None
        definitions[column.name] = (annotation, default)
    return create_model(  # type: ignore[call-overload]
        f"{contract.name}_row",
        __config__=ConfigDict(extra="forbid"),
        **definitions,
    )


def validate_rows(contract: DataContract, rows: list[dict[str, Any]]) -> list[str]:
    """Validate rows, returning human-readable failure messages."""

    model = build_row_model(contract)
    failures: list[str] = []
    for index, row in enumerate(rows):
        try:
            model.model_validate(row)
        except ValidationError as error:
            for issue in error.errors():
                location = ".".join(str(part) for part in issue["loc"])
                failures.append(f"row {index} field '{location}': {issue['msg']}")
    return failures
