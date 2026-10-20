"""Strongly typed partition configuration and partition-key propagation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Annotated, Any, Literal

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    Resolvable,
    StaticPartitionsDefinition,
)
from pydantic import BaseModel, BeforeValidator, ConfigDict, model_validator

PartitionMode = Literal[
    "unpartitioned",
    "full_snapshot",
    "static",
    "daily",
    "hourly",
    "dynamic",
    "multi",
]

DIMENSION_MODES = ("static", "daily", "hourly", "dynamic")
_TEMPLATE_PATTERN = re.compile(r"\{([a-zA-Z0-9_:]+)\}")


def _coerce_iso_temporal(value: Any) -> Any:
    """Accept date/datetime objects wherever an ISO date string is expected."""

    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


IsoDateLike = Annotated[str, BeforeValidator(_coerce_iso_temporal)]


def _required_start(value: str | None, *, hourly: bool = False) -> str:
    """Normalize a partition start into the format Dagster expects."""

    if value is None:
        raise ValueError("partition start is required")
    text = value.strip()
    if hourly and len(text) == 10:
        return f"{text}-00:00"
    return text


def _optional_end(value: str | None, *, hourly: bool = False) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if hourly and len(text) == 10:
        return f"{text}-00:00"
    return text


class PartitionDimensionConfig(Resolvable, BaseModel):
    """One dimension of an optional multi-dimensional partitioning scheme."""

    model_config = ConfigDict(extra="forbid")

    name: str
    mode: Literal["static", "daily", "hourly", "dynamic"] = "static"
    keys: list[str] = []
    start: IsoDateLike | None = None
    end: IsoDateLike | None = None
    timezone: str = "UTC"

    @model_validator(mode="after")
    def _validate_shape(self) -> PartitionDimensionConfig:
        if self.mode == "static" and not self.keys:
            raise ValueError(f"static dimension '{self.name}' requires keys")
        if self.mode in ("daily", "hourly") and self.start is None:
            raise ValueError(f"{self.mode} dimension '{self.name}' requires a start date")
        return self


class PartitioningConfig(Resolvable, BaseModel):
    """Partitioning configuration for one medallion pipeline."""

    model_config = ConfigDict(extra="forbid")

    mode: PartitionMode = "unpartitioned"
    start: IsoDateLike | None = None
    end: IsoDateLike | None = None
    timezone: str = "UTC"
    keys: list[str] = []
    dimensions: list[PartitionDimensionConfig] = []
    partition_column: str | None = None
    partition_columns: list[str] = []
    full_snapshot: bool = False

    @model_validator(mode="after")
    def _validate_shape(self) -> PartitioningConfig:
        if self.mode == "static" and not self.keys:
            raise ValueError("static partitioning requires keys")
        if self.mode in ("daily", "hourly") and self.start is None:
            raise ValueError(f"{self.mode} partitioning requires a start date")
        if self.mode == "multi":
            if not self.dimensions:
                raise ValueError("multi partitioning requires at least one dimension")
            names = [dimension.name for dimension in self.dimensions]
            if len(set(names)) != len(names):
                raise ValueError("multi partition dimension names must be unique")
        if self.mode == "full_snapshot":
            self.full_snapshot = True
        columns = list(self.partition_columns)
        if self.partition_column and self.partition_column not in columns:
            columns.insert(0, self.partition_column)
        self.partition_columns = columns
        return self

    @property
    def is_partitioned(self) -> bool:
        return self.mode not in ("unpartitioned", "full_snapshot")

    @property
    def is_full_snapshot(self) -> bool:
        return self.mode == "full_snapshot" or self.full_snapshot


def _dimension_definition(
    dimension: PartitionDimensionConfig, pipeline_name: str
) -> PartitionsDefinition:
    match dimension.mode:
        case "static":
            return StaticPartitionsDefinition(dimension.keys)
        case "daily":
            return DailyPartitionsDefinition(
                start_date=_required_start(dimension.start),
                end_date=_optional_end(dimension.end),
                timezone=dimension.timezone,
            )
        case "hourly":
            return HourlyPartitionsDefinition(
                start_date=_required_start(dimension.start, hourly=True),
                end_date=_optional_end(dimension.end, hourly=True),
                timezone=dimension.timezone,
            )
        case "dynamic":
            return DynamicPartitionsDefinition(name=f"{pipeline_name}_{dimension.name}_partitions")
    raise ValueError(f"Unsupported dimension mode: {dimension.mode}")


def build_partitions_definition(
    config: PartitioningConfig, *, pipeline_name: str
) -> PartitionsDefinition | None:
    """Translate configuration into a Dagster partitions definition."""

    match config.mode:
        case "unpartitioned" | "full_snapshot":
            return None
        case "static":
            return StaticPartitionsDefinition(config.keys)
        case "daily":
            return DailyPartitionsDefinition(
                start_date=_required_start(config.start),
                end_date=_optional_end(config.end),
                timezone=config.timezone,
            )
        case "hourly":
            return HourlyPartitionsDefinition(
                start_date=_required_start(config.start, hourly=True),
                end_date=_optional_end(config.end, hourly=True),
                timezone=config.timezone,
            )
        case "dynamic":
            return DynamicPartitionsDefinition(name=f"{pipeline_name}_partitions")
        case "multi":
            return MultiPartitionsDefinition(
                {
                    dimension.name: _dimension_definition(dimension, pipeline_name)
                    for dimension in config.dimensions
                }
            )
    raise ValueError(f"Unsupported partition mode: {config.mode}")


@dataclass
class PartitionContext:
    """Resolved partition information available to assets and source requests."""

    key: str | None = None
    dimension_keys: dict[str, str] = field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None
    timezone: str = "UTC"

    def render(self, template: str) -> str:
        """Substitute ``{partition_key}``, ``{start_date}``, ``{end_date}``, ``{dim:name}``."""

        values: dict[str, str] = {
            "partition_key": self.key or "",
            "start_date": self.start_date or self.key or "",
            "end_date": self.end_date or self.key or "",
        }
        for name, value in self.dimension_keys.items():
            values[f"dim:{name}"] = value

        def substitute(match: re.Match[str]) -> str:
            token = match.group(1)
            if token not in values:
                raise KeyError(f"Unknown partition template token '{{{token}}}'")
            return values[token]

        return _TEMPLATE_PATTERN.sub(substitute, template)

    @property
    def empty(self) -> bool:
        return self.key is None and not self.dimension_keys

    def metadata(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.key is not None:
            data["partition/key"] = self.key
        if self.start_date:
            data["partition/start_date"] = self.start_date
        if self.end_date:
            data["partition/end_date"] = self.end_date
        for name, value in self.dimension_keys.items():
            data[f"partition/dimension/{name}"] = value
        return data


def partition_context(
    context: AssetExecutionContext, config: PartitioningConfig
) -> PartitionContext:
    """Extract partition information from a Dagster execution context."""

    result = PartitionContext(timezone=config.timezone)
    if not getattr(context, "has_partition_key", False):
        return result

    partition_key: Any = context.partition_key
    if isinstance(partition_key, MultiPartitionKey):
        result.key = str(partition_key)
        result.dimension_keys = dict(partition_key.keys_by_dimension.items())
        for name, value in result.dimension_keys.items():
            dimension = next((item for item in config.dimensions if item.name == name), None)
            if dimension is not None and dimension.mode == "daily" and result.start_date is None:
                result.start_date = value
    else:
        result.key = str(partition_key)
        if config.mode == "daily" or config.mode == "hourly":
            result.start_date = str(partition_key)

    if getattr(context, "has_partition_time_window", False):
        window = context.partition_time_window
        if window is not None:
            start = window.start
            end = window.end
            result.start_date = start.isoformat()
            result.end_date = end.isoformat()
    return result
