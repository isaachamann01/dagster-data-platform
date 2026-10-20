"""Partition configuration and key propagation tests."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from data_platform_common.partitions import (
    PartitionContext,
    PartitioningConfig,
    build_partitions_definition,
    partition_context,
)


def test_unpartitioned_and_full_snapshot_have_no_definition() -> None:
    assert build_partitions_definition(PartitioningConfig(), pipeline_name="p") is None
    snapshot = PartitioningConfig(mode="full_snapshot")
    assert snapshot.is_full_snapshot is True
    assert build_partitions_definition(snapshot, pipeline_name="p") is None


def test_static_partitions() -> None:
    config = PartitioningConfig(mode="static", keys=["2026-01", "2026-02"])
    definition = build_partitions_definition(config, pipeline_name="p")
    assert definition is not None
    assert definition.get_partition_keys() == ["2026-01", "2026-02"]


def test_daily_and_hourly_partitions() -> None:
    daily = build_partitions_definition(
        PartitioningConfig(mode="daily", start="2026-01-01", timezone="UTC"), pipeline_name="p"
    )
    assert daily is not None
    keys = daily.get_partition_keys()
    assert "2026-01-01" in keys

    hourly = build_partitions_definition(
        PartitioningConfig(mode="hourly", start="2026-01-01"), pipeline_name="p"
    )
    assert hourly is not None
    assert hourly.get_partition_keys()[0] == "2026-01-01-00:00"


def test_date_objects_are_coerced_to_iso_strings() -> None:
    config = PartitioningConfig(mode="daily", start=date(2026, 1, 1))
    assert config.start == "2026-01-01"


def test_dynamic_partition_name_is_pipeline_scoped() -> None:
    definition = build_partitions_definition(
        PartitioningConfig(mode="dynamic"), pipeline_name="orders"
    )
    assert definition is not None
    assert definition.name == "orders_partitions"


def test_multi_dimensional_partitions() -> None:
    config = PartitioningConfig(
        mode="multi",
        dimensions=[
            {"name": "day", "mode": "daily", "start": "2026-01-01"},
            {"name": "region", "mode": "static", "keys": ["emea", "amer"]},
        ],
    )
    definition = build_partitions_definition(config, pipeline_name="p")
    assert definition is not None
    assert len(definition.partitions_defs) == 2


def test_invalid_configurations_rejected() -> None:
    with pytest.raises(ValidationError):
        PartitioningConfig(mode="static")
    with pytest.raises(ValidationError):
        PartitioningConfig(mode="daily")
    with pytest.raises(ValidationError):
        PartitioningConfig(mode="multi")


def test_partition_template_rendering() -> None:
    context = PartitionContext(
        key="2026-01-02",
        start_date="2026-01-02",
        end_date="2026-01-03",
        dimension_keys={"region": "emea"},
    )
    rendered = context.render(
        "generate/{partition_key}?start={start_date}&end={end_date}&r={dim:region}"
    )
    assert rendered == "generate/2026-01-02?start=2026-01-02&end=2026-01-03&r=emea"
    with pytest.raises(KeyError):
        context.render("{unknown_token}")


def test_partition_context_from_execution_context() -> None:
    fake = SimpleNamespace(
        has_partition_key=True,
        partition_key="2026-02-03",
        has_partition_time_window=False,
    )
    config = PartitioningConfig(mode="daily", start="2026-01-01")
    resolved = partition_context(fake, config)  # type: ignore[arg-type]
    assert resolved.key == "2026-02-03"
    assert resolved.start_date == "2026-02-03"
    assert resolved.metadata()["partition/key"] == "2026-02-03"


def test_partition_context_without_partitions() -> None:
    fake = SimpleNamespace(has_partition_key=False)
    resolved = partition_context(fake, PartitioningConfig())  # type: ignore[arg-type]
    assert resolved.empty
    assert resolved.render("{partition_key}") == ""
