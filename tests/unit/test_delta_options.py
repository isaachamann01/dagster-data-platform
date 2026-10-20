"""Delta write option safety tests (no Spark session required)."""

from __future__ import annotations

import pytest

from data_platform_common.errors import MergeKeysRequiredError
from data_platform_common.io.delta import DeltaWriteOptions, WriteMode


def test_merge_without_keys_is_rejected() -> None:
    with pytest.raises(MergeKeysRequiredError):
        DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.MERGE)
    with pytest.raises(MergeKeysRequiredError):
        DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.MERGE, merge_keys=("  ",))


def test_merge_with_keys_is_accepted() -> None:
    options = DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.MERGE, merge_keys=("id",))
    assert options.merge_keys == ("id",)


def test_partition_overwrite_requires_partition_columns() -> None:
    with pytest.raises(MergeKeysRequiredError):
        DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.PARTITION_OVERWRITE)


def test_append_and_overwrite_need_no_keys() -> None:
    assert (
        DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.APPEND).mode is WriteMode.APPEND
    )
    assert (
        DeltaWriteOptions(path="file:///tmp/table", mode=WriteMode.OVERWRITE).mode
        is WriteMode.OVERWRITE
    )


def test_write_mode_values_are_stable() -> None:
    assert WriteMode("merge") is WriteMode.MERGE
    assert WriteMode("partition_overwrite").value == "partition_overwrite"
