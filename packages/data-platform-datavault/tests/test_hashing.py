"""Pure-Python hashing rules that mirror the Spark expressions."""

from __future__ import annotations

import hashlib
from datetime import date

from data_platform_datavault.hashing import hash_payload, hash_value, hash_values_by_column


def test_md5_matches_hashlib() -> None:
    assert hash_value(["STORE-000001"]) == hashlib.md5(b"STORE-000001").hexdigest().upper()


def test_values_are_trimmed_and_uppercased_by_default() -> None:
    assert hash_value([" store-1 "]) == hash_value(["STORE-1"])


def test_normalization_can_be_disabled() -> None:
    assert hash_value(["abc"], normalize=False) != hash_value(["ABC"])


def test_null_placeholder_and_delimiter() -> None:
    assert hash_payload([None]) == "^^"
    assert hash_payload(["A", None, "B"]) == "A||^^||B"
    assert hash_payload(["A", "B"], delimiter="|") == "A|B"


def test_prefix_is_part_of_the_payload() -> None:
    assert hash_payload(["x"], prefix="HUB_STORE") == "HUB_STORE||X"
    assert hash_payload(["x"], prefix="A") != hash_payload(["x"], prefix="B")


def test_booleans_and_dates_normalize_like_spark() -> None:
    assert hash_payload([True]) == "TRUE"
    assert hash_payload([False]) == "FALSE"
    assert hash_payload([date(2026, 1, 1)]) == "2026-01-01"


def test_hash_values_by_column_uses_named_columns() -> None:
    row = {"store_id": "store-1", "region": "emea"}
    assert hash_values_by_column(row, ["store_id"]) == hash_value(["store-1"])
    assert hash_values_by_column(row, ["store_id", "region"]) == hash_value(["store-1", "emea"])
    assert hash_values_by_column(row, ["missing"]) == hash_value([None])
