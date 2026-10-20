"""Deterministic synthetic generation tests."""

from __future__ import annotations

import re

import pytest

import data_contracts
from data_platform_common.contracts import load_contract_file
from data_platform_common.synthetic import SyntheticGenerator
from data_platform_common.validation.records import validate_rows

CONTRACT = load_contract_file(data_contracts.get_example_silver_contract_path())


def test_generated_rows_are_contract_valid() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=11).generate(25)
    assert len(rows) == 25
    assert validate_rows(CONTRACT, rows) == []


def test_generation_is_deterministic_per_seed() -> None:
    first = SyntheticGenerator(CONTRACT, seed=42).generate(10)
    second = SyntheticGenerator(CONTRACT, seed=42).generate(10)
    third = SyntheticGenerator(CONTRACT, seed=43).generate(10)
    assert first == second
    assert first != third


def test_primary_keys_are_unique_and_match_regex() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=5).generate(40)
    ids = [row["entity_id"] for row in rows]
    assert len(set(ids)) == len(ids)
    assert all(re.fullmatch(r"ENT-[0-9]{6}", value) for value in ids)


def test_allowed_values_and_bounds_respected() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=7).generate(30)
    assert {row["category"] for row in rows} <= {"alpha", "beta", "gamma"}
    scores = [row["score"] for row in rows if row["score"] is not None]
    assert all(0 <= score <= 100 for score in scores)


def test_nullable_columns_can_be_null_and_required_columns_never_are() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=3, null_probability=1.0).generate(10)
    assert all(row["tags"] is None for row in rows)
    assert all(row["details"] is None for row in rows)
    assert all(row["name"] for row in rows)
    assert all(row["category"] for row in rows)


def test_arrays_and_nested_objects_respect_constraints() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=99, null_probability=0.0).generate(20)
    for row in rows:
        tags = row["tags"]
        assert isinstance(tags, list)
        assert len(tags) <= 4
        assert all(2 <= len(tag) <= 12 for tag in tags)
        details = row["details"]
        assert details["region"] in {"emea", "amer", "apac"}
        assert 1 <= details["rank"] <= 5


def test_multiple_rows_match_regex_constraint() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=123).generate(15)
    pattern = re.compile(CONTRACT.column_map["owner_email"].constraints.regex or "")
    emails = [row["owner_email"] for row in rows if row["owner_email"] is not None]
    assert emails
    assert all(pattern.match(email) for email in emails)


def test_row_count_defaults_to_contract_hint() -> None:
    rows = SyntheticGenerator(CONTRACT, seed=0).generate()
    assert len(rows) == CONTRACT.synthetic.default_rows


def test_unknown_regex_construct_raises_clear_error() -> None:
    from data_platform_common.contracts import parse_contract_document
    from data_platform_common.errors import SyntheticDataError

    contract = parse_contract_document(
        {
            "name": "tricky",
            "columns": [
                {
                    "name": "value",
                    "type": "string",
                    "constraints": {"regex": "^(?=a)b$"},
                }
            ],
        }
    )
    with pytest.raises(SyntheticDataError):
        SyntheticGenerator(contract, seed=1).generate(1)
