"""Contract model validation tests."""

from __future__ import annotations

from datetime import date

import pytest

from data_platform_common.contracts import (
    ContractRegistry,
    DataContract,
    LogicalType,
    parse_contract_document,
)
from data_platform_common.errors import ContractError


def base_contract(**overrides: object) -> dict:
    payload = {
        "name": "sample",
        "columns": [
            {"name": "id", "type": "string", "primary_key": True},
            {"name": "label", "type": "string"},
        ],
    }
    payload.update(overrides)
    return payload


def test_primary_key_flags_merge_and_force_non_null() -> None:
    contract = parse_contract_document(base_contract())
    assert contract.primary_key == ["id"]
    column = contract.column_map["id"]
    assert column.nullable is False
    assert column.unique is True


def test_contract_level_primary_key_is_respected() -> None:
    contract = parse_contract_document(
        base_contract(
            primary_key=["id"],
            columns=[
                {"name": "id", "type": "long"},
                {"name": "other", "type": "string"},
            ],
        )
    )
    assert contract.primary_key == ["id"]
    assert contract.column_map["id"].nullable is False


def test_duplicate_columns_rejected() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(
                columns=[
                    {"name": "id", "type": "string"},
                    {"name": "id", "type": "long"},
                ]
            )
        )


def test_array_requires_items() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(base_contract(columns=[{"name": "tags", "type": "array"}]))


def test_struct_requires_fields() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(base_contract(columns=[{"name": "details", "type": "struct"}]))


def test_decimal_requires_precision_and_scale() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(base_contract(columns=[{"name": "amount", "type": "decimal"}]))
    contract = parse_contract_document(
        base_contract(
            columns=[
                {
                    "name": "amount",
                    "type": "decimal",
                    "constraints": {"precision": 10, "scale": 2},
                }
            ]
        )
    )
    assert contract.column_map["amount"].type is LogicalType.DECIMAL


def test_invalid_regex_rejected() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(
                columns=[{"name": "id", "type": "string", "constraints": {"regex": "(["}}]
            )
        )


def test_allowed_values_must_match_type() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(
                columns=[{"name": "count", "type": "integer", "allowed_values": ["one", "two"]}]
            )
        )


def test_default_must_match_type_and_allowed_values() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(columns=[{"name": "flag", "type": "boolean", "default": "yes"}])
        )
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(
                columns=[
                    {
                        "name": "color",
                        "type": "string",
                        "allowed_values": ["red", "blue"],
                        "default": "green",
                    }
                ]
            )
        )


def test_column_reference_merges_into_foreign_keys() -> None:
    contract = parse_contract_document(
        base_contract(
            columns=[
                {"name": "id", "type": "string", "primary_key": True},
                {
                    "name": "parent_id",
                    "type": "string",
                    "references": {"contract": "parent", "columns": ["id"]},
                },
            ]
        )
    )
    assert len(contract.foreign_keys) == 1
    foreign_key = contract.foreign_keys[0]
    assert foreign_key.columns == ["parent_id"]
    assert foreign_key.references.contract == "parent"


def test_nested_column_lookup_and_leaf_iteration() -> None:
    contract = parse_contract_document(
        base_contract(
            columns=[
                {"name": "id", "type": "string", "primary_key": True},
                {
                    "name": "details",
                    "type": "struct",
                    "fields": [
                        {"name": "region", "type": "string"},
                        {"name": "rank", "type": "integer"},
                    ],
                },
                {
                    "name": "tags",
                    "type": "array",
                    "items": {"name": "tag", "type": "string"},
                },
            ]
        )
    )
    assert contract.get_column("details.region") is not None
    paths = [path for path, _ in contract.iter_leaf_columns()]
    assert paths == ["id", "details.region", "details.rank"]


def test_expectation_params_are_validated() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(expectations=[{"name": "bad", "type": "row_count_min", "params": {}}])
        )


def test_registry_conflicting_duplicate_rejected() -> None:
    registry = ContractRegistry([parse_contract_document(base_contract())])
    conflicting = parse_contract_document(base_contract(description="different"))
    with pytest.raises(ContractError):
        registry.add(conflicting)


def test_sequential_synthetic_must_match_regex() -> None:
    with pytest.raises(ContractError):
        parse_contract_document(
            base_contract(
                columns=[
                    {
                        "name": "code",
                        "type": "string",
                        "constraints": {"regex": "^AB-[0-9]{4}$"},
                        "synthetic": {"distribution": "sequential", "prefix": "AB-"},
                    }
                ]
            )
        )
    contract = parse_contract_document(
        base_contract(
            columns=[
                {
                    "name": "code",
                    "type": "string",
                    "constraints": {"regex": "^AB-[0-9]{6}$"},
                    "synthetic": {"distribution": "sequential", "prefix": "AB-"},
                }
            ]
        )
    )
    assert contract.column_map["code"].synthetic is not None


def test_contract_helpers() -> None:
    contract = parse_contract_document(
        base_contract(
            columns=[
                {"name": "id", "type": "string", "primary_key": True},
                {"name": "when", "type": "date"},
            ]
        )
    )
    assert contract.table_name == "sample"
    assert contract.column_names == ["id", "when"]
    assert contract.primary_key_columns == ["id"]
    assert isinstance(contract, DataContract)


def test_date_defaults_are_coerced() -> None:
    contract = parse_contract_document(
        base_contract(
            columns=[
                {"name": "id", "type": "string", "primary_key": True},
                {"name": "day", "type": "date", "default": "2026-01-01", "nullable": False},
            ]
        )
    )
    assert contract.column_map["day"].default == "2026-01-01"
    assert date(2026, 1, 1).isoformat() == "2026-01-01"
