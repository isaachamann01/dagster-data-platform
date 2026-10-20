"""Record-level (pydantic) validation tests."""

from __future__ import annotations

from data_platform_common.contracts import parse_contract_document
from data_platform_common.validation.records import validate_rows

CONTRACT = parse_contract_document(
    {
        "name": "records",
        "columns": [
            {"name": "id", "type": "string", "primary_key": True},
            {
                "name": "amount",
                "type": "decimal",
                "constraints": {"precision": 8, "scale": 2},
            },
            {"name": "when", "type": "date", "nullable": False},
            {"name": "score", "type": "double", "constraints": {"min_value": 0, "max_value": 1}},
            {
                "name": "tags",
                "type": "array",
                "constraints": {"max_items": 2},
                "items": {"name": "tag", "type": "string", "constraints": {"max_length": 3}},
            },
            {
                "name": "details",
                "type": "struct",
                "fields": [
                    {
                        "name": "rank",
                        "type": "integer",
                        "constraints": {"min_value": 1, "max_value": 3},
                    }
                ],
            },
        ],
    }
)

GOOD_ROW = {
    "id": "X-1",
    "amount": "12.34",
    "when": "2026-01-02",
    "score": 0.5,
    "tags": ["ab", "cd"],
    "details": {"rank": 2},
}


def test_valid_row_passes() -> None:
    assert validate_rows(CONTRACT, [GOOD_ROW]) == []


def test_decimal_scale_violation_is_reported() -> None:
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "amount": "12.345"}])
    assert failures
    assert any("amount" in failure for failure in failures)


def test_array_length_and_item_constraints() -> None:
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "tags": ["a", "b", "c"]}])
    assert any("tags" in failure for failure in failures)
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "tags": ["toolong"]}])
    assert any("tags" in failure for failure in failures)


def test_nested_struct_constraints() -> None:
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "details": {"rank": 9}}])
    assert any("details.rank" in failure for failure in failures)


def test_missing_required_and_wrong_type() -> None:
    failures = validate_rows(CONTRACT, [{"id": "X-1"}])
    assert any("when" in failure for failure in failures)
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "score": 5}])
    assert any("score" in failure for failure in failures)


def test_unexpected_fields_rejected() -> None:
    failures = validate_rows(CONTRACT, [{**GOOD_ROW, "extra": 1}])
    assert any("extra" in failure for failure in failures)
