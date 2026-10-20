"""Dependency graph and related batch generation tests."""

from __future__ import annotations

import pytest

from data_platform_common.contracts import ContractRegistry, parse_contract_document
from data_platform_common.errors import CircularDependencyError, MissingDependencyError
from data_platform_common.synthetic import (
    dependency_closure,
    dependency_graph,
    generate_related,
    topological_order,
)


def parent_contract() -> dict:
    return {
        "name": "parent",
        "columns": [
            {"name": "parent_id", "type": "string", "primary_key": True},
            {"name": "label", "type": "string"},
        ],
    }


def child_contract() -> dict:
    return {
        "name": "child",
        "columns": [
            {"name": "child_id", "type": "string", "primary_key": True},
            {
                "name": "parent_id",
                "type": "string",
                "references": {"contract": "parent", "columns": ["parent_id"]},
            },
        ],
    }


@pytest.fixture
def contracts() -> dict:
    registry = ContractRegistry()
    return {
        "parent": registry.add(parse_contract_document(parent_contract())),
        "child": registry.add(parse_contract_document(child_contract())),
    }


def test_dependency_graph_and_topological_order(contracts: dict) -> None:
    graph = dependency_graph(contracts)
    assert graph["child"] == {"parent"}
    assert graph["parent"] == set()
    assert topological_order(contracts) == ["parent", "child"]
    assert dependency_closure(contracts, "child") == {"parent", "child"}


def test_missing_dependency_detected() -> None:
    child = parse_contract_document(child_contract())
    with pytest.raises(MissingDependencyError) as error:
        topological_order({"child": child})
    assert "parent" in str(error.value)


def test_cycle_detected() -> None:
    registry = ContractRegistry()
    a = registry.add(
        parse_contract_document(
            {
                "name": "a",
                "columns": [
                    {"name": "a_id", "type": "string", "primary_key": True},
                    {"name": "b_id", "type": "string", "references": {"contract": "b"}},
                ],
            }
        )
    )
    b = registry.add(
        parse_contract_document(
            {
                "name": "b",
                "columns": [
                    {"name": "b_id", "type": "string", "primary_key": True},
                    {"name": "a_id", "type": "string", "references": {"contract": "a"}},
                ],
            }
        )
    )
    with pytest.raises(CircularDependencyError):
        topological_order({"a": a, "b": b})


def test_self_reference_detected() -> None:
    contract = parse_contract_document(
        {
            "name": "selfref",
            "columns": [
                {"name": "id", "type": "string", "primary_key": True},
                {"name": "parent_ref", "type": "string", "references": {"contract": "selfref"}},
            ],
        }
    )
    with pytest.raises(CircularDependencyError):
        dependency_graph({"selfref": contract})


def test_unknown_root_rejected(contracts: dict) -> None:
    with pytest.raises(MissingDependencyError):
        dependency_closure(contracts, "nope")


def test_batch_generation_populates_foreign_keys(contracts: dict) -> None:
    batch = generate_related(contracts, "child", rows=8, seed=21)
    assert batch.order == ["parent", "child"]
    parent_keys = {row["parent_id"] for row in batch.datasets["parent"]}
    child_keys = {row["parent_id"] for row in batch.datasets["child"]}
    assert child_keys
    assert child_keys <= parent_keys


def test_batch_generation_is_deterministic(contracts: dict) -> None:
    first = generate_related(contracts, "child", rows=6, seed=3)
    second = generate_related(contracts, "child", rows=6, seed=3)
    assert first.datasets == second.datasets
    assert first.order == second.order
