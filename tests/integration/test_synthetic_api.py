"""FastAPI synthetic-data service tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import data_contracts
from synthetic_api.app import create_app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(create_app(data_contracts.CONTRACTS_DIR))


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["contracts"] == 6


def test_list_contracts(client: TestClient) -> None:
    response = client.get("/contracts")
    assert response.status_code == 200
    names = {summary["name"] for summary in response.json()}
    assert names == {
        "example_entity",
        "example_entity_gold",
        "example_store",
        "example_store_gold",
        "example_order",
        "example_order_gold",
    }


def test_contract_detail(client: TestClient) -> None:
    response = client.get("/contracts/example_entity")
    assert response.status_code == 200
    document = response.json()
    assert document["name"] == "example_entity"
    assert document["primary_key"] == ["entity_id"]


def test_unknown_contract_returns_404(client: TestClient) -> None:
    assert client.get("/contracts/nope").status_code == 404
    assert client.post("/generate/nope", json={"seed": 1}).status_code == 404


def test_generate_single_is_deterministic(client: TestClient) -> None:
    first = client.post("/generate/example_entity", json={"rows": 5, "seed": 9}).json()
    second = client.post("/generate/example_entity", json={"rows": 5, "seed": 9}).json()
    other = client.post("/generate/example_entity", json={"rows": 5, "seed": 10}).json()
    assert first["rows"] == second["rows"]
    assert first["rows"] != other["rows"]
    assert first["row_count"] == 5


def test_generate_related_order_has_foreign_key_integrity(client: TestClient) -> None:
    response = client.post("/generate", json={"contract": "example_order", "rows": 6, "seed": 4})
    assert response.status_code == 200
    body = response.json()
    assert body["order"] == ["example_store", "example_order"]
    store_ids = {row["store_id"] for row in body["datasets"]["example_store"]}
    order_store_ids = {row["store_id"] for row in body["datasets"]["example_order"]}
    assert order_store_ids
    assert order_store_ids <= store_ids


def test_generate_single_with_foreign_keys_returns_valid_rows(client: TestClient) -> None:
    response = client.post("/generate/example_order", json={"rows": 4, "seed": 2})
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert all(row["store_id"].startswith("STORE-") for row in rows)


def test_default_row_count_uses_contract_hint(client: TestClient) -> None:
    body = client.post("/generate/example_entity", json={"seed": 1}).json()
    assert body["row_count"] == 10


def test_invalid_request_body_rejected(client: TestClient) -> None:
    assert client.post("/generate/example_entity", json={"rows": -1}).status_code == 422
