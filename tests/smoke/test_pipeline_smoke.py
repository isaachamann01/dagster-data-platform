"""End-to-end smoke tests.

Flow exercised: FastAPI synthetic service -> REST resource -> bronze -> silver
-> gold -> Delta Lake -> contract validation -> merge idempotency.
"""

from __future__ import annotations

from typing import Any

import dagster as dg
import pytest

import data_contracts
from data_platform_common.checks import ForeignKeyParent, build_foreign_key_resolver
from data_platform_common.contracts import load_contract_file
from data_platform_common.validation.frame import ValidationContext, run_frame_validation

pytestmark = [pytest.mark.slow, pytest.mark.smoke]

UNPARTITIONED_KEYS = [
    "example_store/bronze/example_store",
    "example_store/silver/example_store",
    "example_store/gold/example_store_gold",
    "example_order/bronze/example_order",
    "example_order/silver/example_order",
    "example_order/gold/example_order_gold",
]
ENTITY_KEYS = [
    "example_entity/bronze/example_entity",
    "example_entity/silver/example_entity",
    "example_entity/gold/example_entity_gold",
]
ENTITY_PARTITION = "2026-01-02"


def _materialize(
    defs: dg.Definitions,
    resources: dict[str, Any],
    keys: list[str],
    *,
    partition_key: str | None = None,
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        list(defs.assets or []),
        resources=resources,
        selection=[dg.AssetKey(key.split("/")) for key in keys],
        partition_key=partition_key,
        raise_on_error=True,
    )


def _error_failures(outcomes: list[Any]) -> list[Any]:
    return [outcome for outcome in outcomes if outcome.failed and outcome.severity == "error"]


def test_smoke_definitions_load_graph_and_multi_document(example_defs: dg.Definitions) -> None:
    """Every smoke run starts by loading definitions and checking the graph."""

    keys: set[str] = set()
    deps: dict[str, set[str]] = {}
    for assets in example_defs.assets or []:
        for key in assets.keys:
            keys.add(key.to_user_string())
            deps[key.to_user_string()] = {
                parent.to_user_string() for parent in assets.asset_deps.get(key, set())
            }
    assert len(keys) == 16  # three medallion pipelines (9) + two data vaults (7)
    assert "example_store/silver/example_store" in keys  # both docs of related/defs.yaml
    assert "example_order/silver/example_order" in keys
    assert "retail/hub/hub_store" in keys  # explicit Data Vault
    assert "entity_vault/sat/sat_example_entity" in keys  # auto-generated Data Vault
    assert "example_entity/bronze/example_entity" in deps["example_entity/silver/example_entity"]
    assert "example_entity/silver/example_entity" in deps["example_entity/gold/example_entity_gold"]
    assert "example_order/bronze/example_order" in deps["example_order/silver/example_order"]
    assert "example_store/silver/example_store" in deps["retail/hub/hub_store"]
    assert "retail/hub/hub_store" in deps["retail/sat/sat_store_profile"]


def test_smoke_synthetic_generation_is_deterministic(service_url: str) -> None:
    import httpx

    def generate(seed: int) -> list[dict[str, Any]]:
        response = httpx.post(
            f"{service_url}/generate/example_entity", json={"rows": 5, "seed": seed}, timeout=30
        )
        response.raise_for_status()
        return response.json()["rows"]

    assert generate(7) == generate(7)
    assert generate(7) != generate(8)


def test_smoke_synthetic_parent_child_foreign_keys(service_url: str) -> None:
    import httpx

    response = httpx.post(
        f"{service_url}/generate",
        json={"contract": "example_order", "rows": 5, "seed": 3},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    assert body["order"] == ["example_store", "example_order"]
    parent_keys = {row["store_id"] for row in body["datasets"]["example_store"]}
    child_keys = {row["store_id"] for row in body["datasets"]["example_order"]}
    assert child_keys <= parent_keys


def test_unpartitioned_parent_child_pipeline_materializes(
    defs: dg.Definitions, resources: dict[str, Any]
) -> None:
    result = _materialize(defs, resources, UNPARTITIONED_KEYS)
    assert result.success
    materialized = {
        event.asset_key.to_user_string()
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }
    assert set(UNPARTITIONED_KEYS) <= materialized


def test_partitioned_pipeline_materializes(defs: dg.Definitions, resources: dict[str, Any]) -> None:
    result = _materialize(defs, resources, ENTITY_KEYS, partition_key=ENTITY_PARTITION)
    assert result.success


def test_gold_tables_satisfy_their_contracts(
    defs: dg.Definitions, resources: dict[str, Any], spark: Any
) -> None:
    lakehouse = resources["lakehouse"]
    gold_tables = {
        "example_entity_gold": data_contracts.get_example_gold_contract_path(),
        "example_store_gold": data_contracts.get_example_store_gold_contract_path(),
        "example_order_gold": data_contracts.get_example_order_gold_contract_path(),
    }
    for table, contract_path in gold_tables.items():
        contract = load_contract_file(contract_path)
        frame = spark.read.format("delta").load(lakehouse.table_path("gold", table))
        outcomes = run_frame_validation(
            frame, contract, context=ValidationContext(contract=contract)
        )
        failures = _error_failures(outcomes)
        assert not failures, [outcome.as_failure() for outcome in failures]
        assert frame.limit(1).count() == 1, f"{table} is empty"


def test_order_foreign_key_integrity(spark: Any, resources: dict[str, Any]) -> None:
    lakehouse = resources["lakehouse"]
    order_contract = load_contract_file(data_contracts.get_example_order_silver_contract_path())
    parents = {
        "store_id": ForeignKeyParent(table="example_store", columns=["store_id"], layer="silver")
    }
    resolver = build_foreign_key_resolver(parents, spark=spark, lakehouse=lakehouse)
    frame = spark.read.format("delta").load(lakehouse.table_path("silver", "example_order"))
    outcomes = run_frame_validation(
        frame,
        order_contract,
        context=ValidationContext(contract=order_contract, foreign_key_resolver=resolver),
    )
    failures = _error_failures(outcomes)
    assert not failures, [outcome.as_failure() for outcome in failures]

    orphan_count = (
        frame.select("store_id")
        .distinct()
        .join(
            spark.read.format("delta")
            .load(lakehouse.table_path("silver", "example_store"))
            .select("store_id")
            .distinct(),
            on="store_id",
            how="left_anti",
        )
        .count()
    )
    assert orphan_count == 0


def test_merge_reruns_are_idempotent(
    defs: dg.Definitions, resources: dict[str, Any], spark: Any
) -> None:
    lakehouse = resources["lakehouse"]
    path = lakehouse.table_path("gold", "example_entity_gold")

    before = spark.read.format("delta").load(path)
    before_count = before.count()

    result = _materialize(defs, resources, ENTITY_KEYS, partition_key=ENTITY_PARTITION)
    assert result.success

    after = spark.read.format("delta").load(path)
    after_count = after.count()
    assert after_count == before_count, "merge rerun created duplicate rows"

    duplicates = after.groupBy("entity_id").count().where("count > 1").count()
    assert duplicates == 0


def test_asset_checks_execute_in_a_real_job(
    defs: dg.Definitions,
    resources: dict[str, Any],
    assets_by_key: dict[str, dg.AssetsDefinition],
) -> None:
    subset_keys = set(UNPARTITIONED_KEYS)
    subset_assets = [assets_by_key[key] for key in UNPARTITIONED_KEYS]
    subset_checks = []
    for checks in defs.asset_checks or []:
        specs = list(getattr(checks, "check_specs", []) or [])
        if specs and specs[0].asset_key.to_user_string() in subset_keys:
            subset_checks.append(checks)

    subset_defs = dg.Definitions(
        assets=subset_assets,
        asset_checks=subset_checks,
        resources=resources,
        jobs=[dg.define_asset_job("subset_checks_job", selection=dg.AssetSelection.all())],
    )
    result = subset_defs.get_job_def("subset_checks_job").execute_in_process(raise_on_error=True)
    assert result.success

    evaluations = result.get_asset_check_evaluations()
    assert evaluations, "expected asset check evaluations"
    failed = [evaluation for evaluation in evaluations if not evaluation.passed]
    assert not failed, list(failed)
