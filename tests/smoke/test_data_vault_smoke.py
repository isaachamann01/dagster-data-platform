"""Data Vault 2.0 smoke tests.

Builds hubs, links, and satellites from the silver layer through a real Spark
session and Delta tables, then verifies hash determinism, referential
integrity, attribute fidelity, and the generated vault checks.
"""

from __future__ import annotations

from typing import Any

import dagster as dg
import pytest
from pyspark.sql import functions as F

pytestmark = [pytest.mark.slow, pytest.mark.smoke]

STORE_ORDER_SILVER = [
    "example_store/bronze/example_store",
    "example_store/silver/example_store",
    "example_order/bronze/example_order",
    "example_order/silver/example_order",
]
RETAIL_KEYS = [
    "retail/hub/hub_store",
    "retail/hub/hub_order",
    "retail/link/lnk_store_order",
    "retail/sat/sat_store_profile",
    "retail/sat/sat_order_details",
]


def _materialize(
    defs: dg.Definitions,
    resources: dict[str, Any],
    keys: list[str],
    *,
    partition_key: str | None = None,
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [asset for asset in defs.assets or [] if isinstance(asset, dg.AssetsDefinition)],
        resources=resources,
        selection=[dg.AssetKey(key.split("/")) for key in keys],
        partition_key=partition_key,
        raise_on_error=True,
    )


def _table(spark: Any, resources: dict[str, Any], layer: str, table: str) -> Any:
    path = resources["lakehouse"].table_path(layer, table)
    return spark.read.format("delta").load(path)


def test_retail_hubs_match_silver_and_hashes_are_stable(
    defs: dg.Definitions, resources: dict[str, Any], spark: Any
) -> None:
    result = _materialize(defs, resources, STORE_ORDER_SILVER + RETAIL_KEYS)
    assert result.success

    hub_store = _table(spark, resources, "vault/retail", "hub_store")
    silver_store = _table(spark, resources, "silver", "example_store")
    assert hub_store.count() == silver_store.select("store_id").distinct().count()
    assert hub_store.where(F.col("hub_store_hk").isNull()).count() == 0
    assert hub_store.select("hub_store_hk").distinct().count() == hub_store.count()

    before = [
        (row["store_id"], row["hub_store_hk"]) for row in hub_store.orderBy("store_id").collect()
    ]

    # Rerun: deterministic hashes and insert-only merge mean identical tables.
    result2 = _materialize(defs, resources, STORE_ORDER_SILVER + RETAIL_KEYS)
    assert result2.success
    after = [
        (row["store_id"], row["hub_store_hk"]) for row in hub_store.orderBy("store_id").collect()
    ]
    assert before == after
    assert len(before) == len(set(before))


def test_link_and_satellites_are_referentially_consistent(
    defs: dg.Definitions, resources: dict[str, Any], spark: Any
) -> None:
    _materialize(defs, resources, STORE_ORDER_SILVER + RETAIL_KEYS)

    hub_store = _table(spark, resources, "vault/retail", "hub_store")
    hub_order = _table(spark, resources, "vault/retail", "hub_order")
    link = _table(spark, resources, "vault/retail", "lnk_store_order")
    satellite = _table(spark, resources, "vault/retail", "sat_store_profile")
    silver_store = _table(spark, resources, "silver", "example_store")

    assert link.select("lnk_store_order_hk").distinct().count() == link.count()
    assert (
        link.select("hub_store_hk")
        .distinct()
        .join(hub_store.select("hub_store_hk"), on="hub_store_hk", how="left_anti")
        .count()
        == 0
    )
    assert (
        link.select("hub_order_hk")
        .distinct()
        .join(hub_order.select("hub_order_hk"), on="hub_order_hk", how="left_anti")
        .count()
        == 0
    )
    assert (
        satellite.select("hub_store_hk")
        .distinct()
        .join(hub_store.select("hub_store_hk"), on="hub_store_hk", how="left_anti")
        .count()
        == 0
    )
    assert (
        satellite.groupBy("hub_store_hk", "hashdiff").count().where(F.col("count") > 1).count() == 0
    )

    # Satellite attributes match the silver source for every store.
    mined = (
        satellite.select("hub_store_hk", F.col("store_name").alias("sat_name"))
        .join(hub_store.select("hub_store_hk", "store_id"), on="hub_store_hk")
        .join(
            silver_store.select("store_id", F.col("store_name").alias("source_name")),
            on="store_id",
        )
    )
    assert mined.where(F.col("sat_name") != F.col("source_name")).count() == 0
    assert mined.count() == satellite.count()


def test_vault_checks_execute_in_a_real_job(
    defs: dg.Definitions,
    resources: dict[str, Any],
    assets_by_key: dict[str, dg.AssetsDefinition],
) -> None:
    subset_keys = set(STORE_ORDER_SILVER + RETAIL_KEYS)
    subset_assets = [assets_by_key[key] for key in subset_keys]
    subset_checks = []
    for checks in defs.asset_checks or []:
        specs = list(getattr(checks, "check_specs", []) or [])
        if specs and specs[0].asset_key.to_user_string() in subset_keys:
            subset_checks.append(checks)

    vault_defs = dg.Definitions(
        assets=subset_assets,
        asset_checks=subset_checks,
        resources=resources,
        jobs=[dg.define_asset_job("vault_checks_job", selection=dg.AssetSelection.all())],
    )
    result = vault_defs.get_job_def("vault_checks_job").execute_in_process(raise_on_error=True)
    assert result.success

    evaluations = result.get_asset_check_evaluations()
    assert evaluations
    failed = [evaluation for evaluation in evaluations if not evaluation.passed]
    assert not failed, list(failed)
