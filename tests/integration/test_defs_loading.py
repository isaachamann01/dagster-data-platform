"""Definitions-level integration tests (no Spark required)."""

from __future__ import annotations

import dagster as dg

EXPECTED_ASSETS = {
    # Medallion (3 pipelines x bronze/silver/gold)
    "example_entity/bronze/example_entity",
    "example_entity/silver/example_entity",
    "example_entity/gold/example_entity_gold",
    "example_store/bronze/example_store",
    "example_store/silver/example_store",
    "example_store/gold/example_store_gold",
    "example_order/bronze/example_order",
    "example_order/silver/example_order",
    "example_order/gold/example_order_gold",
    # Explicit Data Vault (retail)
    "retail/hub/hub_store",
    "retail/hub/hub_order",
    "retail/link/lnk_store_order",
    "retail/sat/sat_store_profile",
    "retail/sat/sat_order_details",
    # Auto-generated Data Vault (entity_vault)
    "entity_vault/hub/hub_example_entity",
    "entity_vault/sat/sat_example_entity",
}


def _keys(definition: dg.Definitions) -> set[str]:
    keys: set[str] = set()
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            keys.add(key.to_user_string())
    return keys


def _deps(definition: dg.Definitions, key_string: str) -> set[str]:
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            if key.to_user_string() == key_string:
                return {parent.to_user_string() for parent in assets.asset_deps.get(key, set())}
    raise AssertionError(f"asset {key_string} not found")


def _check_specs(definition: dg.Definitions) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for checks in definition.asset_checks or []:
        for spec in getattr(checks, "check_specs", []) or []:
            specs.append((spec.asset_key.to_user_string(), spec.name))
    return specs


def test_all_pipelines_and_multiple_documents_load(example_defs: dg.Definitions) -> None:
    # Sixteen assets: medallion (three pipelines, two from one multi-document
    # file) plus two Data Vault factories (one explicit, one auto-generated).
    assert _keys(example_defs) == EXPECTED_ASSETS


def test_bronze_silver_gold_dependency_graph(example_defs: dg.Definitions) -> None:
    assert "example_entity/bronze/example_entity" in _deps(
        example_defs, "example_entity/silver/example_entity"
    )
    assert "example_entity/silver/example_entity" in _deps(
        example_defs, "example_entity/gold/example_entity_gold"
    )
    # The child pipeline depends on the parent's silver asset for FK validation.
    order_silver_deps = _deps(example_defs, "example_order/silver/example_order")
    assert "example_order/bronze/example_order" in order_silver_deps
    assert "example_store/silver/example_store" in order_silver_deps


def test_partitioning_modes_are_applied(example_defs: dg.Definitions) -> None:
    by_key: dict[str, dg.AssetsDefinition] = {}
    for assets in example_defs.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            by_key[key.to_user_string()] = assets
    daily = by_key["example_entity/silver/example_entity"].partitions_def
    assert isinstance(daily, dg.DailyPartitionsDefinition)
    assert by_key["example_store/silver/example_store"].partitions_def is None
    assert by_key["example_order/gold/example_order_gold"].partitions_def is None


def test_contract_checks_are_generated_per_layer(example_defs: dg.Definitions) -> None:
    specs = _check_specs(example_defs)
    names = {name for _, name in specs}
    for required in (
        "missing_columns",
        "unexpected_columns",
        "column_types",
        "nullability",
        "primary_key_unique",
        "not_empty",
        "regex__entity_id",
        "allowed_values__category",
        "range__score",
        "expectation__population_present",
    ):
        assert required in names, required
    assert ("example_order/silver/example_order", "foreign_key__store_id") in specs
    # FK checks exist only where contracts declare them.
    assert all(
        name != "foreign_key__store_id" or key.startswith("example_order") for key, name in specs
    )


def test_asset_keys_are_namespaced_by_pipeline(example_defs: dg.Definitions) -> None:
    keys = _keys(example_defs)
    assert len(keys) == len(EXPECTED_ASSETS)
    assert all(len(key.split("/")) == 3 for key in keys)
