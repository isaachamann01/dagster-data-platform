"""Data Vault definitions-level integration tests (no Spark required)."""

from __future__ import annotations

import dagster as dg

VAULT_KEYS = {
    "retail/hub/hub_store",
    "retail/hub/hub_order",
    "retail/link/lnk_store_order",
    "retail/sat/sat_store_profile",
    "retail/sat/sat_order_details",
}


def _deps(definition: dg.Definitions, key_string: str) -> set[str]:
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            if key.to_user_string() == key_string:
                return {parent.to_user_string() for parent in assets.asset_deps.get(key, set())}
    raise AssertionError(f"asset {key_string} not found")


def _asset_metadata(definition: dg.Definitions, key_string: str) -> dict:
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            if key.to_user_string() == key_string:
                return dict(assets.metadata_by_key.get(key, {}))
    raise AssertionError(f"asset {key_string} not found")


def test_vault_assets_are_generated(example_defs: dg.Definitions) -> None:
    keys = {
        key.to_user_string()
        for assets in example_defs.assets or []
        if isinstance(assets, dg.AssetsDefinition)
        for key in assets.keys
    }
    assert keys >= VAULT_KEYS


def test_vault_lineage_points_at_silver_and_parents(example_defs: dg.Definitions) -> None:
    assert "example_store/silver/example_store" in _deps(example_defs, "retail/hub/hub_store")
    link_deps = _deps(example_defs, "retail/link/lnk_store_order")
    assert {
        "example_order/silver/example_order",
        "retail/hub/hub_order",
        "retail/hub/hub_store",
    } <= link_deps
    satellite_deps = _deps(example_defs, "retail/sat/sat_store_profile")
    assert {"example_store/silver/example_store", "retail/hub/hub_store"} <= satellite_deps


def test_explicit_satellite_metadata(example_defs: dg.Definitions) -> None:
    metadata = _asset_metadata(example_defs, "retail/sat/sat_store_profile")
    assert metadata["datavault/attributes"] == ["store_name", "region", "event_date"]
    assert metadata["datavault/parent"] == "hub:store"


def test_link_metadata_lists_participating_hubs(example_defs: dg.Definitions) -> None:
    metadata = _asset_metadata(example_defs, "retail/link/lnk_store_order")
    assert metadata["datavault/hubs"] == ["order", "store"]
    assert metadata["datavault/sources"] == ["example_order"]
    assert metadata["datavault/source_columns"] == {
        "example_order": {"order_id": "order_id", "store_id": "store_id"}
    }
    assert metadata["datavault/hash_key"] == "lnk_store_order_hk"
