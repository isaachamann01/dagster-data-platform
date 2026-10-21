"""Prove the MedallionFactory component is reusable from a separate project."""

from __future__ import annotations

from pathlib import Path

import dagster as dg


def _asset_keys(definition: dg.Definitions) -> set[str]:
    keys: set[str] = set()
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            keys.add(key.to_user_string())
    return keys


def test_definitions_load_with_expected_assets() -> None:
    defs = dg.load_from_defs_folder(path_within_project=Path(__file__))

    keys = _asset_keys(defs)
    assert keys == {
        "tiny_entity/bronze/tiny_entity",
        "tiny_entity/silver/tiny_entity",
        "tiny_entity/gold/tiny_entity_gold",
    }


def test_asset_checks_are_contract_derived() -> None:
    defs = dg.load_from_defs_folder(path_within_project=Path(__file__))

    check_names = set()
    for checks in defs.asset_checks or []:
        for spec in checks.check_specs:
            if isinstance(spec, dg.AssetCheckSpec):
                check_names.add(spec.name)

    assert "primary_key_unique" in check_names
    assert "not_empty" in check_names
    assert "foreign_key__" not in " ".join(check_names)
    assert any(name.startswith("allowed_values__") for name in check_names)


def test_full_snapshot_pipeline_is_unpartitioned() -> None:
    defs = dg.load_from_defs_folder(path_within_project=Path(__file__))
    for assets in defs.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            if key.path[-2:] == ["silver", "tiny_entity"]:
                assert assets.partitions_def is None
