"""Data Vault model resolution and validation."""

from __future__ import annotations

import dagster as dg

from data_platform_datavault import (
    DataVaultModelSpec,
    HubSpec,
    LinkRelationship,
    LinkSpec,
    SatelliteSpec,
    SourceMappingSpec,
    VaultSourceSpec,
    build_model,
)
from data_platform_datavault.components.data_vault_factory import build_vault_assets
from data_platform_datavault.model import ContractRegistry, parse_contract_document


def store_contract() -> dict:
    return {
        "name": "store",
        "columns": [
            {"name": "store_id", "type": "string", "primary_key": True},
            {"name": "store_name", "type": "string"},
            {"name": "region", "type": "string"},
            {"name": "created_at", "type": "timestamp", "nullable": True},
        ],
    }


def order_contract() -> dict:
    return {
        "name": "order",
        "columns": [
            {"name": "order_id", "type": "string", "primary_key": True},
            {
                "name": "store_id",
                "type": "string",
                "references": {"contract": "store", "columns": ["store_id"]},
            },
            {"name": "quantity", "type": "integer"},
        ],
    }


def customer_contract() -> dict:
    return {
        "name": "customer",
        "columns": [
            {"name": "customer_id", "type": "string", "primary_key": True},
        ],
    }


def legacy_store_contract() -> dict:
    return {
        "name": "legacy_store",
        "columns": [
            {"name": "store_code", "type": "string", "primary_key": True},
            {"name": "store_name", "type": "string"},
            {"name": "region", "type": "string"},
        ],
    }


def registry() -> ContractRegistry:
    return ContractRegistry(
        [
            parse_contract_document(store_contract()),
            parse_contract_document(order_contract()),
            parse_contract_document(customer_contract()),
            parse_contract_document(legacy_store_contract()),
        ]
    )


def sources() -> list[VaultSourceSpec]:
    return [
        VaultSourceSpec(name="store_src", contract="store", pipeline_name="p_store"),
        VaultSourceSpec(name="order_src", contract="order", pipeline_name="p_order"),
    ]


def hub(name: str, source: str, keys: list[str], *, hash_prefix: str | None = None) -> HubSpec:
    return HubSpec(
        name=name,
        sources=[SourceMappingSpec(name=source, columns={key: key for key in keys})],
        business_keys=keys,
        hash_prefix=hash_prefix,
    )


def base_spec(**overrides: object) -> DataVaultModelSpec:
    payload: dict = {
        "vault_name": "v",
        "sources": sources(),
        "hubs": [hub("store", "store_src", ["store_id"])],
    }
    payload.update(overrides)
    return DataVaultModelSpec(**payload)


def multi_source_link_spec() -> DataVaultModelSpec:
    return base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(
                name="order_archive_src", contract="order", pipeline_name="p_order_archive"
            ),
        ],
        hubs=[
            hub("store", "store_src", ["store_id"]),
            hub("order", "order_src", ["order_id"]),
        ],
        links=[
            LinkSpec(
                name="order_store",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"]),
                    LinkRelationship(hub="store", columns=["store_id"]),
                ],
                sources=[
                    SourceMappingSpec(
                        name="order_src",
                        columns={"order_id": "order_id", "store_id": "store_id"},
                    ),
                    SourceMappingSpec(
                        name="order_archive_src",
                        columns={"order_id": "order_id", "store_id": "store_id"},
                    ),
                ],
            )
        ],
    )


def multi_source_hub_spec() -> DataVaultModelSpec:
    return base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(
                name="legacy_store_src", contract="legacy_store", pipeline_name="p_legacy_store"
            ),
        ],
        hubs=[
            HubSpec(
                name="store",
                business_keys=["store_id"],
                sources=[
                    SourceMappingSpec(name="store_src", columns={"store_id": "store_id"}),
                    SourceMappingSpec(name="legacy_store_src", columns={"store_id": "store_code"}),
                ],
            )
        ],
    )


def test_explicit_model_resolution_defaults() -> None:
    spec = base_spec(
        satellites=[SatelliteSpec(name="profile", parent="store")],
    )
    model = build_model(spec, registry())
    assert model.satellites["profile"].table == "sat_profile"
    assert model.satellites["profile"].attributes == ("store_name", "region", "created_at")
    assert model.satellites["profile"].parent_hash_key == "hub_store_hk"
    assert model.satellites["profile"].parent_asset_key == model.hubs["store"].asset_key


def test_hash_prefix_is_carried_into_hash_config() -> None:
    spec = base_spec(
        hubs=[hub("store", "store_src", ["store_id"], hash_prefix="HUB_STORE")],
    )
    model = build_model(spec, registry())
    assert model.hubs["store"].hash.prefix == "HUB_STORE"


def test_hub_sources_resolve_column_mappings() -> None:
    resolved_hub = build_model(multi_source_hub_spec(), registry()).hubs["store"]

    assert [mapping.source.name for mapping in resolved_hub.sources] == [
        "store_src",
        "legacy_store_src",
    ]
    assert resolved_hub.sources[0].columns == (("store_id", "store_id"),)
    assert resolved_hub.sources[1].columns == (("store_id", "store_code"),)
    assert [key.to_user_string() for key in resolved_hub.source_asset_keys] == [
        "p_store/silver/store",
        "p_legacy_store/silver/legacy_store",
    ]


def test_hub_asset_dependencies_and_mappings() -> None:
    model = build_model(multi_source_hub_spec(), registry())
    assets = build_vault_assets(
        model,
        {
            "spark": "spark",
            "lakehouse": "lakehouse",
            "metadata": "metadata",
            "io_manager": "delta_io_manager",
        },
    )
    key = dg.AssetKey(["v", "hub", "hub_store"])
    asset = next(item for item in assets if key in item.keys)

    assert {dependency.to_user_string() for dependency in asset.asset_deps[key]} == {
        "p_store/silver/store",
        "p_legacy_store/silver/legacy_store",
    }
    metadata = asset.metadata_by_key[key]
    assert metadata["datavault/sources"] == ["store_src", "legacy_store_src"]
    assert metadata["datavault/source_columns"] == {
        "store_src": {"store_id": "store_id"},
        "legacy_store_src": {"store_id": "store_code"},
    }
    assert "datavault/source" not in metadata


def test_satellite_inherits_parent_source_and_key_mapping() -> None:
    spec = base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(
                name="legacy_store_src", contract="legacy_store", pipeline_name="p_legacy_store"
            ),
        ],
        hubs=[
            HubSpec(
                name="store",
                business_keys=["store_id"],
                sources=[
                    SourceMappingSpec(name="legacy_store_src", columns={"store_id": "store_code"})
                ],
            )
        ],
        satellites=[SatelliteSpec(name="store_profile", parent="store", attributes=["store_name"])],
    )
    satellite = build_model(spec, registry()).satellites["store_profile"]

    assert satellite.source.name == "legacy_store_src"
    assert satellite.parent_source_columns == (("store_id", "store_code"),)
    assert satellite.attribute_source_columns == (("store_name", "store_name"),)


def test_satellite_default_attributes_exclude_renamed_parent_key() -> None:
    spec = base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(
                name="legacy_store_src", contract="legacy_store", pipeline_name="p_legacy_store"
            ),
        ],
        hubs=[
            HubSpec(
                name="store",
                business_keys=["store_id"],
                sources=[
                    SourceMappingSpec(name="legacy_store_src", columns={"store_id": "store_code"})
                ],
            )
        ],
        satellites=[SatelliteSpec(name="store_profile", parent="store")],
    )
    satellite = build_model(spec, registry()).satellites["store_profile"]

    assert satellite.attributes == ("store_name", "region")
    assert "store_code" not in satellite.attributes


def test_satellite_attributes_can_map_columns() -> None:
    spec = base_spec(
        satellites=[
            SatelliteSpec(
                name="store_profile",
                parent="store",
                columns={"display_name": "store_name", "region": "region"},
            )
        ]
    )
    satellite = build_model(spec, registry()).satellites["store_profile"]

    assert satellite.attributes == ("display_name", "region")
    assert satellite.attribute_source_columns == (
        ("display_name", "store_name"),
        ("region", "region"),
    )
