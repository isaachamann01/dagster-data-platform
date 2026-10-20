"""Data Vault model resolution, validation, and auto-generation."""

from __future__ import annotations

import dagster as dg
import pytest
from pydantic import ValidationError

from data_platform_common.contracts import ContractRegistry, parse_contract_document
from data_platform_common.datavault import (
    DataVaultModelSpec,
    HubSpec,
    LinkRelationship,
    LinkSpec,
    SatelliteSpec,
    VaultSourceSpec,
    build_model,
)
from data_platform_common.datavault.assets import build_vault_assets
from data_platform_common.errors import ContractError


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


def registry() -> ContractRegistry:
    return ContractRegistry(
        [
            parse_contract_document(store_contract()),
            parse_contract_document(order_contract()),
            parse_contract_document(customer_contract()),
        ]
    )


def sources() -> list[VaultSourceSpec]:
    return [
        VaultSourceSpec(name="store_src", contract="store", pipeline_name="p_store"),
        VaultSourceSpec(name="order_src", contract="order", pipeline_name="p_order"),
    ]


def base_spec(**overrides: object) -> DataVaultModelSpec:
    payload: dict = {
        "vault_name": "v",
        "sources": sources(),
        "hubs": [HubSpec(name="store", source="store_src", business_keys=["store_id"])],
    }
    payload.update(overrides)
    return DataVaultModelSpec(**payload)


def test_auto_generation_from_contracts() -> None:
    spec = DataVaultModelSpec(vault_name="v", auto_generate=True, sources=sources())
    model = build_model(spec, registry())

    assert set(model.hubs) == {"store", "order"}
    assert set(model.links) == {"order_store"}
    assert set(model.satellites) == {"store", "order"}
    assert model.layer == "vault/v"

    link = model.links["order_store"]
    assert [relationship.hub_name for relationship in link.relationships] == ["order", "store"]
    assert link.table == "lnk_order_store"
    assert link.hash_key == "lnk_order_store_hk"

    assert model.hubs["store"].table == "hub_store"
    assert model.hubs["store"].hash_key == "hub_store_hk"
    assert model.hubs["store"].source.asset_key == model.hubs["store"].source.asset_key
    assert model.hubs["store"].source.asset_key.to_user_string() == "p_store/silver/store"

    order_satellite = model.satellites["order"]
    assert order_satellite.attributes == ("quantity",)
    assert order_satellite.parent_name == "order"
    store_satellite = model.satellites["store"]
    assert store_satellite.attributes == ("store_name", "region", "created_at")
    assert store_satellite.load_ts_column is None


def test_explicit_model_resolution_defaults() -> None:
    spec = base_spec(
        satellites=[SatelliteSpec(name="profile", source="store_src", parent="store")],
    )
    model = build_model(spec, registry())
    assert model.satellites["profile"].table == "sat_profile"
    assert model.satellites["profile"].attributes == ("store_name", "region", "created_at")
    assert model.satellites["profile"].parent_hash_key == "hub_store_hk"
    assert model.satellites["profile"].parent_asset_key == model.hubs["store"].asset_key


def test_hash_prefix_and_algorithm_are_carried_into_hash_config() -> None:
    spec = base_spec(
        hash_algorithm="sha256",
        hubs=[
            HubSpec(
                name="store",
                source="store_src",
                business_keys=["store_id"],
                hash_prefix="HUB_STORE",
            )
        ],
    )
    model = build_model(spec, registry())
    hub = model.hubs["store"]
    assert hub.hash.algorithm == "sha256"
    assert hub.hash.prefix == "HUB_STORE"


def test_unknown_source_rejected() -> None:
    spec = base_spec(hubs=[HubSpec(name="x", source="nope", business_keys=["store_id"])])
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_unknown_hub_in_link_rejected() -> None:
    spec = base_spec(
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="ghost", columns=["order_id"], source="order_src")
                ],
            )
        ]
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_link_relationship_arity_must_match_hub_keys() -> None:
    spec = base_spec(
        hubs=[HubSpec(name="store", source="store_src", business_keys=["store_id", "region"])],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="store", columns=["store_id"], source="order_src"),
                ],
            )
        ],
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_cross_contract_link_keys_resolve_declared_sources() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
        ],
        links=[
            LinkSpec(
                name="order_store",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"], source="order_src"),
                    LinkRelationship(hub="store", columns=["store_id"], source="store_src"),
                ],
            )
        ],
    )
    model = build_model(spec, registry())
    link = model.links["order_store"]

    # No base source in the spec: the first source containing both keys wins.
    assert link.source.name == "order_src"
    assert [source.name for source in link.sources] == ["order_src", "store_src"]
    assert [relationship.source.name for relationship in link.relationships] == [
        "order_src",
        "store_src",
    ]
    assert link.relationships[1].hub_hash.columns == ("store_id",)
    assert link.relationships[1].hub_hash.prefix == model.hubs["store"].hash.prefix
    assert [key.to_user_string() for key in link.source_asset_keys] == [
        "p_order/silver/order",
        "p_store/silver/store",
    ]


def test_cross_contract_link_asset_dependencies() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
        ],
        links=[
            LinkSpec(
                name="order_store",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"], source="order_src"),
                    LinkRelationship(hub="store", columns=["store_id"], source="store_src"),
                ],
            )
        ],
    )
    model = build_model(spec, registry())
    assets = build_vault_assets(
        model,
        {
            "spark": "spark",
            "lakehouse": "lakehouse",
            "metadata": "metadata",
            "io_manager": "delta_io_manager",
        },
    )
    key = dg.AssetKey(["v", "link", "lnk_order_store"])
    asset = next(item for item in assets if key in item.keys)

    assert {dependency.to_user_string() for dependency in asset.asset_deps[key]} == {
        "p_order/silver/order",
        "p_store/silver/store",
        "v/hub/hub_order",
        "v/hub/hub_store",
    }
    assert asset.metadata_by_key[key]["datavault/sources"] == ["order_src", "store_src"]
    assert asset.metadata_by_key[key]["datavault/source"] == "order_src"


def test_link_relationship_source_must_exist() -> None:
    spec = base_spec(
        links=[
            LinkSpec(
                name="bad",
                relationships=[LinkRelationship(hub="store", columns=["store_id"], source="nope")],
            )
        ],
    )
    with pytest.raises(ContractError, match="unknown source 'nope'"):
        build_model(spec, registry())


def test_relationship_column_must_exist_in_declared_source() -> None:
    spec = base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(name="customer_src", contract="customer", pipeline_name="p_customer"),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="store", columns=["store_id"], source="customer_src")
                ],
            )
        ],
    )
    with pytest.raises(ContractError, match="not a column of source 'customer_src'"):
        build_model(spec, registry())


def test_link_requires_a_source_with_all_key_columns() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="store", source="store_src", business_keys=["store_name"]),
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"], source="order_src"),
                    LinkRelationship(hub="store", columns=["store_name"], source="store_src"),
                ],
            )
        ],
    )
    with pytest.raises(ContractError, match="no declared source contains all key columns"):
        build_model(spec, registry())


def test_link_relationship_requires_source() -> None:
    with pytest.raises(ValidationError):
        LinkRelationship(hub="order", columns=["order_id"])


def test_unknown_satellite_parent_rejected() -> None:
    spec = base_spec(satellites=[SatelliteSpec(name="x", source="store_src", parent="ghost")])
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_satellite_attribute_must_exist() -> None:
    spec = base_spec(
        satellites=[
            SatelliteSpec(name="x", source="store_src", parent="store", attributes=["nope"])
        ]
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_satellite_attribute_cannot_be_parent_key() -> None:
    spec = base_spec(
        satellites=[
            SatelliteSpec(name="x", source="store_src", parent="store", attributes=["store_id"])
        ]
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_satellite_load_ts_column_must_exist() -> None:
    spec = base_spec(
        satellites=[
            SatelliteSpec(
                name="x",
                source="store_src",
                parent="store",
                attributes=["store_name"],
                load_ts_column="missing",
            )
        ]
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_duplicate_hub_names_rejected() -> None:
    with pytest.raises(ValidationError):
        base_spec(
            hubs=[
                HubSpec(name="store", source="store_src", business_keys=["store_id"]),
                HubSpec(name="store", source="store_src", business_keys=["region"]),
            ]
        )


def test_auto_generate_requires_parent_contracts() -> None:
    spec = DataVaultModelSpec(
        vault_name="v",
        auto_generate=True,
        sources=[VaultSourceSpec(name="order_src", contract="order")],
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_config_validation() -> None:
    with pytest.raises(ValidationError):
        DataVaultModelSpec(vault_name="v", sources=[], auto_generate=True)
    with pytest.raises(ValidationError):
        DataVaultModelSpec(
            vault_name="v",
            sources=sources(),
            auto_generate=True,
            hubs=[HubSpec(name="store", source="store_src", business_keys=["store_id"])],
        )
    with pytest.raises(ValidationError):
        DataVaultModelSpec(vault_name="v", sources=sources())
    with pytest.raises(ValidationError):
        VaultSourceSpec(name="has space", contract="store")
