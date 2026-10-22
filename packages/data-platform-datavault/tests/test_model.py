"""Data Vault model resolution and validation."""

from __future__ import annotations

import dagster as dg
import pytest
from pydantic import ValidationError

from data_platform_datavault import (
    DataVaultModelSpec,
    HubSpec,
    LinkRelationship,
    LinkSourceSpec,
    LinkSpec,
    SatelliteSpec,
    VaultSourceSpec,
    build_model,
)
from data_platform_datavault._common import (
    ContractError,
    ContractRegistry,
    parse_contract_document,
)
from data_platform_datavault.assets import build_vault_assets


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


def multi_source_link_spec() -> DataVaultModelSpec:
    return base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(
                name="order_archive_src", contract="order", pipeline_name="p_order_archive"
            ),
        ],
        hubs=[
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
        ],
        links=[
            LinkSpec(
                name="order_store",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"]),
                    LinkRelationship(hub="store", columns=["store_id"]),
                ],
                sources=[
                    LinkSourceSpec(
                        name="order_src",
                        columns={"order_id": "order_id", "store_id": "store_id"},
                    ),
                    LinkSourceSpec(
                        name="order_archive_src",
                        columns={"order_id": "order_id", "store_id": "store_id"},
                    ),
                ],
            )
        ],
    )


def test_explicit_model_resolution_defaults() -> None:
    spec = base_spec(
        satellites=[SatelliteSpec(name="profile", source="store_src", parent="store")],
    )
    model = build_model(spec, registry())
    assert model.satellites["profile"].table == "sat_profile"
    assert model.satellites["profile"].attributes == ("store_name", "region", "created_at")
    assert model.satellites["profile"].parent_hash_key == "hub_store_hk"
    assert model.satellites["profile"].parent_asset_key == model.hubs["store"].asset_key


def test_hash_prefix_is_carried_into_hash_config() -> None:
    spec = base_spec(
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
    assert model.hubs["store"].hash.prefix == "HUB_STORE"


def test_unknown_source_rejected() -> None:
    spec = base_spec(hubs=[HubSpec(name="x", source="nope", business_keys=["store_id"])])
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_unknown_hub_in_link_rejected() -> None:
    spec = base_spec(
        links=[
            LinkSpec(
                name="bad",
                relationships=[LinkRelationship(hub="ghost", columns=["order_id"])],
                sources=[LinkSourceSpec(name="order_src", columns={"order_id": "order_id"})],
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
                relationships=[LinkRelationship(hub="store", columns=["store_id"])],
                sources=[LinkSourceSpec(name="order_src", columns={"store_id": "store_id"})],
            )
        ],
    )
    with pytest.raises(ContractError):
        build_model(spec, registry())


def test_link_sources_resolve_column_mappings() -> None:
    link = build_model(multi_source_link_spec(), registry()).links["order_store"]

    assert [link_source.source.name for link_source in link.sources] == [
        "order_src",
        "order_archive_src",
    ]
    assert link.sources[0].columns == (("order_id", "order_id"), ("store_id", "store_id"))
    assert link.sources[0].physical_columns == ["order_id", "store_id"]
    assert link.hash.columns == ("order_id", "store_id")
    assert link.relationships[1].hub_hash.columns == ("store_id",)
    assert [key.to_user_string() for key in link.source_asset_keys] == [
        "p_order/silver/order",
        "p_order_archive/silver/order",
    ]


def test_link_asset_dependencies_and_mappings() -> None:
    model = build_model(multi_source_link_spec(), registry())
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
        "p_order_archive/silver/order",
        "v/hub/hub_order",
        "v/hub/hub_store",
    }
    metadata = asset.metadata_by_key[key]
    assert metadata["datavault/sources"] == ["order_src", "order_archive_src"]
    assert metadata["datavault/source_columns"] == {
        "order_src": {"order_id": "order_id", "store_id": "store_id"},
        "order_archive_src": {"order_id": "order_id", "store_id": "store_id"},
    }


def test_link_source_must_exist() -> None:
    spec = base_spec(
        links=[
            LinkSpec(
                name="bad",
                relationships=[LinkRelationship(hub="store", columns=["store_id"])],
                sources=[LinkSourceSpec(name="nope", columns={"store_id": "store_id"})],
            )
        ],
    )
    with pytest.raises(ContractError, match="unknown source 'nope'"):
        build_model(spec, registry())


def test_mapped_column_must_exist_in_source() -> None:
    spec = base_spec(
        sources=[
            *sources(),
            VaultSourceSpec(name="customer_src", contract="customer", pipeline_name="p_customer"),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[LinkRelationship(hub="store", columns=["store_id"])],
                sources=[LinkSourceSpec(name="customer_src", columns={"store_id": "store_id"})],
            )
        ],
    )
    with pytest.raises(ContractError, match="has no column 'store_id'"):
        build_model(spec, registry())


def test_link_source_must_map_every_key_column() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"]),
                    LinkRelationship(hub="store", columns=["store_id"]),
                ],
                sources=[LinkSourceSpec(name="order_src", columns={"order_id": "order_id"})],
            )
        ],
    )
    with pytest.raises(ContractError, match=r"missing mappings for \['store_id'\]"):
        build_model(spec, registry())


def test_link_source_must_not_map_unknown_columns() -> None:
    spec = base_spec(
        hubs=[HubSpec(name="order", source="order_src", business_keys=["order_id"])],
        links=[
            LinkSpec(
                name="bad",
                relationships=[LinkRelationship(hub="order", columns=["order_id"])],
                sources=[
                    LinkSourceSpec(
                        name="order_src",
                        columns={"order_id": "order_id", "bogus": "quantity"},
                    )
                ],
            )
        ],
    )
    with pytest.raises(ContractError, match=r"maps unknown link columns \['bogus'\]"):
        build_model(spec, registry())


def test_link_source_must_not_map_two_keys_to_one_column() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"]),
                    LinkRelationship(hub="store", columns=["store_id"]),
                ],
                sources=[
                    LinkSourceSpec(
                        name="order_src",
                        columns={"order_id": "order_id", "store_id": "order_id"},
                    )
                ],
            )
        ],
    )
    with pytest.raises(ContractError, match="maps multiple keys to the same column"):
        build_model(spec, registry())


def test_link_duplicate_key_columns_rejected() -> None:
    spec = base_spec(
        hubs=[
            HubSpec(name="order", source="order_src", business_keys=["order_id"]),
            HubSpec(name="store", source="store_src", business_keys=["store_id"]),
        ],
        links=[
            LinkSpec(
                name="bad",
                relationships=[
                    LinkRelationship(hub="order", columns=["order_id"]),
                    LinkRelationship(hub="store", columns=["order_id"]),
                ],
                sources=[LinkSourceSpec(name="order_src", columns={"order_id": "order_id"})],
            )
        ],
    )
    with pytest.raises(ContractError, match="duplicate link columns"):
        build_model(spec, registry())


def test_link_requires_sources() -> None:
    with pytest.raises(ValidationError):
        LinkSpec.model_validate(
            {
                "name": "bad",
                "relationships": [{"hub": "order", "columns": ["order_id"]}],
            }
        )


def test_link_duplicate_source_names_rejected() -> None:
    with pytest.raises(ValidationError):
        LinkSpec(
            name="bad",
            relationships=[LinkRelationship(hub="order", columns=["order_id"])],
            sources=[
                LinkSourceSpec(name="order_src", columns={"order_id": "order_id"}),
                LinkSourceSpec(name="order_src", columns={"order_id": "order_id"}),
            ],
        )


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


def test_config_validation() -> None:
    with pytest.raises(ValidationError):
        DataVaultModelSpec(vault_name="v", sources=sources())
    with pytest.raises(ValidationError):
        VaultSourceSpec(name="has space", contract="store")
