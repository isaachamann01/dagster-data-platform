"""Configuration models for the DataVaultFactory component."""

from __future__ import annotations

import re
from typing import Literal

from dagster import Resolvable
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BaseSpec(Resolvable, BaseModel):
    """Base for vault attribute models with strict pydantic validation."""

    model_config = ConfigDict(extra="forbid")


NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _check_name(value: str, kind: str) -> str:
    if not NAME_PATTERN.match(value):
        raise ValueError(f"{kind} name '{value}' must match {NAME_PATTERN.pattern}")
    return value


class VaultSourceSpec(BaseSpec):
    """One silver source feeding the vault (a contract plus where it lives)."""

    name: str
    contract: str
    pipeline_name: str | None = None
    table: str | None = None
    layer: str = "silver"
    asset_key: list[str] = Field(default_factory=list)
    record_source: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "source")


class HubSpec(BaseSpec):
    """A hub: one row per distinct business key."""

    name: str
    source: str
    business_keys: list[str] = Field(min_length=1)
    table: str | None = None
    hash_key: str | None = None
    hash_prefix: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "hub")


class LinkRelationship(BaseSpec):
    """A participating hub in a link, its source contract, and key columns."""

    hub: str
    columns: list[str] = Field(min_length=1)
    source: str


class LinkSpec(BaseSpec):
    """A link: one row per distinct combination of participating hub keys.

    The link has no base source of its own. Each relationship names the contract
    that owns its key; the component loads from the first declared source that
    contains every key column.
    """

    name: str
    relationships: list[LinkRelationship] = Field(min_length=1)
    table: str | None = None
    hash_key: str | None = None
    hash_prefix: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "link")


class SatelliteSpec(BaseSpec):
    """A satellite: descriptive attributes attached to a hub or link."""

    name: str
    source: str
    parent: str
    attributes: list[str] = Field(default_factory=list)
    table: str | None = None
    hashdiff_column: str = "hashdiff"
    load_ts_column: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "satellite")


class DataVaultModelSpec(BaseSpec):
    """Optional VaultModel schema for the component attributes."""

    vault_name: str
    sources: list[VaultSourceSpec] = Field(min_length=1)
    hubs: list[HubSpec] = Field(default_factory=list)
    links: list[LinkSpec] = Field(default_factory=list)
    satellites: list[SatelliteSpec] = Field(default_factory=list)
    hash_delimiter: str = "||"
    normalize_key_values: bool = True
    layer: str = "vault"
    load_timestamp_column_name: str = "load_ts"
    record_source_column_name: str = "record_source"
    write_mode: Literal["merge", "append", "overwrite"] = "merge"
    contract_paths: list[str] = Field(default_factory=list)
    trusted_import_prefixes: list[str] = Field(default_factory=list)
    resource_keys: dict[str, str] = Field(
        default_factory=lambda: {
            "spark": "spark",
            "lakehouse": "lakehouse",
            "metadata": "metadata",
            "io_manager": "delta_io_manager",
        }
    )

    @field_validator("vault_name")
    @classmethod
    def _validate_vault_name(cls, value: str) -> str:
        return _check_name(value, "vault")

    @model_validator(mode="after")
    def _validate_duplicates(self) -> DataVaultModelSpec:
        duplicates = [
            kind
            for kind, names in {
                "source": [source.name for source in self.sources],
                "hub": [hub.name for hub in self.hubs],
                "link": [link.name for link in self.links],
                "satellite": [satellite.name for satellite in self.satellites],
            }.items()
            if len(set(names)) != len(names)
        ]
        if duplicates:
            raise ValueError(f"duplicate definition names in: {', '.join(duplicates)}")

        if not (self.hubs or self.links or self.satellites):
            raise ValueError("the vault has no hubs, links, or satellites; declare them")
        return self

    def resource_key(self, name: str, default: str) -> str:
        return self.resource_keys.get(name, default)


def validate_attributes_against_contract(
    attributes: list[str], available: set[str], *, satellite: str
) -> None:
    missing = [attribute for attribute in attributes if attribute not in available]
    if missing:
        raise ValueError(f"satellite '{satellite}' attributes not in source: {missing}")


__all__ = [
    "DataVaultModelSpec",
    "HubSpec",
    "LinkRelationship",
    "LinkSpec",
    "SatelliteSpec",
    "VaultSourceSpec",
    "validate_attributes_against_contract",
]
