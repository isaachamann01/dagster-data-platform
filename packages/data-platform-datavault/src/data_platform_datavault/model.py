"""Data Vault 2.0 specs and resolved model.

The Pydantic specs at the top mirror the component's YAML attributes. The
resolved dataclasses below are built by :func:`build_model` and consumed by the
asset builders.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal

import dagster as dg
from pydantic import Field

from data_platform_common.contracts import parse_contract_document
from data_platform_common.contracts.models import DataContract
from data_platform_common.contracts.registry import ContractRegistry
from data_platform_common.imports import build_trusted_prefixes
from data_platform_common.io.delta import DeltaTableWriter, DeltaWriteOptions, WriteMode
from data_platform_common.spark_types import METADATA_COLUMN_PREFIX


class BaseSpec(dg.Resolvable, dg.Model):
    """Base for vault attribute models."""


class VaultSourceSpec(BaseSpec):
    """One silver source feeding the vault (a contract plus where it lives)."""

    name: str
    contract: str
    pipeline_name: str | None = None
    table: str | None = None
    layer: str = "silver"
    asset_key: list[str] = Field(default_factory=list)
    record_source: str | None = None


class SourceMappingSpec(BaseSpec):
    """One physical source and its canonical-to-physical column mapping."""

    name: str
    columns: dict[str, str] = Field(default_factory=dict)


class SourceMappedSpec(BaseSpec):
    """Base for specs that read canonical columns from one or more sources."""

    sources: list[SourceMappingSpec] = Field(default_factory=list)


class HubSpec(SourceMappedSpec):
    """A hub: one row per distinct business key."""

    name: str
    business_keys: list[str] = Field(default_factory=list)
    table: str | None = None
    hash_key: str | None = None
    hash_prefix: str | None = None


class LinkRelationship(BaseSpec):
    """A participating hub and the canonical key columns it contributes."""

    hub: str
    columns: list[str] = Field(default_factory=list)


class LinkSpec(SourceMappedSpec):
    """A link: one row per distinct combination of participating hub keys."""

    name: str
    relationships: list[LinkRelationship] = Field(default_factory=list)
    table: str | None = None
    hash_key: str | None = None
    hash_prefix: str | None = None


class SatelliteSpec(BaseSpec):
    """A satellite: descriptive attributes attached to a hub or link."""

    name: str
    parent: str
    attributes: list[str] = Field(default_factory=list)
    columns: dict[str, str] = Field(default_factory=dict)
    table: str | None = None
    hashdiff_column: str = "hashdiff"
    load_ts_column: str | None = None


class DataVaultModelSpec(BaseSpec):
    """Vault component attributes."""

    vault_name: str
    sources: list[VaultSourceSpec] = Field(default_factory=list)
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


HUB_KIND = "hub"
LINK_KIND = "link"
SAT_KIND = "sat"


@dataclass(frozen=True)
class HashConfig:
    """How one MD5 hash key / hashdiff is computed."""

    columns: tuple[str, ...]
    delimiter: str
    prefix: str | None
    normalize: bool


@dataclass(frozen=True)
class ResolvedSource:
    name: str
    contract: DataContract
    table_name: str
    layer: str
    asset_key: dg.AssetKey
    record_source: str


@dataclass(frozen=True)
class ResolvedSourceMapping:
    """A physical source and its canonical-to-physical column mapping."""

    source: ResolvedSource
    columns: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ResolvedHub:
    name: str
    table: str
    hash_key: str
    sources: tuple[ResolvedSourceMapping, ...]
    business_keys: tuple[str, ...]
    hash: HashConfig
    asset_key: dg.AssetKey

    @property
    def source_asset_keys(self) -> list[dg.AssetKey]:
        return [source_mapping.source.asset_key for source_mapping in self.sources]


@dataclass(frozen=True)
class ResolvedLinkRelationship:
    hub_name: str
    hub_hash_key: str
    columns: tuple[str, ...]
    hub_hash: HashConfig
    parent_asset_key: dg.AssetKey


@dataclass(frozen=True)
class ResolvedLink:
    name: str
    table: str
    hash_key: str
    sources: tuple[ResolvedSourceMapping, ...]
    relationships: tuple[ResolvedLinkRelationship, ...]
    hash: HashConfig
    asset_key: dg.AssetKey

    @property
    def parent_asset_keys(self) -> list[dg.AssetKey]:
        return [relationship.parent_asset_key for relationship in self.relationships]

    @property
    def source_asset_keys(self) -> list[dg.AssetKey]:
        return [source_mapping.source.asset_key for source_mapping in self.sources]


@dataclass(frozen=True)
class ResolvedSatellite:
    name: str
    table: str
    source: ResolvedSource
    parent_name: str
    parent_kind: str
    parent_hash_key: str
    parent_source_columns: tuple[tuple[str, str], ...]
    parent_hash: HashConfig
    attributes: tuple[str, ...]
    attribute_source_columns: tuple[tuple[str, str], ...]
    hashdiff_column: str
    load_ts_column: str | None
    asset_key: dg.AssetKey
    parent_asset_key: dg.AssetKey


@dataclass
class DataVaultModel:
    vault_name: str
    layer: str
    specification: DataVaultModelSpec
    hubs: dict[str, ResolvedHub] = field(default_factory=dict)
    links: dict[str, ResolvedLink] = field(default_factory=dict)
    satellites: dict[str, ResolvedSatellite] = field(default_factory=dict)

    def parent(self, name: str) -> ResolvedHub | ResolvedLink:
        return self.hubs.get(name) or self.links[name]


def _resolve_source(spec: VaultSourceSpec, registry: ContractRegistry) -> ResolvedSource:
    contract = registry.resolve(spec.contract)
    table_name = spec.table or contract.table_name
    pipeline = spec.pipeline_name or spec.name
    asset_key = (
        dg.AssetKey(spec.asset_key)
        if spec.asset_key
        else dg.AssetKey([pipeline, spec.layer, table_name])
    )
    return ResolvedSource(
        name=spec.name,
        contract=contract,
        table_name=table_name,
        layer=spec.layer,
        asset_key=asset_key,
        record_source=spec.record_source or spec.name,
    )


def _resolve_source_mappings(
    canonical_columns: list[str],
    source_specs: list[SourceMappingSpec],
    sources: dict[str, ResolvedSource],
) -> list[ResolvedSourceMapping]:
    """Resolve each source and map its physical columns; no validation."""

    return [
        ResolvedSourceMapping(
            source=sources[source_spec.name],
            columns=tuple(
                (column, source_spec.columns.get(column, column)) for column in canonical_columns
            ),
        )
        for source_spec in source_specs
    ]


def _hash_config(
    columns: Iterable[str], *, prefix: str | None, model: DataVaultModelSpec
) -> HashConfig:
    return HashConfig(
        columns=tuple(columns),
        delimiter=model.hash_delimiter,
        prefix=prefix,
        normalize=model.normalize_key_values,
    )


def _resolve_hub(
    spec: HubSpec, sources: dict[str, ResolvedSource], model: DataVaultModelSpec
) -> ResolvedHub:
    table = spec.table or f"hub_{spec.name}"
    return ResolvedHub(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        sources=tuple(_resolve_source_mappings(spec.business_keys, spec.sources, sources)),
        business_keys=tuple(spec.business_keys),
        hash=_hash_config(spec.business_keys, prefix=spec.hash_prefix, model=model),
        asset_key=dg.AssetKey([model.vault_name, HUB_KIND, table]),
    )


def _resolve_link(
    spec: LinkSpec,
    sources: dict[str, ResolvedSource],
    hubs: dict[str, ResolvedHub],
    model: DataVaultModelSpec,
) -> ResolvedLink:
    resolved_relationships = []
    key_columns: list[str] = []
    for relationship in spec.relationships:
        hub = hubs[relationship.hub]
        resolved_relationships.append(
            ResolvedLinkRelationship(
                hub_name=hub.name,
                hub_hash_key=hub.hash_key,
                columns=tuple(relationship.columns),
                hub_hash=_hash_config(relationship.columns, prefix=hub.hash.prefix, model=model),
                parent_asset_key=hub.asset_key,
            )
        )
        key_columns.extend(relationship.columns)

    table = spec.table or f"lnk_{spec.name}"
    return ResolvedLink(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        sources=tuple(_resolve_source_mappings(key_columns, spec.sources, sources)),
        relationships=tuple(resolved_relationships),
        hash=_hash_config(key_columns, prefix=spec.hash_prefix, model=model),
        asset_key=dg.AssetKey([model.vault_name, LINK_KIND, table]),
    )


def _resolve_satellite(
    spec: SatelliteSpec,
    parent: ResolvedHub | ResolvedLink,
    model: DataVaultModelSpec,
) -> ResolvedSatellite:
    parent_mapping = parent.sources[0]
    source = parent_mapping.source
    parent_kind = HUB_KIND if isinstance(parent, ResolvedHub) else LINK_KIND
    parent_source_columns = parent_mapping.columns
    parent_columns = [canonical for canonical, _ in parent_source_columns]

    attributes = list(spec.attributes) or list(spec.columns)
    columns = dict(spec.columns)
    if not attributes:
        excluded = (
            set(parent_columns)
            | {physical for _, physical in parent_source_columns}
            | set(source.contract.primary_key_columns)
        )
        attributes = [
            column
            for column in source.contract.column_names
            if column not in excluded and not column.startswith(METADATA_COLUMN_PREFIX)
        ]
        columns = {attribute: attribute for attribute in attributes}
    attribute_source_columns = tuple(
        (attribute, columns.get(attribute, attribute)) for attribute in attributes
    )

    table = spec.table or f"sat_{spec.name}"
    return ResolvedSatellite(
        name=spec.name,
        table=table,
        source=source,
        parent_name=parent.name,
        parent_kind=parent_kind,
        parent_hash_key=parent.hash_key,
        parent_source_columns=parent_source_columns,
        parent_hash=parent.hash,
        attributes=tuple(attributes),
        attribute_source_columns=attribute_source_columns,
        hashdiff_column=spec.hashdiff_column,
        load_ts_column=spec.load_ts_column,
        asset_key=dg.AssetKey([model.vault_name, SAT_KIND, table]),
        parent_asset_key=parent.asset_key,
    )


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def build_model(spec: DataVaultModelSpec, registry: ContractRegistry) -> DataVaultModel:
    """Resolve the vault model; invalid references fail when queried."""

    sources = {source.name: _resolve_source(source, registry) for source in spec.sources}
    model = DataVaultModel(
        vault_name=spec.vault_name,
        layer=f"{spec.layer}/{spec.vault_name}",
        specification=spec,
    )

    for hub_spec in spec.hubs:
        model.hubs[hub_spec.name] = _resolve_hub(hub_spec, sources, spec)
    for link_spec in spec.links:
        model.links[link_spec.name] = _resolve_link(link_spec, sources, model.hubs, spec)
    for satellite_spec in spec.satellites:
        model.satellites[satellite_spec.name] = _resolve_satellite(
            satellite_spec, model.parent(satellite_spec.parent), spec
        )
    return model


__all__ = [
    "DeltaTableWriter",
    "DeltaWriteOptions",
    "WriteMode",
    "build_trusted_prefixes",
    "parse_contract_document",
]
