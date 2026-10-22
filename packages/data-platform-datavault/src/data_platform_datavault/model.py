"""Data Vault 2.0 specs and resolved model.

The Pydantic specs at the top mirror the component's YAML attributes. The
resolved dataclasses below are built by :func:`build_model` and consumed by the
asset builders.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal, Self

import dagster as dg
from dagster import Resolvable
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from data_platform_datavault._common import (
    METADATA_COLUMN_PREFIX,
    ContractError,
    ContractRegistry,
    DataContract,
)


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


class SourceMappingSpec(BaseSpec):
    """One physical source and its canonical-to-physical column mapping."""

    name: str
    columns: dict[str, str] = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "source")


class SourceMappedSpec(BaseSpec):
    """Base for specs that read canonical columns from one or more sources."""

    sources: list[SourceMappingSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_source_names(self) -> Self:
        names = [source.name for source in self.sources]
        if len(set(names)) != len(names):
            label = getattr(self, "name", "vault")
            raise ValueError(f"{label}: duplicate source names in sources")
        return self


class HubSpec(SourceMappedSpec):
    """A hub: one row per distinct business key."""

    name: str
    business_keys: list[str] = Field(min_length=1)
    table: str | None = None
    hash_key: str | None = None
    hash_prefix: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _check_name(value, "hub")

    @model_validator(mode="after")
    def _validate_business_keys(self) -> Self:
        if len(set(self.business_keys)) != len(self.business_keys):
            raise ValueError(f"hub '{self.name}': duplicate business keys")
        return self


class LinkRelationship(BaseSpec):
    """A participating hub and the canonical key columns it contributes."""

    hub: str
    columns: list[str] = Field(min_length=1)


class LinkSpec(SourceMappedSpec):
    """A link: one row per distinct combination of participating hub keys.

    ``relationships`` defines the canonical key schema and hash order;
    ``sources`` maps that schema onto one or more physical silver tables. Rows
    from every source are unioned and deduplicated by the link hash.
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
    """A satellite: descriptive attributes attached to a hub or link.

    The physical source is inherited from the parent hub/link, which must
    currently have exactly one source. ``attributes`` lists canonical output
    names; ``columns`` optionally maps each canonical attribute to a differently
    named source column.
    """

    name: str
    parent: str
    attributes: list[str] = Field(default_factory=list)
    columns: dict[str, str] = Field(default_factory=dict)
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
        if name in self.hubs:
            return self.hubs[name]
        if name in self.links:
            return self.links[name]
        raise ContractError(f"satellite parent '{name}' is neither a hub nor a link")


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
    *,
    entity: str,
) -> list[ResolvedSourceMapping]:
    """Resolve and validate a canonical-to-physical mapping per source."""

    resolved: list[ResolvedSourceMapping] = []
    for source_spec in source_specs:
        source = sources.get(source_spec.name)
        if source is None:
            raise ContractError(
                f"{entity} references unknown source '{source_spec.name}' "
                f"(known: {sorted(sources)})"
            )
        missing = [column for column in canonical_columns if column not in source_spec.columns]
        if missing:
            raise ContractError(
                f"{entity}: source '{source.name}' is missing mappings for {missing}"
            )
        extra = [column for column in source_spec.columns if column not in canonical_columns]
        if extra:
            raise ContractError(f"{entity}: source '{source.name}' maps unknown columns {extra}")

        physical_columns = [source_spec.columns[column] for column in canonical_columns]
        if len(set(physical_columns)) != len(physical_columns):
            raise ContractError(
                f"{entity}: source '{source.name}' maps multiple keys to the same column"
            )
        available = set(source.contract.column_names)
        for column in physical_columns:
            if column not in available:
                raise ContractError(
                    f"{entity}: source '{source.name}' has no column '{column}' "
                    f"(contract '{source.contract.name}')"
                )
        resolved.append(
            ResolvedSourceMapping(
                source=source,
                columns=tuple(zip(canonical_columns, physical_columns, strict=True)),
            )
        )
    return resolved


def _hash_config(
    columns: Iterable[str], *, prefix: str | None, model: DataVaultModelSpec
) -> HashConfig:
    """Vault-wide hashing rules applied to one column list."""

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
        sources=tuple(
            _resolve_source_mappings(
                spec.business_keys,
                spec.sources,
                sources,
                entity=f"hub '{spec.name}'",
            )
        ),
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
    resolved_relationships: list[ResolvedLinkRelationship] = []
    key_columns: list[str] = []
    seen_hubs: set[str] = set()

    for relationship in spec.relationships:
        hub = hubs.get(relationship.hub)
        if hub is None:
            raise ContractError(
                f"link '{spec.name}': relationship references unknown hub '{relationship.hub}'"
            )
        if hub.name in seen_hubs:
            raise ContractError(f"link '{spec.name}': hub '{hub.name}' participates more than once")
        seen_hubs.add(hub.name)

        if len(relationship.columns) != len(hub.business_keys):
            raise ContractError(
                f"link '{spec.name}': relationship to hub '{hub.name}' expects "
                f"{len(hub.business_keys)} column(s) ({list(hub.business_keys)}), got "
                f"{list(relationship.columns)}"
            )
        resolved_relationships.append(
            ResolvedLinkRelationship(
                hub_name=hub.name,
                hub_hash_key=hub.hash_key,
                columns=tuple(relationship.columns),
                # Hash the relationship's own columns with the hub's rules: the
                # columns may be named differently from the hub business keys
                # (``store_key`` vs ``store_id``) but carry the same values.
                hub_hash=_hash_config(relationship.columns, prefix=hub.hash.prefix, model=model),
                parent_asset_key=hub.asset_key,
            )
        )
        key_columns.extend(relationship.columns)

    duplicates = sorted({column for column in key_columns if key_columns.count(column) > 1})
    if duplicates:
        raise ContractError(f"link '{spec.name}': duplicate link columns {duplicates}")

    table = spec.table or f"lnk_{spec.name}"
    return ResolvedLink(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        sources=tuple(
            _resolve_source_mappings(
                key_columns,
                spec.sources,
                sources,
                entity=f"link '{spec.name}'",
            )
        ),
        relationships=tuple(resolved_relationships),
        hash=_hash_config(key_columns, prefix=spec.hash_prefix, model=model),
        asset_key=dg.AssetKey([model.vault_name, LINK_KIND, table]),
    )


def _resolve_satellite(
    spec: SatelliteSpec,
    parent: ResolvedHub | ResolvedLink,
    model: DataVaultModelSpec,
) -> ResolvedSatellite:
    if len(parent.sources) != 1:
        raise ContractError(
            f"satellite '{spec.name}': parent '{parent.name}' has {len(parent.sources)} sources; "
            "satellites currently require a single-source hub or link"
        )
    parent_mapping = parent.sources[0]
    source = parent_mapping.source
    available = set(source.contract.column_names)

    if isinstance(parent, ResolvedHub):
        parent_kind = HUB_KIND
        parent_columns = parent.business_keys
    else:
        parent_kind = LINK_KIND
        parent_columns = parent.hash.columns

    parent_source_columns = parent_mapping.columns
    for canonical, physical in parent_source_columns:
        if physical not in available:
            raise ContractError(
                f"satellite '{spec.name}': source '{source.name}' is missing parent key column "
                f"'{physical}' (for '{canonical}') needed to compute '{parent.hash_key}'"
            )

    attributes = list(spec.attributes)
    columns = dict(spec.columns)
    parent_physical = {physical for _, physical in parent_source_columns}
    if not attributes and columns:
        attributes = list(columns)
    if columns:
        missing = [attribute for attribute in attributes if attribute not in columns]
        if missing:
            raise ContractError(
                f"satellite '{spec.name}': columns is missing mappings for {missing}"
            )
        extra = [attribute for attribute in columns if attribute not in attributes]
        if extra:
            raise ContractError(f"satellite '{spec.name}': columns maps unknown attributes {extra}")
    if not attributes:
        excluded = set(parent_columns) | parent_physical | set(source.contract.primary_key_columns)
        attributes = [
            column
            for column in source.contract.column_names
            if column not in excluded and not column.startswith(METADATA_COLUMN_PREFIX)
        ]
        columns = {attribute: attribute for attribute in attributes}
    attribute_source_columns = tuple(
        (attribute, columns.get(attribute, attribute)) for attribute in attributes
    )

    for canonical, physical in attribute_source_columns:
        if canonical in parent_columns:
            raise ContractError(
                f"satellite '{spec.name}': attribute '{canonical}' is a parent key column; "
                "attributes must be descriptive payload"
            )
        if physical in parent_physical:
            raise ContractError(
                f"satellite '{spec.name}': attribute '{canonical}' maps to parent key column "
                f"'{physical}'"
            )
        if physical not in available:
            raise ContractError(
                f"satellite '{spec.name}': attribute '{canonical}' maps to missing column "
                f"'{physical}' in source '{source.name}'"
            )
    if not attributes:
        raise ContractError(f"satellite '{spec.name}' has no attributes to track")

    if spec.load_ts_column is not None and spec.load_ts_column not in available:
        raise ContractError(
            f"satellite '{spec.name}': load_ts_column '{spec.load_ts_column}' is not a column "
            f"of source '{source.name}'"
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
    """Resolve and validate a vault model against the configured contracts."""

    sources = {source.name: _resolve_source(source, registry) for source in spec.sources}

    hub_specs, link_specs, satellite_specs = spec.hubs, spec.links, spec.satellites

    model = DataVaultModel(
        vault_name=spec.vault_name,
        layer=f"{spec.layer}/{spec.vault_name}",
        specification=spec,
    )

    parent_names: set[str] = set()
    satellite_names: set[str] = set()
    for hub_spec in hub_specs:
        if hub_spec.name in parent_names:
            raise ContractError(f"duplicate hub/link name '{hub_spec.name}'")
        parent_names.add(hub_spec.name)
        model.hubs[hub_spec.name] = _resolve_hub(hub_spec, sources, spec)

    for link_spec in link_specs:
        if link_spec.name in parent_names:
            raise ContractError(f"duplicate hub/link name '{link_spec.name}'")
        parent_names.add(link_spec.name)
        model.links[link_spec.name] = _resolve_link(link_spec, sources, model.hubs, spec)

    for satellite_spec in satellite_specs:
        if satellite_spec.name in satellite_names:
            raise ContractError(f"duplicate satellite name '{satellite_spec.name}'")
        parent = model.parent(satellite_spec.parent)
        satellite_names.add(satellite_spec.name)
        model.satellites[satellite_spec.name] = _resolve_satellite(satellite_spec, parent, spec)

    if not (model.hubs or model.links or model.satellites):
        raise ContractError("the vault model resolved to zero tables")
    return model
