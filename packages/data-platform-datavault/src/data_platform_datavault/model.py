"""Resolved Data Vault 2.0 model: hubs, links, and satellites.

Hubs, links, and satellites are declared explicitly in ``defs.yaml``; the result
is a validated :class:`DataVaultModel` that the asset and check builders consume.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

import dagster as dg

from data_platform_datavault._common import (
    METADATA_COLUMN_PREFIX,
    ContractError,
    ContractRegistry,
    DataContract,
)
from data_platform_datavault.config import (
    DataVaultModelSpec,
    HubSpec,
    LinkSpec,
    SatelliteSpec,
    VaultSourceSpec,
)

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
class ResolvedHub:
    name: str
    table: str
    hash_key: str
    source: ResolvedSource
    business_keys: tuple[str, ...]
    hash: HashConfig
    asset_key: dg.AssetKey


@dataclass(frozen=True)
class ResolvedLinkRelationship:
    hub_name: str
    hub_hash_key: str
    source: ResolvedSource
    columns: tuple[str, ...]
    hub_hash: HashConfig
    parent_asset_key: dg.AssetKey


@dataclass(frozen=True)
class ResolvedLink:
    name: str
    table: str
    hash_key: str
    source: ResolvedSource
    sources: tuple[ResolvedSource, ...]
    relationships: tuple[ResolvedLinkRelationship, ...]
    hash: HashConfig
    asset_key: dg.AssetKey

    @property
    def parent_asset_keys(self) -> list[dg.AssetKey]:
        return [relationship.parent_asset_key for relationship in self.relationships]

    @property
    def source_asset_keys(self) -> list[dg.AssetKey]:
        return [source.asset_key for source in self.sources]


@dataclass(frozen=True)
class ResolvedSatellite:
    name: str
    table: str
    source: ResolvedSource
    parent_name: str
    parent_kind: str
    parent_hash_key: str
    parent_columns: tuple[str, ...]
    parent_hash: HashConfig
    attributes: tuple[str, ...]
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

    def tables(self) -> list[ResolvedHub | ResolvedLink | ResolvedSatellite]:
        return [*self.hubs.values(), *self.links.values(), *self.satellites.values()]


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


def _source_columns(source: ResolvedSource) -> set[str]:
    return set(source.contract.column_names)


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


def _source_or_error(
    source_name: str, sources: dict[str, ResolvedSource], *, entity: str
) -> ResolvedSource:
    source = sources.get(source_name)
    if source is None:
        raise ContractError(
            f"{entity} references unknown source '{source_name}' (known: {sorted(sources)})"
        )
    return source


def _resolve_hub(
    spec: HubSpec, sources: dict[str, ResolvedSource], model: DataVaultModelSpec
) -> ResolvedHub:
    source = _source_or_error(spec.source, sources, entity=f"hub '{spec.name}'")
    for key in spec.business_keys:
        if key not in _source_columns(source):
            raise ContractError(
                f"hub '{spec.name}': business key '{key}' is not a column of source "
                f"'{source.name}' (contract '{source.contract.name}')"
            )
    table = spec.table or f"hub_{spec.name}"
    return ResolvedHub(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        source=source,
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
    used_sources: dict[str, ResolvedSource] = {}
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

        relationship_source = _source_or_error(
            relationship.source,
            sources,
            entity=f"link '{spec.name}' relationship to hub '{hub.name}'",
        )
        used_sources.setdefault(relationship_source.name, relationship_source)

        if len(relationship.columns) != len(hub.business_keys):
            raise ContractError(
                f"link '{spec.name}': relationship to hub '{hub.name}' expects "
                f"{len(hub.business_keys)} column(s) ({list(hub.business_keys)}), got "
                f"{list(relationship.columns)}"
            )
        for column in relationship.columns:
            if column not in _source_columns(relationship_source):
                raise ContractError(
                    f"link '{spec.name}': column '{column}' is not a column of source "
                    f"'{relationship_source.name}' (contract '{relationship_source.contract.name}')"
                )
        resolved_relationships.append(
            ResolvedLinkRelationship(
                hub_name=hub.name,
                hub_hash_key=hub.hash_key,
                source=relationship_source,
                columns=tuple(relationship.columns),
                # Hash the relationship's own columns with the hub's rules: the
                # columns may be named differently from the hub business keys
                # (``store_key`` vs ``store_id``) but carry the same values.
                hub_hash=_hash_config(relationship.columns, prefix=hub.hash.prefix, model=model),
                parent_asset_key=hub.asset_key,
            )
        )

    key_columns = [
        column for relationship in resolved_relationships for column in relationship.columns
    ]
    candidates = [
        candidate
        for candidate in used_sources.values()
        if all(column in _source_columns(candidate) for column in key_columns)
    ]
    if not candidates:
        raise ContractError(
            f"link '{spec.name}': no declared source contains all key columns {key_columns}; "
            "consolidate the keys into one silver table or declare a source that exposes them"
        )
    source = candidates[0]

    table = spec.table or f"lnk_{spec.name}"
    return ResolvedLink(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        source=source,
        sources=tuple(used_sources.values()),
        relationships=tuple(resolved_relationships),
        hash=_hash_config(key_columns, prefix=spec.hash_prefix, model=model),
        asset_key=dg.AssetKey([model.vault_name, LINK_KIND, table]),
    )


def _resolve_satellite(
    spec: SatelliteSpec,
    sources: dict[str, ResolvedSource],
    parent: ResolvedHub | ResolvedLink,
    model: DataVaultModelSpec,
) -> ResolvedSatellite:
    source = _source_or_error(spec.source, sources, entity=f"satellite '{spec.name}'")
    available = _source_columns(source)
    if isinstance(parent, ResolvedHub):
        parent_kind = HUB_KIND
        parent_columns = parent.business_keys
    else:
        parent_kind = LINK_KIND
        parent_columns = parent.hash.columns

    for column in parent_columns:
        if column not in available:
            raise ContractError(
                f"satellite '{spec.name}': source '{source.name}' is missing parent key column "
                f"'{column}' needed to compute '{parent.hash_key}'"
            )

    attributes = list(spec.attributes)
    if not attributes:
        excluded = set(parent_columns) | set(source.contract.primary_key_columns)
        attributes = [
            column
            for column in source.contract.column_names
            if column not in excluded and not column.startswith(METADATA_COLUMN_PREFIX)
        ]
    for attribute in attributes:
        if attribute not in available:
            raise ContractError(
                f"satellite '{spec.name}': attribute '{attribute}' is not a column of source "
                f"'{source.name}'"
            )
        if attribute in parent_columns:
            raise ContractError(
                f"satellite '{spec.name}': attribute '{attribute}' is a parent key column; "
                "attributes must be descriptive payload"
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
        parent_columns=tuple(parent_columns),
        parent_hash=parent.hash,
        attributes=tuple(attributes),
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
        model.satellites[satellite_spec.name] = _resolve_satellite(
            satellite_spec, sources, parent, spec
        )

    if not model.tables():
        raise ContractError("the vault model resolved to zero tables")
    return model
