"""Resolved Data Vault 2.0 model: hubs, links, and satellites.

The factory supports two modes:

- **explicit**: hubs/links/satellites are declared in ``defs.yaml``.
- **auto_generate**: the model is derived from the silver contracts — primary
  keys become hubs, foreign keys become links, and the remaining columns form
  satellites attached to the child hub.

Either way the result is a validated :class:`DataVaultModel` that the asset and
check builders consume.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import dagster as dg

from data_platform_common.contracts.models import DataContract
from data_platform_common.contracts.registry import ContractRegistry
from data_platform_common.datavault.config import (
    DataVaultModelSpec,
    HubSpec,
    LinkRelationship,
    LinkSpec,
    SatelliteSpec,
    VaultSourceSpec,
)
from data_platform_common.errors import ContractError
from data_platform_common.spark_types import METADATA_COLUMN_PREFIX

HUB_KIND = "hub"
LINK_KIND = "link"
SAT_KIND = "sat"


@dataclass(frozen=True)
class HashConfig:
    """How one hash key / hashdiff is computed."""

    columns: tuple[str, ...]
    algorithm: str
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

    def parent_hash_value(self, row: dict[str, object]) -> str:
        from data_platform_common.datavault.hashing import hash_values_by_column

        return hash_values_by_column(
            row,
            self.parent_columns,
            algorithm=self.parent_hash.algorithm,  # type: ignore[arg-type]
            delimiter=self.parent_hash.delimiter,
            prefix=self.parent_hash.prefix,
            normalize=self.parent_hash.normalize,
        )


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


def _hub_hash_config(spec: HubSpec, spec_model: DataVaultModelSpec) -> HashConfig:
    return HashConfig(
        columns=tuple(spec.business_keys),
        algorithm=spec_model.hash_algorithm,
        delimiter=spec_model.hash_delimiter,
        prefix=spec.hash_prefix,
        normalize=spec_model.normalize_key_values,
    )


def _link_hash_config(
    spec: LinkSpec,
    relationships: tuple[ResolvedLinkRelationship, ...],
    spec_model: DataVaultModelSpec,
) -> HashConfig:
    return HashConfig(
        columns=tuple(column for relationship in relationships for column in relationship.columns),
        algorithm=spec_model.hash_algorithm,
        delimiter=spec_model.hash_delimiter,
        prefix=spec.hash_prefix,
        normalize=spec_model.normalize_key_values,
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
        hash=_hub_hash_config(spec, model),
        asset_key=dg.AssetKey([model.vault_name, HUB_KIND, table]),
    )


def _relationship_hash_config(relationship: LinkRelationship, hub: ResolvedHub) -> HashConfig:
    """Hash the relationship's own columns with the hub's hashing rules.

    The columns may be named differently from the hub business keys (for example
    ``store_key`` in an association table), but they carry the same values, so
    the resulting hash matches the hub's hash key.
    """

    return HashConfig(
        columns=tuple(relationship.columns),
        algorithm=hub.hash.algorithm,
        delimiter=hub.hash.delimiter,
        prefix=hub.hash.prefix,
        normalize=hub.hash.normalize,
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
                hub_hash=_relationship_hash_config(relationship, hub),
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

    relationships_tuple = tuple(resolved_relationships)
    table = spec.table or f"lnk_{spec.name}"
    return ResolvedLink(
        name=spec.name,
        table=table,
        hash_key=spec.hash_key or f"{table}_hk",
        source=source,
        sources=tuple(used_sources.values()),
        relationships=relationships_tuple,
        hash=_link_hash_config(spec, relationships_tuple, model),
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
        parent_hash = parent.hash
        parent_asset_key = parent.asset_key
    else:
        parent_kind = LINK_KIND
        parent_columns = parent.hash.columns
        parent_hash = parent.hash
        parent_asset_key = parent.asset_key

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
        parent_hash=parent_hash,
        attributes=tuple(attributes),
        hashdiff_column=spec.hashdiff_column,
        load_ts_column=spec.load_ts_column,
        asset_key=dg.AssetKey([model.vault_name, SAT_KIND, table]),
        parent_asset_key=parent_asset_key,
    )


# --------------------------------------------------------------------------- #
# Auto generation
# --------------------------------------------------------------------------- #


def derive_specs(
    sources: dict[str, ResolvedSource],
) -> tuple[list[HubSpec], list[LinkSpec], list[SatelliteSpec]]:
    """Derive hubs, links, and satellites from the silver contracts."""

    contract_to_source: dict[str, ResolvedSource] = {}
    for source in sources.values():
        contract_to_source.setdefault(source.contract.name, source)

    hubs: list[HubSpec] = []
    links: list[LinkSpec] = []
    satellites: list[SatelliteSpec] = []
    seen_links: set[str] = set()

    for source in sources.values():
        contract = source.contract
        primary_key = list(contract.primary_key_columns)
        if primary_key:
            hubs.append(HubSpec(name=contract.name, source=source.name, business_keys=primary_key))

        foreign_columns = {column for fk in contract.foreign_keys for column in fk.columns}
        if primary_key:
            for fk in contract.foreign_keys:
                parent = contract_to_source.get(fk.references.contract)
                if parent is None:
                    raise ContractError(
                        f"auto_generate: contract '{contract.name}' references "
                        f"'{fk.references.contract}' which is not part of the vault sources"
                    )
                if not parent.contract.primary_key_columns:
                    raise ContractError(
                        f"auto_generate: parent contract '{parent.contract.name}' has no "
                        "primary key to build a hub from"
                    )
                link_name = f"{contract.name}_{parent.contract.name}"
                if link_name in seen_links:
                    raise ContractError(
                        f"auto_generate: duplicate derived link '{link_name}'; declare the model "
                        "explicitly to disambiguate"
                    )
                seen_links.add(link_name)
                links.append(
                    LinkSpec(
                        name=link_name,
                        relationships=[
                            LinkRelationship(
                                hub=contract.name, columns=primary_key, source=source.name
                            ),
                            LinkRelationship(
                                hub=parent.contract.name,
                                columns=list(fk.columns),
                                source=source.name,
                            ),
                        ],
                    )
                )

        attribute_columns = [
            column
            for column in contract.column_names
            if column not in primary_key
            and column not in foreign_columns
            and not column.startswith(METADATA_COLUMN_PREFIX)
        ]
        if primary_key and attribute_columns:
            satellites.append(
                SatelliteSpec(
                    name=contract.name,
                    source=source.name,
                    parent=contract.name,
                    attributes=attribute_columns,
                )
            )
    return hubs, links, satellites


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def build_model(spec: DataVaultModelSpec, registry: ContractRegistry) -> DataVaultModel:
    """Resolve and validate a vault model against the configured contracts."""

    sources = {source.name: _resolve_source(source, registry) for source in spec.sources}

    if spec.auto_generate:
        hub_specs, link_specs, satellite_specs = derive_specs(sources)
    else:
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
