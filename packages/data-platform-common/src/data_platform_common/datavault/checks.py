"""Data Vault 2.0 asset checks.

Generated per table: key uniqueness, non-null keys, non-empty tables,
referential integrity to parent hubs/links, and satellite hashdiff rules.

Note: this module intentionally avoids ``from __future__ import annotations`` so
Dagster decorators receive real annotation objects for ``context``.
"""

from collections.abc import Callable
from typing import Any

import dagster as dg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_platform_common.datavault.model import (
    DataVaultModel,
    ResolvedHub,
    ResolvedLink,
    ResolvedSatellite,
)
from data_platform_common.io.delta import DeltaTableWriter

CheckResult = tuple[bool, str, dict[str, Any]]
Evaluator = Callable[[DataFrame, Any, Any], CheckResult]


def _read_table(
    context: dg.AssetCheckExecutionContext, keys: dict[str, str], layer: str, table: str
) -> tuple[Any, Any, Any, DataFrame | None]:
    spark = getattr(context.resources, keys["spark"]).get_session()
    lakehouse = getattr(context.resources, keys["lakehouse"])
    lakehouse.configure_spark(spark)
    path = lakehouse.table_path(layer, table)
    if not DeltaTableWriter(spark).table_exists(path):
        return spark, lakehouse, path, None
    return spark, lakehouse, path, spark.read.format("delta").load(path)


def _make_check(
    *,
    name: str,
    description: str,
    asset_key: dg.AssetKey,
    keys: dict[str, str],
    layer: str,
    table: str,
    evaluator: Evaluator,
    dependencies: list[dg.AssetKey] | None = None,
) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=asset_key,
        name=name,
        description=description,
        blocking=False,
        additional_deps=dependencies or None,
        required_resource_keys={keys["spark"], keys["lakehouse"]},
    )
    def _check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
        _spark, _lakehouse, path, frame = _read_table(context, keys, layer, table)
        if frame is None:
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"reason": f"table not materialized at {path}"},
            )
        passed, message, metadata = evaluator(frame, _spark, _lakehouse)
        return dg.AssetCheckResult(
            passed=passed,
            metadata={"message": message, **metadata},
        )

    return _check


def _not_empty() -> Evaluator:
    def evaluate(frame: DataFrame, spark: Any, lakehouse: Any) -> CheckResult:
        empty = frame.limit(1).count() == 0
        return (not empty, "dataset is empty" if empty else "dataset is not empty", {})

    return evaluate


def _unique(name: str, columns: list[str]) -> Evaluator:
    def evaluate(frame: DataFrame, spark: Any, lakehouse: Any) -> CheckResult:
        duplicates = frame.groupBy(*columns).count().where(F.col("count") > 1)
        count = duplicates.count()
        if count:
            return (
                False,
                f"{count} duplicate key combination(s) on {columns}",
                {"duplicates": count},
            )
        return (True, f"{name} is unique", {})

    return evaluate


def _not_null(columns: list[str]) -> Evaluator:
    def evaluate(frame: DataFrame, spark: Any, lakehouse: Any) -> CheckResult:
        nulls = sum(frame.where(F.col(column).isNull()).count() for column in columns)
        if nulls:
            return (False, f"{nulls} null value(s) in {columns}", {"nulls": nulls})
        return (True, f"no nulls in {columns}", {})

    return evaluate


def _references(
    parent_table: str, column: str, *, child_columns: list[str], layer: str
) -> Evaluator:
    def evaluate(frame: DataFrame, spark: Any, lakehouse: Any) -> CheckResult:
        parent_path = lakehouse.table_path(layer, parent_table)
        if not DeltaTableWriter(spark).table_exists(parent_path):
            return (False, f"parent table not materialized at {parent_path}", {})
        parent = spark.read.format("delta").load(parent_path).select(column).distinct()
        orphans = frame.select(*child_columns).distinct().join(parent, on=column, how="left_anti")
        count = orphans.count()
        if count:
            return (
                False,
                f"{count} orphan key(s) not present in {parent_table}",
                {"orphans": count},
            )
        return (True, f"all keys exist in {parent_table}", {})

    return evaluate


def _table_checks(
    model: DataVaultModel,
    keys: dict[str, str],
    *,
    asset_key: dg.AssetKey,
    table: str,
    specs: list[tuple[str, str, Evaluator]],
    dependencies: list[dg.AssetKey] | None = None,
) -> list[dg.AssetChecksDefinition]:
    """Build one check per ``(name, description, evaluator)`` spec for one table."""

    return [
        _make_check(
            name=name,
            description=description,
            asset_key=asset_key,
            keys=keys,
            layer=model.layer,
            table=table,
            evaluator=evaluator,
            dependencies=dependencies,
        )
        for name, description, evaluator in specs
    ]


def _hub_checks(
    model: DataVaultModel, hub: ResolvedHub, keys: dict[str, str]
) -> list[dg.AssetChecksDefinition]:
    return _table_checks(
        model,
        keys,
        asset_key=hub.asset_key,
        table=hub.table,
        specs=[
            ("not_empty", f"Hub {hub.table} is not empty", _not_empty()),
            (
                "hash_key_unique",
                f"Hash key {hub.hash_key} is unique",
                _unique("hash key", [hub.hash_key]),
            ),
            (
                "hash_key_not_null",
                f"Hash key {hub.hash_key} is never null",
                _not_null([hub.hash_key]),
            ),
            (
                "business_keys_not_null",
                "Business keys are never null",
                _not_null(list(hub.business_keys)),
            ),
        ],
    )


def _link_checks(
    model: DataVaultModel, link: ResolvedLink, keys: dict[str, str]
) -> list[dg.AssetChecksDefinition]:
    specs = [
        ("not_empty", f"Link {link.table} is not empty", _not_empty()),
        (
            "hash_key_unique",
            f"Link hash key {link.hash_key} is unique",
            _unique("link hash key", [link.hash_key]),
        ),
        (
            "hub_keys_not_null",
            "Participating hub keys are never null",
            _not_null([relationship.hub_hash_key for relationship in link.relationships]),
        ),
    ]
    checks = _table_checks(model, keys, asset_key=link.asset_key, table=link.table, specs=specs)
    for relationship in link.relationships:
        hub = model.hubs[relationship.hub_name]
        checks.extend(
            _table_checks(
                model,
                keys,
                asset_key=link.asset_key,
                table=link.table,
                specs=[
                    (
                        f"hub_reference__{relationship.hub_name}",
                        f"Every row references hub {hub.table}",
                        _references(
                            hub.table,
                            hub.hash_key,
                            child_columns=[relationship.hub_hash_key],
                            layer=model.layer,
                        ),
                    )
                ],
                dependencies=[hub.asset_key],
            )
        )
    return checks


def _satellite_checks(
    model: DataVaultModel, satellite: ResolvedSatellite, keys: dict[str, str]
) -> list[dg.AssetChecksDefinition]:
    parent = model.parent(satellite.parent_name)
    checks = _table_checks(
        model,
        keys,
        asset_key=satellite.asset_key,
        table=satellite.table,
        specs=[
            ("not_empty", f"Satellite {satellite.table} is not empty", _not_empty()),
            (
                "hashdiff_not_null",
                f"Hashdiff {satellite.hashdiff_column} is never null",
                _not_null([satellite.hashdiff_column]),
            ),
            (
                "parent_hashdiff_unique",
                "(parent key, hashdiff) pairs are unique",
                _unique("satellite key", [satellite.parent_hash_key, satellite.hashdiff_column]),
            ),
        ],
    )
    checks.extend(
        _table_checks(
            model,
            keys,
            asset_key=satellite.asset_key,
            table=satellite.table,
            specs=[
                (
                    "parent_key_exists",
                    f"Parent key {satellite.parent_hash_key} exists in {parent.table}",
                    _references(
                        parent.table,
                        parent.hash_key,
                        child_columns=[satellite.parent_hash_key],
                        layer=model.layer,
                    ),
                )
            ],
            dependencies=[parent.asset_key],
        )
    )
    return checks


def build_vault_checks(
    model: DataVaultModel, keys: dict[str, str]
) -> list[dg.AssetChecksDefinition]:
    """Build all Data Vault asset checks for a resolved model."""

    checks: list[dg.AssetChecksDefinition] = []
    for hub in model.hubs.values():
        checks.extend(_hub_checks(model, hub, keys))
    for link in model.links.values():
        checks.extend(_link_checks(model, link, keys))
    for satellite in model.satellites.values():
        checks.extend(_satellite_checks(model, satellite, keys))
    return checks
