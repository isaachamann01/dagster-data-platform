"""Contract-derived Dagster asset checks.

Every :class:`~data_platform_common.validation.frame.CheckSpec` produced from a
contract becomes one Dagster asset check, so the validation gate in the assets
and the checks surfaced in the UI are always the same logic.

Note: this module intentionally avoids ``from __future__ import annotations`` so
Dagster decorators receive real annotation objects for ``context``.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_platform_common.contracts.models import DataContract, ForeignKey
from data_platform_common.io.delta import DeltaTableWriter
from data_platform_common.validation.frame import CheckSpec, ValidationContext, iter_check_specs

DEFAULT_SPARK_KEY = "spark"
DEFAULT_LAKEHOUSE_KEY = "lakehouse"
DEFAULT_LAYER = "silver"

SEVERITY_MAP = {
    "error": dg.AssetCheckSeverity.ERROR,
    "warn": dg.AssetCheckSeverity.WARN,
}


@dataclass(frozen=True)
class ForeignKeyParent:
    """Where to find a referenced parent table at check/validation time."""

    table: str
    columns: list[str]
    layer: str = DEFAULT_LAYER
    asset_key: dg.AssetKey | None = None

    def fk_key(self, foreign_key: ForeignKey) -> str:
        return "+".join(foreign_key.columns)


def build_asset_checks(
    contract: DataContract,
    asset_key: dg.AssetKey,
    *,
    layer: str,
    resource_keys: Mapping[str, str] | None = None,
    foreign_key_parents: Mapping[str, ForeignKeyParent] | None = None,
    extra_columns: Sequence[str] = (),
) -> list[dg.AssetChecksDefinition]:
    """Build one Dagster asset check per contract check."""

    keys = dict(resource_keys or {"spark": DEFAULT_SPARK_KEY, "lakehouse": DEFAULT_LAKEHOUSE_KEY})
    spark_key = keys.get("spark", DEFAULT_SPARK_KEY)
    lakehouse_key = keys.get("lakehouse", DEFAULT_LAKEHOUSE_KEY)
    parents = dict(foreign_key_parents or {})
    context_extras = frozenset(extra_columns)

    checks: list[dg.AssetChecksDefinition] = []
    for spec in iter_check_specs(contract):
        dependencies: list[dg.AssetKey] = []
        foreign_key: ForeignKey | None = None
        if spec.name.startswith("foreign_key__"):
            foreign_key = next(
                (
                    candidate
                    for candidate in contract.foreign_keys
                    if f"foreign_key__{'+'.join(candidate.columns)}" == spec.name
                ),
                None,
            )
            if foreign_key is not None:
                parent = parents.get("+".join(foreign_key.columns))
                if parent is not None and parent.asset_key is not None:
                    dependencies.append(parent.asset_key)

        checks.append(
            _make_check(
                spec,
                contract,
                asset_key=asset_key,
                layer=layer,
                spark_key=spark_key,
                lakehouse_key=lakehouse_key,
                foreign_key=foreign_key,
                foreign_key_parents=parents,
                extra_columns=context_extras,
                dependencies=dependencies,
            )
        )
    return checks


def _make_check(
    spec: CheckSpec,
    contract: DataContract,
    *,
    asset_key: dg.AssetKey,
    layer: str,
    spark_key: str,
    lakehouse_key: str,
    foreign_key: ForeignKey | None,
    foreign_key_parents: Mapping[str, ForeignKeyParent],
    extra_columns: frozenset[str],
    dependencies: list[dg.AssetKey],
) -> dg.AssetChecksDefinition:
    severity = SEVERITY_MAP.get(spec.severity, dg.AssetCheckSeverity.ERROR)

    @dg.asset_check(
        asset=asset_key,
        name=spec.name,
        description=spec.description,
        blocking=False,
        additional_deps=dependencies or None,
        required_resource_keys={spark_key, lakehouse_key},
    )
    def _check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
        spark_resource = getattr(context.resources, spark_key)
        lakehouse = getattr(context.resources, lakehouse_key)
        spark = spark_resource.get_session()
        lakehouse.configure_spark(spark)
        path = lakehouse.table_path(layer, contract.table_name)
        if not DeltaTableWriter(spark).table_exists(path):
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"reason": f"table not materialized at {path}"},
            )

        frame = spark.read.format("delta").load(path)
        resolved: dict[str, DataFrame] = {}

        def resolve_foreign_key(key: ForeignKey) -> DataFrame:
            if foreign_key is None:  # pragma: no cover - defensive
                raise ValueError("no foreign key bound to this check")
            parent = foreign_key_parents.get("+".join(key.columns))
            if parent is None:
                raise ValueError(
                    f"No parent table is registered for foreign key "
                    f"{key.columns} -> {key.references.contract}"
                )
            if parent.table not in resolved:
                parent_path = lakehouse.table_path(parent.layer, parent.table)
                parent_frame = spark.read.format("delta").load(parent_path)
                resolved[parent.table] = parent_frame
            parent_frame = resolved[parent.table]
            return parent_frame.select(
                *[
                    F.col(parent_column).alias(child_column)
                    for child_column, parent_column in zip(key.columns, parent.columns, strict=True)
                ]
            )

        validation_context = ValidationContext(
            contract=contract,
            foreign_key_resolver=resolve_foreign_key if foreign_key is not None else None,
            extra_columns=extra_columns,
        )
        outcome = spec.run(frame, validation_context)
        metadata: dict[str, Any] = {"message": outcome.message, "check": spec.name}
        for key, value in outcome.metadata.items():
            if isinstance(value, (str, int, float, bool)):
                metadata[key] = value
        return dg.AssetCheckResult(
            passed=outcome.passed,
            severity=severity,
            metadata=metadata,
        )

    return _check


def build_foreign_key_resolver(
    foreign_key_parents: Mapping[str, ForeignKeyParent],
    *,
    spark: Any,
    lakehouse: Any,
) -> Callable[[ForeignKey], DataFrame]:
    """Runtime resolver used by asset validation (not just checks)."""

    resolved: dict[str, DataFrame] = {}

    def resolve(key: ForeignKey) -> DataFrame:
        parent = foreign_key_parents.get("+".join(key.columns))
        if parent is None:
            raise ValueError(
                f"No parent table is registered for foreign key "
                f"{key.columns} -> {key.references.contract}"
            )
        if parent.table not in resolved:
            path = lakehouse.table_path(parent.layer, parent.table)
            resolved[parent.table] = spark.read.format("delta").load(path)
        return resolved[parent.table].select(
            *[
                F.col(parent_column).alias(child_column)
                for child_column, parent_column in zip(key.columns, parent.columns, strict=True)
            ]
        )

    return resolve
