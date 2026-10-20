"""Batch generation of related contracts with foreign-key integrity."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from data_platform_common.contracts.models import DataContract
from data_platform_common.synthetic.generator import SyntheticGenerator
from data_platform_common.synthetic.graph import dependency_closure, topological_order


@dataclass
class BatchGenerationResult:
    """Generated datasets plus generation metadata."""

    datasets: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    seed: int | None = None
    rows: dict[str, int] = field(default_factory=dict)


def _resolve_parent_columns(contract: DataContract, parent: DataContract) -> list[str]:
    for foreign_key in contract.foreign_keys:
        if foreign_key.references.contract == parent.name:
            return list(foreign_key.references.columns) or parent.primary_key_columns
    return parent.primary_key_columns


def _rows_for(rows: int | Mapping[str, int], name: str) -> int:
    if isinstance(rows, int):
        return rows
    return rows.get(name, 10)


def generate_related(
    contracts: Mapping[str, DataContract],
    root: str,
    *,
    rows: int | Mapping[str, int] = 10,
    seed: int | None = 0,
    null_probability: float | None = None,
) -> BatchGenerationResult:
    """Generate ``root`` and all its dependencies, parents first.

    Child foreign keys are populated from the generated parent key values, so
    the resulting datasets are referentially consistent by construction.
    """

    closure = dependency_closure(contracts, root)
    subset = {name: contracts[name] for name in closure}
    order = topological_order(subset)

    result = BatchGenerationResult(seed=seed)
    key_pools: dict[tuple[str, str], list[Any]] = {}

    for name in order:
        contract = subset[name]
        foreign_key_values: dict[tuple[str, str], Sequence[Any]] = {}
        for foreign_key in contract.foreign_keys:
            parent = contracts[foreign_key.references.contract]
            parent_columns = _resolve_parent_columns(contract, parent)
            for child_column, parent_column in zip(
                foreign_key.columns, parent_columns, strict=True
            ):
                pool = key_pools.get((parent.name, parent_column))
                if pool:
                    foreign_key_values[(parent.name, child_column)] = pool
            first_pool = key_pools.get((parent.name, parent_columns[0]))
            if first_pool:
                foreign_key_values[(parent.name, "*")] = first_pool

        generator = SyntheticGenerator(
            contract,
            seed=seed,
            null_probability=null_probability,
            foreign_key_values=foreign_key_values,
        )
        row_count = _rows_for(rows, name)
        dataset = generator.generate(row_count)
        result.datasets[name] = dataset
        result.order.append(name)
        result.rows[name] = row_count

        # Record pools for every column other contracts may reference: the
        # parent's own key columns, plus any explicitly referenced columns.
        referenced: set[str] = set()
        for other in contracts.values():
            for other_fk in other.foreign_keys:
                if other_fk.references.contract != name:
                    continue
                referenced.update(other_fk.references.columns or contract.primary_key_columns)
        key_columns = (
            referenced
            | set(contract.primary_key)
            | {column for key in contract.unique_keys for column in key}
        )
        for key_column in sorted(key_columns):
            key_pools[(contract.name, key_column)] = [
                row[key_column] for row in dataset if row.get(key_column) is not None
            ]

    return result
