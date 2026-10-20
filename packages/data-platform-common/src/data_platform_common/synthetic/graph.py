"""Contract relationship graph for synthetic generation."""

from __future__ import annotations

from collections.abc import Mapping

from data_platform_common.contracts.models import DataContract
from data_platform_common.errors import CircularDependencyError, MissingDependencyError


def dependency_graph(contracts: Mapping[str, DataContract]) -> dict[str, set[str]]:
    """Map each contract to the contracts it depends on (its parents)."""

    graph: dict[str, set[str]] = {name: set() for name in contracts}
    for name, contract in contracts.items():
        for foreign_key in contract.foreign_keys:
            parent = foreign_key.references.contract
            if parent not in contracts:
                raise MissingDependencyError(
                    f"Contract '{name}' references missing contract '{parent}'. "
                    f"Provide it in the generation set (known: {sorted(contracts)})"
                )
            if parent == name:
                raise CircularDependencyError(
                    f"Contract '{name}' references itself through a foreign key"
                )
            graph[name].add(parent)
    return graph


def topological_order(contracts: Mapping[str, DataContract]) -> list[str]:
    """Return parents before children; raise on missing dependencies or cycles."""

    graph = dependency_graph(contracts)
    remaining = {name: set(parents) for name, parents in graph.items()}
    ordered: list[str] = []
    while remaining:
        ready = sorted(name for name, parents in remaining.items() if not parents)
        if not ready:
            cycle = _find_cycle(remaining)
            raise CircularDependencyError(
                "Circular contract relationships detected: " + " -> ".join(cycle)
            )
        for name in ready:
            ordered.append(name)
            del remaining[name]
        for parents in remaining.values():
            parents.difference_update(ready)
    return ordered


def dependency_closure(contracts: Mapping[str, DataContract], root: str) -> set[str]:
    """All contracts needed to generate ``root`` (its ancestors plus itself)."""

    if root not in contracts:
        raise MissingDependencyError(f"Unknown contract '{root}' (known: {sorted(contracts)})")
    graph = dependency_graph(contracts)
    closure = {root}
    stack = [root]
    while stack:
        current = stack.pop()
        for parent in graph[current]:
            if parent not in closure:
                closure.add(parent)
                stack.append(parent)
    return closure


def _find_cycle(graph: Mapping[str, set[str]]) -> list[str]:
    visited: set[str] = set()
    stack: list[str] = []
    on_stack: set[str] = set()

    def visit(node: str) -> list[str] | None:
        visited.add(node)
        stack.append(node)
        on_stack.add(node)
        for neighbour in sorted(graph.get(node, ())):
            if neighbour not in visited:
                cycle = visit(neighbour)
                if cycle is not None:
                    return cycle
            elif neighbour in on_stack:
                start = stack.index(neighbour)
                return [*stack[start:], neighbour]
        stack.pop()
        on_stack.discard(node)
        return None

    for node in sorted(graph):
        if node not in visited:
            cycle = visit(node)
            if cycle is not None:
                return cycle
    return []  # pragma: no cover - defensive
