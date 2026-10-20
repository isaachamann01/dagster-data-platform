"""Dotted-path helpers referenced from defs.yaml files.

The component resolves contract references like
``data_contracts.get_example_silver_contract_path`` through the trusted import
resolver, which allows a small callable to return a path, mapping, or contract.
"""

from __future__ import annotations

from pathlib import Path

CONTRACTS_DIR = Path(__file__).parent / "yaml"


def _path(name: str) -> Path:
    return CONTRACTS_DIR / name


def get_example_silver_contract_path() -> Path:
    return _path("example_entity_silver.yaml")


def get_example_gold_contract_path() -> Path:
    return _path("example_entity_gold.yaml")


def get_example_store_silver_contract_path() -> Path:
    return _path("example_store_silver.yaml")


def get_example_store_gold_contract_path() -> Path:
    return _path("example_store_gold.yaml")


def get_example_order_silver_contract_path() -> Path:
    return _path("example_order_silver.yaml")


def get_example_order_gold_contract_path() -> Path:
    return _path("example_order_gold.yaml")


def all_contract_paths() -> list[Path]:
    return sorted(CONTRACTS_DIR.glob("*.yaml"))


__all__ = [
    "CONTRACTS_DIR",
    "all_contract_paths",
    "get_example_gold_contract_path",
    "get_example_order_gold_contract_path",
    "get_example_order_silver_contract_path",
    "get_example_silver_contract_path",
    "get_example_store_gold_contract_path",
    "get_example_store_silver_contract_path",
]
