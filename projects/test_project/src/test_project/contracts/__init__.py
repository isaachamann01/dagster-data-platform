"""Contract path helpers for the reusable-component test project."""

from __future__ import annotations

from pathlib import Path

CONTRACTS_DIR = Path(__file__).parent


def get_tiny_silver_contract_path() -> Path:
    return CONTRACTS_DIR / "tiny_silver.yaml"


def get_tiny_gold_contract_path() -> Path:
    return CONTRACTS_DIR / "tiny_gold.yaml"


def get_tiny_store_silver_contract_path() -> Path:
    return CONTRACTS_DIR / "tiny_store_silver.yaml"


def get_tiny_order_silver_contract_path() -> Path:
    return CONTRACTS_DIR / "tiny_order_silver.yaml"


__all__ = [
    "CONTRACTS_DIR",
    "get_tiny_gold_contract_path",
    "get_tiny_order_silver_contract_path",
    "get_tiny_silver_contract_path",
    "get_tiny_store_silver_contract_path",
]
