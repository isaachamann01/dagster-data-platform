"""Load data contracts from YAML files, dotted callables, or registries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from data_platform_common.contracts.models import DataContract
from data_platform_common.errors import ContractError
from data_platform_common.imports import resolve_dotted


def _require_mapping(payload: Any, source: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise ContractError(f"Contract source '{source}' did not produce a mapping")
    return payload


def parse_contract_document(
    payload: Mapping[str, Any], *, source: str = "<inline>"
) -> DataContract:
    """Parse a single YAML/JSON mapping into a validated contract."""

    try:
        return DataContract.model_validate(dict(payload))
    except Exception as error:  # pydantic ValidationError, wrapped with source context
        raise ContractError(f"Invalid contract from {source}: {error}") from error


def load_contract_file(path: Path) -> DataContract:
    """Load a single contract from a YAML or JSON file."""

    if not path.exists():
        raise ContractError(f"Contract file does not exist: {path}")
    try:
        documents = [doc for doc in yaml.safe_load_all(path.read_text()) if doc is not None]
    except yaml.YAMLError as error:
        raise ContractError(f"Could not parse contract file {path}: {error}") from error
    if not documents:
        raise ContractError(f"Contract file is empty: {path}")
    if len(documents) > 1:
        raise ContractError(
            f"Contract file {path} contains {len(documents)} documents; expected exactly one"
        )
    return parse_contract_document(_require_mapping(documents[0], str(path)), source=str(path))


def load_contracts_dir(directory: Path) -> dict[str, DataContract]:
    """Load every ``*.yaml``/``*.yml``/``*.json`` contract in a directory tree."""

    if not directory.exists():
        raise ContractError(f"Contract directory does not exist: {directory}")
    contracts: dict[str, DataContract] = {}
    patterns = ("*.yaml", "*.yml", "*.json")
    paths = sorted(path for pattern in patterns for path in directory.rglob(pattern))
    for path in paths:
        contract = load_contract_file(path)
        if contract.name in contracts:
            raise ContractError(
                f"Duplicate contract name '{contract.name}' in {path} "
                f"(already loaded from another file)"
            )
        contracts[contract.name] = contract
    return contracts


def resolve_contract_source(source: object, *, prefixes: Sequence[str]) -> DataContract:
    """Resolve a contract from a path, a mapping, or a trusted dotted callable.

    Supported source forms (as used by ``defs.yaml``):

    - ``data_contracts.get_example_silver_contract_path`` (trusted callable
      returning a ``Path``, string path, mapping, or ``DataContract``)
    - ``path/to/contract.yaml`` (path string)
    - an inline mapping (already-parsed YAML)
    - a ``DataContract`` instance
    """

    if isinstance(source, DataContract):
        return source
    if isinstance(source, Path):
        return load_contract_file(source)
    if isinstance(source, Mapping):
        return parse_contract_document(source)

    text = str(source).strip()
    looks_like_path = (
        text.endswith((".yaml", ".yml", ".json")) or text.startswith((".", "/", "~")) or "/" in text
    )
    if not looks_like_path:
        resolved = resolve_dotted(text, prefixes=prefixes)
        if callable(resolved):
            resolved = resolved()
        return resolve_contract_source(resolved, prefixes=prefixes)

    path = Path(text).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return load_contract_file(path)
