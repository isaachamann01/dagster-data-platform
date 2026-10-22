"""Adapter to the host platform package.

This is the **only** module in ``data_platform_datavault`` that imports from the
host platform (currently ``data_platform_common``). To port the Data Vault
package to another, similar common library, retarget these imports to the
equivalent modules there — no other file needs to change.

Required host symbols:

- ``DataContract``            contract model with ``columns``/``column_names``
- ``ContractRegistry``        resolves YAML/dotted contract references
- ``parse_contract_document`` parses inline contract mappings (tests)
- ``ContractError``           error type raised for invalid vault models
- ``build_trusted_prefixes``  trusted dotted-import resolution for contracts
- ``DeltaTableWriter``        table existence checks / Delta access
- ``META_*``                  IO-manager metadata keys (write contract)
- ``METADATA_COLUMN_PREFIX``  prefix used to exclude ingestion metadata
"""

from data_platform_common.contracts import parse_contract_document
from data_platform_common.contracts.models import DataContract
from data_platform_common.contracts.registry import ContractRegistry
from data_platform_common.errors import ContractError
from data_platform_common.imports import build_trusted_prefixes
from data_platform_common.io.delta import DeltaTableWriter
from data_platform_common.io.manager import (
    META_LAYER,
    META_MERGE_KEYS,
    META_PARTITION_BY,
    META_SCHEMA_EVOLUTION,
    META_TABLE,
    META_UPDATE_MATCHED,
    META_WRITE_MODE,
)
from data_platform_common.spark_types import METADATA_COLUMN_PREFIX

__all__ = [
    "META_LAYER",
    "META_MERGE_KEYS",
    "META_PARTITION_BY",
    "META_SCHEMA_EVOLUTION",
    "META_TABLE",
    "META_UPDATE_MATCHED",
    "META_WRITE_MODE",
    "METADATA_COLUMN_PREFIX",
    "ContractError",
    "ContractRegistry",
    "DataContract",
    "DeltaTableWriter",
    "build_trusted_prefixes",
    "parse_contract_document",
]
