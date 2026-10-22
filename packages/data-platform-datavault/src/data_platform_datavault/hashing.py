"""Deterministic hash keys and hashdiffs for Data Vault 2.0 tables.

The same payload rules are implemented twice on purpose:

- :func:`hash_expression` builds the Spark column expression used at load time.
- :func:`hash_value` reproduces it in pure Python for tests and tooling.

Values are normalized (trimmed + uppercased) unless disabled, and nulls hash
as the Data Vault placeholder ``^^``. Keep the two implementations in sync.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pyspark.sql import functions as F

HashAlgorithm = Literal["md5", "sha256"]
NULL_PLACEHOLDER = "^^"


def normalize_value(value: Any, *, normalize: bool = True) -> str:
    """String form of a value as Spark casts it, before hashing."""

    if value is None:
        return NULL_PLACEHOLDER
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, datetime):
        text = value.isoformat(sep=" ")
    elif isinstance(value, date):
        text = value.isoformat()
    elif isinstance(value, Decimal):
        text = format(value, "f")
    else:
        text = str(value)
    return text.strip().upper() if normalize else text


def hash_payload(
    values: Sequence[Any],
    *,
    delimiter: str = "||",
    prefix: str | None = None,
    normalize: bool = True,
) -> str:
    parts = [normalize_value(value, normalize=normalize) for value in values]
    payload_parts = ([prefix] if prefix is not None else []) + parts
    return delimiter.join(payload_parts)


def hash_value(
    values: Sequence[Any],
    *,
    algorithm: HashAlgorithm = "md5",
    delimiter: str = "||",
    prefix: str | None = None,
    normalize: bool = True,
) -> str:
    payload = hash_payload(values, delimiter=delimiter, prefix=prefix, normalize=normalize)
    digest = (
        hashlib.md5(payload.encode("utf-8"))
        if algorithm == "md5"
        else hashlib.sha256(payload.encode("utf-8"))
    )
    return digest.hexdigest().upper()


def hash_expression(
    columns: Sequence[str],
    *,
    algorithm: HashAlgorithm = "md5",
    delimiter: str = "||",
    prefix: str | None = None,
    normalize: bool = True,
) -> F.Column:
    """Spark expression computing the hash key/hashdiff for ``columns``."""

    parts: list[F.Column] = []
    for column in columns:
        source = F.col(column).cast("string")
        if normalize:
            source = F.upper(F.trim(source))
        parts.append(F.coalesce(source, F.lit(NULL_PLACEHOLDER)))
    payload_parts = ([F.lit(prefix)] if prefix is not None else []) + parts
    payload = F.concat_ws(delimiter, *payload_parts)
    digest = F.md5(payload) if algorithm == "md5" else F.sha2(payload, 256)
    return F.upper(digest)


def hash_values_by_column(
    row: Mapping[str, Any],
    columns: Sequence[str],
    *,
    algorithm: HashAlgorithm = "md5",
    delimiter: str = "||",
    prefix: str | None = None,
    normalize: bool = True,
) -> str:
    """Hash one record using the same rules as :func:`hash_expression`."""

    return hash_value(
        [row.get(column) for column in columns],
        algorithm=algorithm,
        delimiter=delimiter,
        prefix=prefix,
        normalize=normalize,
    )
