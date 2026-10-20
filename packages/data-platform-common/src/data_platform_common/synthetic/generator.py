"""Deterministic, contract-driven synthetic data generation."""

from __future__ import annotations

import hashlib
import random
import string
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from data_platform_common.contracts.models import (
    ColumnContract,
    DataContract,
    LogicalType,
    SyntheticHints,
)
from data_platform_common.errors import SyntheticDataError
from data_platform_common.synthetic.regex import generate_from_regex
from data_platform_common.validation.records import validate_rows

DEFAULT_DATE_START = date(2023, 1, 1)
DEFAULT_DATE_END = date(2024, 12, 31)
DEFAULT_WORDS = (
    "atlas",
    "beacon",
    "cobalt",
    "delta",
    "ember",
    "forge",
    "garnet",
    "harbor",
    "ingot",
    "juniper",
    "kestrel",
    "lantern",
    "marlin",
    "nimbus",
    "onyx",
    "pylon",
    "quarry",
    "rivet",
    "summit",
    "timber",
)
MAX_UNIQUE_ATTEMPTS = 100


def deterministic_seed(seed: int | None, name: str) -> int:
    """Derive a process-stable seed from a user seed and a contract name."""

    digest = hashlib.sha256(f"{seed}:{name}".encode()).hexdigest()
    return int(digest[:16], 16)


class SyntheticGenerator:
    """Generate contract-valid rows deterministically for a given seed."""

    def __init__(
        self,
        contract: DataContract,
        *,
        seed: int | None = 0,
        null_probability: float | None = None,
        foreign_key_values: Mapping[tuple[str, str], Sequence[Any]] | None = None,
        self_validate: bool = True,
    ) -> None:
        self.contract = contract
        self.seed = seed
        self._rng = random.Random(deterministic_seed(seed, contract.name))
        self._null_probability = (
            null_probability
            if null_probability is not None
            else contract.synthetic.null_probability or 0.0
        )
        self._foreign_key_values = {
            key: list(values) for key, values in (foreign_key_values or {}).items() if values
        }
        self._sequences: dict[tuple[str, ...], int] = {}
        self._fk_remaining: dict[tuple[str, ...], list[Any]] = {}
        unique_columns = [column.name for column in contract.columns if column.unique]
        self._unique_seen: dict[str, set[str]] = {
            key: set()
            for key in dict.fromkeys(
                [
                    *contract.primary_key,
                    *unique_columns,
                    *(name for key in contract.unique_keys for name in key),
                ]
            )
        }
        self._self_validate = self_validate

    # -- public API ---------------------------------------------------------

    def generate(self, rows: int | None = None) -> list[dict[str, Any]]:
        """Generate ``rows`` contract-valid records (defaults to the contract hint)."""

        count = self.contract.synthetic.default_rows if rows is None else rows
        if count < 0:
            raise SyntheticDataError("rows must be >= 0")
        records = [self._generate_row() for _ in range(count)]
        if self._self_validate:
            failures = validate_rows(self.contract, records)
            if failures:
                raise SyntheticDataError(
                    f"Generated data violates contract '{self.contract.name}':\n"
                    + "\n".join(f"  - {failure}" for failure in failures[:10])
                )
        return records

    def generate_one(self) -> dict[str, Any]:
        return self._generate_row()

    # -- internals ----------------------------------------------------------

    def _generate_row(self) -> dict[str, Any]:
        return {
            column.name: self._generate_field(
                column,
                (column.name,),
                is_key=column.name in self.contract.primary_key or column.unique,
            )
            for column in self.contract.columns
        }

    def _generate_field(
        self, column: ColumnContract, path: tuple[str, ...], *, is_key: bool
    ) -> Any:
        pool = self._foreign_key_pool(column)
        if pool is not None:
            if is_key:
                return self._unique_from_pool(path, pool)
            return self._rng.choice(pool)

        if column.nullable and not is_key and self._rng.random() < self._null_probability:
            return None

        if is_key:
            return self._generate_unique(column, path)
        return self._generate_value(column)

    def _unique_from_pool(self, path: tuple[str, ...], pool: Sequence[Any]) -> Any:
        """Sample without replacement when a foreign key is also unique."""

        remaining = self._fk_remaining.get(path)
        if remaining is None:
            remaining = list(pool)
            self._rng.shuffle(remaining)
            self._fk_remaining[path] = remaining
        if not remaining:
            raise SyntheticDataError(
                f"foreign key value pool exhausted for '{'.'.join(path)}'; "
                "generate more parent rows or fewer child rows"
            )
        return remaining.pop()

    def _foreign_key_pool(self, column: ColumnContract) -> list[Any] | None:
        if column.references is None:
            return None
        values = self._foreign_key_values.get((column.references.contract, column.name))
        if values is None:
            values = self._foreign_key_values.get((column.references.contract, "*"))
        return values or None

    def _generate_unique(self, column: ColumnContract, path: tuple[str, ...]) -> Any:
        seen = self._unique_seen.setdefault(path[0], set())
        for _ in range(MAX_UNIQUE_ATTEMPTS):
            value = self._generate_value(column, force_sequence=True)
            marker = repr(value)
            if marker not in seen and value is not None:
                seen.add(marker)
                return value
        raise SyntheticDataError(
            f"Could not generate a unique value for '{'.'.join(path)}' after "
            f"{MAX_UNIQUE_ATTEMPTS} attempts"
        )

    def _generate_value(self, column: ColumnContract, *, force_sequence: bool = False) -> Any:
        hints = column.synthetic or SyntheticHints()
        if column.allowed_values is not None:
            return self._rng.choice(column.allowed_values)

        match column.type:
            case LogicalType.STRING:
                return self._generate_string(column, hints, force_sequence=force_sequence)
            case LogicalType.BINARY:
                return bytes(self._rng.randrange(256) for _ in range(self._rng.randint(4, 12)))
            case LogicalType.BOOLEAN:
                return self._rng.random() < 0.5
            case LogicalType.INTEGER | LogicalType.LONG:
                return int(self._generate_number(column, hints, force_sequence=force_sequence))
            case LogicalType.FLOAT | LogicalType.DOUBLE:
                return float(self._generate_number(column, hints, force_sequence=force_sequence))
            case LogicalType.DECIMAL:
                precision = column.constraints.precision or 10
                scale = column.constraints.scale or 2
                value = Decimal(
                    str(self._generate_number(column, hints, force_sequence=force_sequence))
                )
                quantum = Decimal(1).scaleb(-scale)
                quantized = value.quantize(quantum)
                limit = Decimal(10) ** (precision - scale)
                return max(min(quantized, limit - quantum), -limit)
            case LogicalType.DATE:
                return self._generate_date(column, hints).isoformat()
            case LogicalType.TIMESTAMP:
                moment = datetime.combine(self._generate_date(column, hints), datetime.min.time())
                return (moment + timedelta(seconds=self._rng.randint(0, 86_399))).isoformat()
            case LogicalType.ARRAY:
                assert column.items is not None
                minimum = (
                    column.constraints.min_items if column.constraints.min_items is not None else 0
                )
                maximum = (
                    column.constraints.max_items
                    if column.constraints.max_items is not None
                    else max(minimum, 3)
                )
                length = self._rng.randint(minimum, maximum)
                return [self._generate_value(column.items) for _ in range(length)]
            case LogicalType.STRUCT:
                assert column.fields is not None
                return {
                    field.name: self._generate_field(field, (field.name,), is_key=False)
                    for field in column.fields
                }
        raise SyntheticDataError(f"Unsupported logical type: {column.type}")

    def _generate_string(
        self, column: ColumnContract, hints: SyntheticHints, *, force_sequence: bool
    ) -> str:
        if hints.choices:
            return str(self._rng.choice(hints.choices))
        if hints.distribution == "sequential" or force_sequence and hints.prefix:
            counter = self._sequences.get((column.name,), hints.sequence_start - 1) + 1
            self._sequences[(column.name,)] = counter
            return f"{hints.prefix or ''}{counter:06d}"
        if column.constraints.regex is not None:
            return generate_from_regex(column.constraints.regex, self._rng)

        minimum = (
            hints.min_length if hints.min_length is not None else column.constraints.min_length
        )
        maximum = (
            hints.max_length if hints.max_length is not None else column.constraints.max_length
        )
        minimum = 1 if minimum is None else minimum
        maximum = maximum if maximum is not None else max(minimum, 16)
        word = self._rng.choice(DEFAULT_WORDS)
        suffix = str(self._rng.randint(0, 9999))
        text = f"{hints.prefix or ''}{word}-{suffix}"
        while len(text) < minimum:
            text += self._rng.choice(string.ascii_lowercase)
        if len(text) > maximum:
            text = text[:maximum]
        return text

    def _generate_number(
        self, column: ColumnContract, hints: SyntheticHints, *, force_sequence: bool
    ) -> float | int:
        minimum = hints.min_value if hints.min_value is not None else column.constraints.min_value
        maximum = hints.max_value if hints.max_value is not None else column.constraints.max_value
        is_integer = column.type in (LogicalType.INTEGER, LogicalType.LONG)
        if minimum is None:
            minimum = 0 if is_integer else 0.0
        if maximum is None:
            maximum = 10_000 if is_integer else 1.0
        if hints.distribution == "normal":
            mean = hints.mean if hints.mean is not None else (minimum + maximum) / 2
            stddev = (
                hints.stddev if hints.stddev is not None else max((maximum - minimum) / 6, 1e-9)
            )
            value = self._rng.gauss(mean, stddev)
            value = max(min(value, maximum), minimum)
        elif hints.distribution == "sequential" or force_sequence:
            counter = self._sequences.get((column.name,), int(minimum) - 1) + 1
            self._sequences[(column.name,)] = counter
            value = counter
        elif is_integer:
            value = self._rng.randint(int(minimum), int(maximum))
        else:
            value = self._rng.uniform(minimum, maximum)
        return int(value) if is_integer else round(float(value), 6)

    def _generate_date(self, column: ColumnContract, hints: SyntheticHints) -> date:
        start = hints.start_date or DEFAULT_DATE_START
        end = hints.end_date or DEFAULT_DATE_END
        if end < start:
            start, end = end, start
        span = (end - start).days or 1
        return start + timedelta(days=self._rng.randint(0, span))


def generate_contract(
    contract: DataContract,
    *,
    rows: int | None = None,
    seed: int | None = 0,
    null_probability: float | None = None,
    foreign_key_values: Mapping[tuple[str, str], Sequence[Any]] | None = None,
) -> list[dict[str, Any]]:
    """Convenience wrapper around :class:`SyntheticGenerator`."""

    return SyntheticGenerator(
        contract,
        seed=seed,
        null_probability=null_probability,
        foreign_key_values=foreign_key_values,
    ).generate(rows)
