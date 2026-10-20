"""Helper module exercising transformation signature validation edge cases."""

from __future__ import annotations

from typing import Any


def takes_contract(df: Any, *, contract: Any) -> Any:
    return df


def takes_partition(df: Any, partition: Any) -> Any:
    return df


def no_arguments() -> Any:  # pragma: no cover - never called
    raise AssertionError


def unsupported_keyword(df: Any, *, unknown: Any) -> Any:  # pragma: no cover
    return df


def variadic(df: Any, **kwargs: Any) -> Any:
    return df


def positional_extra(df: Any, extra: Any) -> Any:  # pragma: no cover
    return df


def variadic_positional(df: Any, *args: Any) -> Any:  # pragma: no cover
    return df


NOT_CALLABLE = 5
