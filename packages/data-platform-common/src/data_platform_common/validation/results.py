"""Shared result models for contract validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"


@dataclass(frozen=True)
class CheckOutcome:
    """The result of a single contract check."""

    name: str
    passed: bool
    message: str
    severity: str = SEVERITY_ERROR
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return not self.passed

    def as_failure(self) -> str:
        prefix = "ERROR" if self.severity == SEVERITY_ERROR else "WARN"
        return f"[{prefix}] {self.name}: {self.message}"


@dataclass
class ValidationReport:
    """A collection of check outcomes for one dataset."""

    contract: str
    outcomes: list[CheckOutcome] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(
            outcome.failed and outcome.severity == SEVERITY_ERROR for outcome in self.outcomes
        )

    @property
    def failures(self) -> list[CheckOutcome]:
        return [outcome for outcome in self.outcomes if outcome.failed]

    @property
    def errors(self) -> list[CheckOutcome]:
        return [
            outcome
            for outcome in self.outcomes
            if outcome.failed and outcome.severity == SEVERITY_ERROR
        ]

    @property
    def warnings(self) -> list[CheckOutcome]:
        return [
            outcome
            for outcome in self.outcomes
            if outcome.failed and outcome.severity == SEVERITY_WARN
        ]

    def failure_messages(self) -> list[str]:
        return [outcome.as_failure() for outcome in self.failures]

    def summary(self) -> dict[str, int]:
        return {
            "checks": len(self.outcomes),
            "failed": len(self.failures),
            "errors": len(self.errors),
            "warnings": len(self.warnings),
        }
