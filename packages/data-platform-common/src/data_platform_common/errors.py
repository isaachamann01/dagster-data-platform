"""Domain errors for the data platform toolkit."""

from __future__ import annotations


class DataPlatformError(Exception):
    """Base class for all data platform errors."""


class ContractError(DataPlatformError):
    """Raised when a data contract is invalid or cannot be loaded."""


class ContractNotFoundError(ContractError):
    """Raised when a referenced contract cannot be resolved."""


class UntrustedImportError(DataPlatformError):
    """Raised when a dotted import is outside the trusted package prefixes."""


class TransformationSignatureError(DataPlatformError):
    """Raised when a transformation callable has an unsupported signature."""


class MergeKeysRequiredError(DataPlatformError):
    """Raised when merge mode is requested without valid merge keys."""


class ContractValidationError(DataPlatformError):
    """Raised when data does not satisfy its contract."""

    def __init__(self, contract: str, failures: list[str], *, severity: str = "error") -> None:
        self.contract = contract
        self.failures = failures
        self.severity = severity
        detail = "\n".join(f"  - {failure}" for failure in failures)
        super().__init__(f"Contract '{contract}' validation failed:\n{detail}")


class SyntheticDataError(DataPlatformError):
    """Base class for synthetic generation failures."""


class MissingDependencyError(SyntheticDataError):
    """Raised when a referenced parent contract is missing from the generation set."""


class CircularDependencyError(SyntheticDataError):
    """Raised when contract relationships contain a cycle."""


class ResourceNotConfiguredError(DataPlatformError):
    """Raised when an optional resource is used without being configured."""
