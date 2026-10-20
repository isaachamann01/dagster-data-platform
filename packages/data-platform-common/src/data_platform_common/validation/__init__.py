"""Contract validation: record-level models and Spark frame checks."""

from data_platform_common.validation.frame import (
    CheckSpec,
    ValidationContext,
    iter_check_specs,
    run_frame_validation,
)
from data_platform_common.validation.records import build_row_model, validate_rows
from data_platform_common.validation.results import (
    SEVERITY_ERROR,
    SEVERITY_WARN,
    CheckOutcome,
    ValidationReport,
)

__all__ = [
    "SEVERITY_ERROR",
    "SEVERITY_WARN",
    "CheckOutcome",
    "CheckSpec",
    "ValidationContext",
    "ValidationReport",
    "build_row_model",
    "iter_check_specs",
    "run_frame_validation",
    "validate_rows",
]
