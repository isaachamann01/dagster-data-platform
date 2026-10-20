"""Dagster component entry points."""

from data_platform_common.components.data_vault_factory import DataVaultFactory
from data_platform_common.components.medallion_factory import MedallionFactory

__all__ = ["DataVaultFactory", "MedallionFactory"]
