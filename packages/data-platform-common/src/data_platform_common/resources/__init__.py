"""Reusable Dagster resources configured through environment variables."""

from data_platform_common.resources.env import build_resources_from_env
from data_platform_common.resources.lakehouse import LakehouseResource
from data_platform_common.resources.metadata import MetadataResource
from data_platform_common.resources.rest import RestResource
from data_platform_common.resources.spark import SparkResource
from data_platform_common.resources.trino import TrinoResource

__all__ = [
    "LakehouseResource",
    "MetadataResource",
    "RestResource",
    "SparkResource",
    "TrinoResource",
    "build_resources_from_env",
]
