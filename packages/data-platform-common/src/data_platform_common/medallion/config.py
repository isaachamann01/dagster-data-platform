"""Configuration models for the MedallionFactory component."""

from __future__ import annotations

from typing import Any, Literal

from dagster import Resolvable
from pydantic import BaseModel, ConfigDict, Field

WriteModeName = Literal["merge", "append", "overwrite", "partition_overwrite"]


class BaseSpec(Resolvable, BaseModel):
    """Base for component attribute models.

    Resolvable lets these models participate in the Dagster component
    resolution system while keeping strict pydantic validation.
    """

    model_config = ConfigDict(extra="forbid")


class RestSourceSpec(BaseSpec):
    """How a pipeline retrieves raw records from a REST resource."""

    type: Literal["rest"] = "rest"
    resource_key: str = "rest_client"
    route: str
    table_name: str | None = None
    method: Literal["GET", "POST"] = "GET"
    params: dict[str, str] = Field(default_factory=dict)
    request_body: dict[str, Any] | None = None
    response_path: str | None = None
    timeout_seconds: float | None = None


class ContractsSpec(BaseSpec):
    """Dotted contract references (or paths) for the silver and gold layers."""

    silver: str
    gold: str | None = None


class WriteModesSpec(BaseSpec):
    """Optional per-layer write mode overrides.

    Defaults are derived: merge when merge keys are configured, partition
    overwrite when the pipeline is partitioned, full overwrite otherwise.
    """

    bronze: WriteModeName | None = None
    silver: WriteModeName | None = None
    gold: WriteModeName | None = None


class ResourceKeysSpec(BaseSpec):
    """Names of the shared resources this pipeline binds to."""

    rest: str = "rest_client"
    spark: str = "spark"
    lakehouse: str = "lakehouse"
    metadata: str = "metadata"
    io_manager: str = "delta_io_manager"
