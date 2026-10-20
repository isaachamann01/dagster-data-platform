"""Build the shared resources from environment variables.

Resource configuration lives in the environment so the same definitions run
against local development, Docker Compose, or production without edits.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any, Literal, cast

from data_platform_common.resources.lakehouse import LakehouseResource
from data_platform_common.resources.metadata import MetadataResource
from data_platform_common.resources.rest import RestResource
from data_platform_common.resources.spark import SparkResource
from data_platform_common.resources.trino import TrinoResource

SPARK_DEFAULT_CONFIG = {"spark.sql.session.timeZone": "UTC"}


def _env(env: Mapping[str, str], name: str, default: str = "") -> str:
    return env.get(name, default).strip()


def _bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = _env(env, name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = _env(env, name)
    return float(raw) if raw else default


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _env(env, name)
    return int(raw) if raw else default


def _json_object(env: Mapping[str, str], name: str) -> dict[str, str]:
    raw = _env(env, name)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{name} must be valid JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(key): str(value) for key, value in parsed.items()}


def build_rest_resource(env: Mapping[str, str]) -> RestResource:
    return RestResource(
        base_url=_env(env, "DATA_PLATFORM_REST_BASE_URL", "http://localhost:8000"),
        timeout_seconds=_float(env, "DATA_PLATFORM_REST_TIMEOUT_SECONDS", 10.0),
        max_retries=_int(env, "DATA_PLATFORM_REST_MAX_RETRIES", 3),
        backoff_seconds=_float(env, "DATA_PLATFORM_REST_BACKOFF_SECONDS", 0.5),
        headers=_json_object(env, "DATA_PLATFORM_REST_HEADERS"),
        api_key_header=_env(env, "DATA_PLATFORM_REST_API_KEY_HEADER") or None,
        api_key=_env(env, "DATA_PLATFORM_REST_API_KEY") or None,
        verify_ssl=_bool(env, "DATA_PLATFORM_REST_VERIFY_SSL", True),
    )


def build_spark_resource(env: Mapping[str, str]) -> SparkResource:
    packages = [
        item.strip()
        for item in _env(env, "DATA_PLATFORM_SPARK_EXTRA_PACKAGES").split(",")
        if item.strip()
    ]
    config = dict(SPARK_DEFAULT_CONFIG)
    config.update(_json_object(env, "DATA_PLATFORM_SPARK_CONFIG"))
    return SparkResource(
        mode=cast(
            Literal["local", "remote"], _env(env, "DATA_PLATFORM_SPARK_MODE", "local") or "local"
        ),
        master=_env(env, "DATA_PLATFORM_SPARK_MASTER", "local[2]"),
        remote_url=_env(env, "DATA_PLATFORM_SPARK_REMOTE_URL") or None,
        app_name=_env(env, "DATA_PLATFORM_SPARK_APP_NAME", "data-platform"),
        warehouse_dir=_env(env, "DATA_PLATFORM_SPARK_WAREHOUSE_DIR") or None,
        extra_packages=packages,
        config=config,
    )


def build_lakehouse_resource(env: Mapping[str, str]) -> LakehouseResource:
    return LakehouseResource(
        mode=cast(
            Literal["local", "s3"],
            _env(env, "DATA_PLATFORM_LAKEHOUSE_MODE", "local") or "local",
        ),
        local_root=_env(
            env, "DATA_PLATFORM_LAKEHOUSE_LOCAL_ROOT", "/tmp/dagster_pipelines/lakehouse"
        ),
        endpoint_url=_env(env, "DATA_PLATFORM_S3_ENDPOINT_URL") or None,
        access_key_id=_env(env, "DATA_PLATFORM_S3_ACCESS_KEY_ID") or None,
        secret_access_key=_env(env, "DATA_PLATFORM_S3_SECRET_ACCESS_KEY") or None,
        bucket=_env(env, "DATA_PLATFORM_S3_BUCKET", "lakehouse"),
        base_prefix=_env(env, "DATA_PLATFORM_S3_BASE_PREFIX", "medallion"),
        region=_env(env, "DATA_PLATFORM_S3_REGION", "us-east-1"),
        use_ssl=_bool(env, "DATA_PLATFORM_S3_USE_SSL", False),
    )


def build_trino_resource(env: Mapping[str, str]) -> TrinoResource:
    return TrinoResource(
        enabled=_bool(env, "DATA_PLATFORM_TRINO_ENABLED", False),
        host=_env(env, "DATA_PLATFORM_TRINO_HOST", "localhost"),
        port=_int(env, "DATA_PLATFORM_TRINO_PORT", 8080),
        user=_env(env, "DATA_PLATFORM_TRINO_USER", "opencode"),
        catalog=_env(env, "DATA_PLATFORM_TRINO_CATALOG", "delta"),
        schema_=_env(env, "DATA_PLATFORM_TRINO_SCHEMA", "default"),
        http_scheme=_env(env, "DATA_PLATFORM_TRINO_HTTP_SCHEME", "http"),
    )


def build_metadata_resource(env: Mapping[str, str]) -> MetadataResource:
    return MetadataResource(
        endpoint=_env(env, "DATA_PLATFORM_METADATA_ENDPOINT") or None,
        timeout_seconds=_float(env, "DATA_PLATFORM_METADATA_TIMEOUT_SECONDS", 5.0),
        api_key=_env(env, "DATA_PLATFORM_METADATA_API_KEY") or None,
    )


def build_resources_from_env(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return the resource mapping shared by every Dagster project."""

    source = os.environ if env is None else env
    spark = build_spark_resource(source)
    lakehouse = build_lakehouse_resource(source)

    from data_platform_common.io.manager import (
        DeltaLakeIOManager,  # noqa: PLC0415 - avoid import cycle
    )

    io_manager = DeltaLakeIOManager(spark=spark, lakehouse=lakehouse)
    return {
        "rest_client": build_rest_resource(source),
        "spark": spark,
        "lakehouse": lakehouse,
        "trino": build_trino_resource(source),
        "metadata": build_metadata_resource(source),
        "delta_io_manager": io_manager,
        "io_manager": io_manager,
    }
