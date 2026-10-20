"""Resource construction from the environment."""

from __future__ import annotations

import pytest

from data_platform_common.errors import ResourceNotConfiguredError
from data_platform_common.resources import build_resources_from_env


def local_env() -> dict[str, str]:
    return {
        "DATA_PLATFORM_REST_BASE_URL": "http://example.test/api/",
        "DATA_PLATFORM_REST_MAX_RETRIES": "5",
        "DATA_PLATFORM_REST_HEADERS": '{"X-Env": "test"}',
        "DATA_PLATFORM_REST_API_KEY": "secret",
        "DATA_PLATFORM_REST_API_KEY_HEADER": "X-Key",
        "DATA_PLATFORM_LAKEHOUSE_MODE": "local",
        "DATA_PLATFORM_LAKEHOUSE_LOCAL_ROOT": "/tmp/lakehouse-test",
        "DATA_PLATFORM_SPARK_MASTER": "local[1]",
        "DATA_PLATFORM_SPARK_EXTRA_PACKAGES": "org.example:one:1.0,org.example:two:2.0",
        "DATA_PLATFORM_TRINO_ENABLED": "false",
        "DATA_PLATFORM_METADATA_ENDPOINT": "",
    }


def test_build_resources_from_env() -> None:
    resources = build_resources_from_env(local_env())
    assert set(resources) >= {
        "rest_client",
        "spark",
        "lakehouse",
        "trino",
        "metadata",
        "delta_io_manager",
        "io_manager",
    }
    rest = resources["rest_client"]
    assert rest.url_for("generate/x") == "http://example.test/api/generate/x"
    assert rest.max_retries == 5
    session = rest.session()
    assert session.headers["X-Env"] == "test"
    assert session.headers["X-Key"] == "secret"

    spark = resources["spark"]
    assert spark.master == "local[1]"
    assert spark.extra_packages == ["org.example:one:1.0", "org.example:two:2.0"]

    lakehouse = resources["lakehouse"]
    assert lakehouse.table_path("silver", "t") == "file:///tmp/lakehouse-test/silver/t"

    io_manager = resources["delta_io_manager"]
    assert io_manager.spark is spark
    assert io_manager.lakehouse is lakehouse


def test_s3_lakehouse_paths_and_options() -> None:
    env = {
        **local_env(),
        "DATA_PLATFORM_LAKEHOUSE_MODE": "s3",
        "DATA_PLATFORM_S3_ENDPOINT_URL": "http://localhost:9000",
        "DATA_PLATFORM_S3_BUCKET": "lake",
        "DATA_PLATFORM_S3_BASE_PREFIX": "medallion",
    }
    lakehouse = build_resources_from_env(env)["lakehouse"]
    assert lakehouse.table_path("gold", "orders") == "s3a://lake/medallion/gold/orders"
    options = lakehouse.storage_options()
    assert options["fs.s3a.endpoint"] == "http://localhost:9000"
    assert options["fs.s3a.path.style.access"] == "true"


def test_s3_mode_requires_endpoint() -> None:
    env = {**local_env(), "DATA_PLATFORM_LAKEHOUSE_MODE": "s3"}
    lakehouse = build_resources_from_env(env)["lakehouse"]
    with pytest.raises(ResourceNotConfiguredError):
        lakehouse.storage_options()


def test_trino_disabled_raises_clear_error() -> None:
    trino = build_resources_from_env(local_env())["trino"]
    assert trino.is_configured() is False
    with pytest.raises(ResourceNotConfiguredError):
        trino.connect()


def test_metadata_disabled_is_a_noop() -> None:
    metadata = build_resources_from_env(local_env())["metadata"]
    assert metadata.is_configured() is False
    assert metadata.record("anything", {"a": 1}) is False


def test_invalid_headers_json_raises() -> None:
    with pytest.raises(ValueError):
        build_resources_from_env({**local_env(), "DATA_PLATFORM_REST_HEADERS": "not-json"})
