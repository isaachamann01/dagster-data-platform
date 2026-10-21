"""Shared fixtures for the Spark/Delta smoke tests.

One in-process synthetic-data service, one resource set, one Spark session, and
one loaded Definitions object are reused across every smoke module.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
import uvicorn

import data_contracts
from data_platform_common.resources import build_resources_from_env
from synthetic_api.app import create_app


def _smoke_env(root: Path, base_url: str) -> dict[str, str]:
    return {
        "DATA_PLATFORM_REST_BASE_URL": base_url,
        "DATA_PLATFORM_LAKEHOUSE_MODE": "local",
        "DATA_PLATFORM_LAKEHOUSE_LOCAL_ROOT": str(root),
        "DATA_PLATFORM_SPARK_MASTER": "local[2]",
        "DATA_PLATFORM_SPARK_WAREHOUSE_DIR": str(root / "warehouse"),
        "DATA_PLATFORM_SPARK_CONFIG": (
            '{"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}'
        ),
    }


@pytest.fixture(scope="session")
def service_url() -> Iterator[str]:
    app = create_app(data_contracts.CONTRACTS_DIR)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 30
    while not server.started and time.time() < deadline:
        time.sleep(0.05)
    assert server.started, "synthetic data service failed to start"
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture(scope="session")
def smoke_env(service_url: str, tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    root = tmp_path_factory.mktemp("smoke-lakehouse")
    return _smoke_env(root, service_url)


@pytest.fixture(scope="session")
def resources(smoke_env: dict[str, str]) -> dict[str, Any]:
    return build_resources_from_env(smoke_env)


@pytest.fixture(scope="session")
def spark(resources: dict[str, Any]) -> Any:
    return resources["spark"].get_session()


@pytest.fixture(scope="session")
def defs(example_defs: dg.Definitions, resources: dict[str, Any]) -> dg.Definitions:
    return dg.Definitions.merge(example_defs, dg.Definitions(resources=resources))


@pytest.fixture(scope="session")
def assets_by_key(defs: dg.Definitions) -> dict[str, dg.AssetsDefinition]:
    mapping: dict[str, dg.AssetsDefinition] = {}
    for assets in defs.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            mapping[key.to_user_string()] = assets
    return mapping
