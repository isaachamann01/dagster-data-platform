"""Shared fixtures for the data platform test suite."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path

import dagster as dg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_DEFINITIONS = REPO_ROOT / "projects/example_pipelines/src/example_pipelines/definitions.py"
TEST_PROJECT_DEFINITIONS = REPO_ROOT / "projects/test_project/src/test_project/definitions.py"


def java_available() -> bool:
    if shutil.which("java"):
        return True
    java_home = os.environ.get("JAVA_HOME")
    return bool(java_home and (Path(java_home) / "bin" / "java").exists())


JAVA_AVAILABLE = java_available()

requires_spark = pytest.mark.skipif(
    not JAVA_AVAILABLE, reason="Java is required for local Spark sessions"
)


def make_resource_env(lakehouse_root: Path, base_url: str = "http://127.0.0.1:9") -> dict[str, str]:
    """Environment mapping for local, isolated resources."""

    return {
        "DATA_PLATFORM_REST_BASE_URL": base_url,
        "DATA_PLATFORM_LAKEHOUSE_MODE": "local",
        "DATA_PLATFORM_LAKEHOUSE_LOCAL_ROOT": str(lakehouse_root),
        "DATA_PLATFORM_SPARK_MASTER": "local[2]",
        "DATA_PLATFORM_SPARK_WAREHOUSE_DIR": str(lakehouse_root / "warehouse"),
        "DATA_PLATFORM_SPARK_CONFIG": (
            '{"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}'
        ),
    }


@pytest.fixture(scope="session")
def lakehouse_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("lakehouse")


@pytest.fixture(scope="session")
def resource_env(lakehouse_root: Path) -> dict[str, str]:
    return make_resource_env(lakehouse_root)


@pytest.fixture(scope="session")
def example_defs() -> dg.Definitions:
    return dg.load_from_defs_folder(path_within_project=EXAMPLE_DEFINITIONS)


@pytest.fixture(scope="session")
def spark_session(resource_env: dict[str, str]) -> Iterator[object]:
    pytest.importorskip("pyspark")
    if not JAVA_AVAILABLE:
        pytest.skip("Java is required for local Spark sessions")
    from data_platform_common.resources import build_resources_from_env

    spark_resource = build_resources_from_env(resource_env)["spark"]
    session = spark_resource.get_session()
    yield session
    spark_resource.stop()


def asset_keys(definition: dg.Definitions) -> set[str]:
    keys: set[str] = set()
    for assets in definition.assets or []:
        if not isinstance(assets, dg.AssetsDefinition):
            continue
        for key in assets.keys:
            keys.add(key.to_user_string())
    return keys


def check_specs(definition: dg.Definitions) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for checks in definition.asset_checks or []:
        for spec in getattr(checks, "check_specs", []) or []:
            specs.append((spec.asset_key.to_user_string(), spec.name))
    return specs
