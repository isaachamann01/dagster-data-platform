"""Docker-backed MinIO/S3 integration test.

Skipped unless ``RUN_DOCKER_TESTS=1`` and a MinIO endpoint is reachable, so the
default `pytest -m "not docker and not slow"` run stays fast and offline.

Run it with:

    docker compose up -d minio minio-init
    RUN_DOCKER_TESTS=1 pytest tests/docker -m docker -q
"""

from __future__ import annotations

import os
import uuid

import pytest

from data_platform_common.io.delta import WriteMode, write_dataframe
from data_platform_common.resources.lakehouse import LakehouseResource
from data_platform_common.resources.spark import SparkResource

pytestmark = [pytest.mark.docker, pytest.mark.slow]

ENDPOINT = os.environ.get("DATA_PLATFORM_S3_ENDPOINT_URL", "http://localhost:9000")
BUCKET = os.environ.get("DATA_PLATFORM_S3_BUCKET", "lakehouse")
# Matches the Hadoop version bundled with Spark 4.2.0 (3.5.0).
HADOOP_PACKAGES = ["org.apache.hadoop:hadoop-aws:3.5.0"]


def _docker_enabled() -> bool:
    return os.environ.get("RUN_DOCKER_TESTS") == "1"


@pytest.fixture(scope="module")
def lakehouse() -> LakehouseResource:
    return LakehouseResource(
        mode="s3",
        endpoint_url=ENDPOINT,
        access_key_id=os.environ.get("DATA_PLATFORM_S3_ACCESS_KEY_ID", "minioadmin"),
        secret_access_key=os.environ.get("DATA_PLATFORM_S3_SECRET_ACCESS_KEY", "minioadmin"),
        bucket=BUCKET,
        base_prefix="pytest",
        use_ssl=False,
    )


@pytest.mark.skipif(not _docker_enabled(), reason="set RUN_DOCKER_TESTS=1 to run")
def test_minio_delta_merge_roundtrip(lakehouse: LakehouseResource) -> None:
    pytest.importorskip("boto3")
    lakehouse.ensure_bucket()

    spark_resource = SparkResource(
        master="local[2]",
        extra_packages=HADOOP_PACKAGES,
        config={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "2"},
    )
    spark = spark_resource.get_session()
    lakehouse.configure_spark(spark)

    table = f"roundtrip_{uuid.uuid4().hex[:8]}"
    path = lakehouse.table_path("pytest", table)
    try:
        first = spark.createDataFrame([("a", 1), ("b", 2)], "id string, value long")
        write_dataframe(first, path=path, mode=WriteMode.MERGE, merge_keys=["id"])

        second = spark.createDataFrame([("a", 10), ("c", 3)], "id string, value long")
        write_dataframe(second, path=path, mode=WriteMode.MERGE, merge_keys=["id"])

        rows = {row["id"]: row["value"] for row in spark.read.format("delta").load(path).collect()}
        assert rows == {"a": 10, "b": 2, "c": 3}
    finally:
        spark_resource.stop()
