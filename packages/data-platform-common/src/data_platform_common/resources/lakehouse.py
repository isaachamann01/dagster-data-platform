"""S3-compatible (or local) lakehouse storage resource."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dagster import ConfigurableResource

from data_platform_common.errors import ResourceNotConfiguredError

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


class LakehouseResource(ConfigurableResource):
    """Resolves Delta table paths and configures Spark for S3-compatible storage.

    ``local`` mode writes to the local filesystem (useful for tests and single
    machine development). ``s3`` mode targets any S3-compatible endpoint such as
    MinIO; Spark needs the Hadoop AWS packages, which can be supplied through
    ``SparkResource.extra_packages``.
    """

    mode: Literal["local", "s3"] = "local"
    local_root: str = "/tmp/dagster_pipelines/lakehouse"
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    bucket: str = "lakehouse"
    base_prefix: str = "medallion"
    region: str = "us-east-1"
    use_ssl: bool = False

    def table_path(self, layer: str, table: str) -> str:
        """The storage location for one medallion table."""

        if self.mode == "local":
            root = Path(self.local_root).expanduser().resolve()
            return (root / layer / table).as_uri()
        prefix = f"{self.base_prefix.strip('/')}/" if self.base_prefix else ""
        return f"s3a://{self.bucket}/{prefix}{layer}/{table}"

    def storage_options(self) -> dict[str, str]:
        """Hadoop options required by PySpark writes in ``s3`` mode."""

        if self.mode != "s3":
            return {}
        if not self.endpoint_url:
            raise ResourceNotConfiguredError("s3 lakehouse mode requires endpoint_url")
        options = {
            "fs.s3a.endpoint": self.endpoint_url,
            "fs.s3a.endpoint.region": self.region,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": "true" if self.use_ssl else "false",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
        if self.access_key_id and self.secret_access_key:
            options.update(
                {
                    "fs.s3a.access.key": self.access_key_id,
                    "fs.s3a.secret.key": self.secret_access_key,
                }
            )
        return options

    def configure_spark(self, spark: SparkSession) -> None:
        """Apply storage options to an existing Spark session."""

        options = self.storage_options()
        if not options:
            return
        hadoop = spark.sparkContext._jsc.hadoopConfiguration()
        for key, value in options.items():
            hadoop.set(key, value)

    def ensure_bucket(self) -> None:
        """Create the bucket when running against S3-compatible storage."""

        if self.mode != "s3":
            return
        import boto3  # noqa: PLC0415 - optional dependency

        client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            use_ssl=self.use_ssl,
        )
        try:
            client.head_bucket(Bucket=self.bucket)
        except client.exceptions.ClientError:
            client.create_bucket(Bucket=self.bucket)

    def metadata(self) -> dict[str, Any]:
        return {
            "lakehouse/mode": self.mode,
            "lakehouse/root": self.local_root if self.mode == "local" else f"s3://{self.bucket}",
        }
