"""Spark session resource supporting local and remote (Connect) modes."""

from __future__ import annotations

from typing import Any, Literal

from dagster import ConfigurableResource
from pydantic import PrivateAttr

try:  # pragma: no cover - import guard for environments without pyspark
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover
    SparkSession = Any  # type: ignore[assignment,misc]

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


class SparkResource(ConfigurableResource):
    """Creates (and caches) a Spark session.

    ``local`` mode runs an embedded session with the given master. ``remote``
    mode connects to Spark Connect via ``remote_url``. Delta Lake support is
    enabled by default; the Delta artifacts are resolved by Spark from Maven
    the first time a local session starts (cached by Ivy afterwards).
    """

    mode: Literal["local", "remote"] = "local"
    master: str = "local[2]"
    remote_url: str | None = None
    app_name: str = "data-platform"
    warehouse_dir: str | None = None
    delta_enabled: bool = True
    extra_packages: list[str] = []
    config: dict[str, str] = {}

    _session: SparkSession | None = PrivateAttr(default=None)

    def get_session(self) -> SparkSession:
        if self._session is not None:
            return self._session

        from pyspark.sql import SparkSession as Session

        if self.mode == "remote":
            if not self.remote_url:
                raise ValueError("SparkResource.mode='remote' requires remote_url")
            builder = Session.builder.remote(self.remote_url).appName(self.app_name)
            session = builder.getOrCreate()
        else:
            builder = Session.builder.appName(self.app_name).master(self.master)
            if self.warehouse_dir:
                builder = builder.config("spark.sql.warehouse.dir", self.warehouse_dir)
            if self.delta_enabled:
                from delta import configure_spark_with_delta_pip

                builder = configure_spark_with_delta_pip(
                    builder, extra_packages=list(self.extra_packages or [])
                )
                # Delta 4.x's helper only registers Maven artifacts; the session
                # extensions still have to be configured explicitly.
                builder = builder.config("spark.sql.extensions", DELTA_EXTENSION)
                builder = builder.config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
            elif self.extra_packages:
                builder = builder.config("spark.jars.packages", ",".join(self.extra_packages))
            for key, value in (self.config or {}).items():
                builder = builder.config(key, value)
            session = builder.getOrCreate()

        session.sparkContext.setLogLevel("WARN")
        self._session = session
        return session

    def stop(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
