"""Optional Trino query resource that stays disabled when unconfigured."""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from data_platform_common.errors import ResourceNotConfiguredError


class TrinoResource(ConfigurableResource):
    """Thin Trino client used by optional query assets.

    When ``enabled`` is false (the default) every method raises a clear
    :class:`ResourceNotConfiguredError`, so the resource can be wired into
    definitions without requiring a running Trino cluster.
    """

    enabled: bool = False
    host: str = "localhost"
    port: int = 8080
    user: str = "opencode"
    catalog: str = "delta"
    schema_: str = "default"
    http_scheme: str = "http"
    timeout_seconds: float = 30.0

    _connection: Any = PrivateAttr(default=None)

    def is_configured(self) -> bool:
        return self.enabled and bool(self.host)

    def connect(self) -> Any:
        if not self.is_configured():
            raise ResourceNotConfiguredError(
                "Trino is not configured; set DATA_PLATFORM_TRINO_ENABLED=true and the "
                "connection environment variables to use it"
            )
        if self._connection is None:
            import trino.dbapi  # noqa: PLC0415 - optional dependency

            self._connection = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema_,
                http_scheme=self.http_scheme,
            )
        return self._connection

    def query(self, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        cursor = self.connect().cursor()
        cursor.execute(sql, params or [])
        return cursor.fetchall()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
