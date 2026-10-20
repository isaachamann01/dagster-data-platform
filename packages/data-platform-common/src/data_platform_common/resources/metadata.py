"""Optional external metadata integration.

The resource is a no-op when no endpoint is configured, so environments that do
not run a metadata catalog keep working without any special casing.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class MetadataResource(ConfigurableResource):
    endpoint: str | None = None
    timeout_seconds: float = 5.0
    api_key: str | None = None

    def is_configured(self) -> bool:
        return bool(self.endpoint)

    def record(self, event_type: str, payload: dict[str, Any]) -> bool:
        """Send one metadata event. Returns True when it was delivered.

        Failures are logged and reported through the return value rather than
        raising, so an unavailable metadata service never blocks data delivery.
        """

        if not self.is_configured():
            logger.debug("Metadata integration disabled; dropping event %s", event_type)
            return False
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body = {"event_type": event_type, "payload": payload}
        try:
            response = requests.post(
                self.endpoint or "",
                json=body,
                headers=headers,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            return True
        except requests.RequestException as error:
            logger.warning("Metadata event %s failed: %s", event_type, error)
            return False
