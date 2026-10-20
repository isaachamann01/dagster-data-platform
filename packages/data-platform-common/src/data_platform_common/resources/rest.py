"""REST client resource with timeouts and bounded retries."""

from __future__ import annotations

from typing import Any, Literal

import requests
from dagster import ConfigurableResource
from pydantic import PrivateAttr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


class RestResource(ConfigurableResource):
    """A small, generic JSON REST client for pipeline source retrieval."""

    base_url: str
    timeout_seconds: float = 10.0
    max_retries: int = 3
    backoff_seconds: float = 0.5
    headers: dict[str, str] = {}
    api_key_header: str | None = None
    api_key: str | None = None
    verify_ssl: bool = True

    _session: requests.Session | None = PrivateAttr(default=None)

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            backoff_factor=self.backoff_seconds,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.headers)
        if self.api_key and self.api_key_header:
            session.headers[self.api_key_header] = self.api_key
        return session

    def session(self) -> requests.Session:
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def url_for(self, route: str) -> str:
        return f"{self.base_url.rstrip('/')}/{route.lstrip('/')}"

    def fetch_text(
        self,
        route: str,
        *,
        params: dict[str, Any] | None = None,
        method: Literal["GET", "POST"] = "GET",
        json_body: dict[str, Any] | None = None,
    ) -> str:
        response = self.session().request(
            method,
            self.url_for(route),
            params={key: value for key, value in (params or {}).items() if value is not None},
            json=json_body,
            timeout=self.timeout_seconds,
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        return response.text

    def fetch_json(
        self,
        route: str,
        *,
        params: dict[str, Any] | None = None,
        method: Literal["GET", "POST"] = "GET",
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        response = self.session().request(
            method,
            self.url_for(route),
            params={key: value for key, value in (params or {}).items() if value is not None},
            json=json_body,
            timeout=self.timeout_seconds,
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
