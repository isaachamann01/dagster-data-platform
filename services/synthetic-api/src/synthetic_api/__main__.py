"""Run the synthetic-data service with ``python -m synthetic_api``."""

from __future__ import annotations

import os

import uvicorn

from synthetic_api.app import app


def main() -> None:
    host = os.environ.get("SYNTHETIC_API_HOST", "0.0.0.0")
    port = int(os.environ.get("SYNTHETIC_API_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
