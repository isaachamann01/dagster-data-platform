"""Generic FastAPI synthetic-data service driven by YAML data contracts.

Endpoints
---------
- ``GET  /health``            liveness plus contract count
- ``GET  /contracts``         list contract summaries
- ``GET  /contracts/{name}``  full contract document
- ``POST /generate/{name}``   deterministic rows for one contract (FK-valid)
- ``POST /generate``          batch generation of a contract and its dependencies
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from data_platform_common import __version__
from data_platform_common.contracts.loader import load_contracts_dir
from data_platform_common.contracts.models import DataContract
from data_platform_common.errors import DataPlatformError
from data_platform_common.synthetic.batch import generate_related
from data_platform_common.synthetic.generator import SyntheticGenerator

ENV_CONTRACTS_DIR = "SYNTHETIC_API_CONTRACTS_DIR"


class GenerateOneRequest(BaseModel):
    rows: int | None = Field(default=None, ge=0)
    seed: int = 0
    null_probability: float | None = Field(default=None, ge=0.0, le=1.0)


class GenerateBatchRequest(BaseModel):
    contract: str
    rows: int | dict[str, int] | None = None
    seed: int = 0
    null_probability: float | None = Field(default=None, ge=0.0, le=1.0)


def default_contracts_dir() -> Path:
    """Resolve the contracts directory from the environment or package layout."""

    configured = os.environ.get(ENV_CONTRACTS_DIR, "").strip()
    if configured:
        return Path(configured).expanduser()
    try:  # pragma: no cover - convenience for the example project
        import data_contracts  # noqa: PLC0415

        return Path(data_contracts.CONTRACTS_DIR)
    except ImportError:
        return Path.cwd() / "contracts"


def _contract_summary(contract: DataContract) -> dict[str, Any]:
    return {
        "name": contract.name,
        "table": contract.table_name,
        "layer": contract.layer,
        "version": contract.version,
        "description": contract.description,
        "columns": contract.column_names,
        "primary_key": contract.primary_key,
        "unique_keys": contract.unique_keys,
        "foreign_keys": [
            {"columns": fk.columns, "references": fk.references.contract}
            for fk in contract.foreign_keys
        ],
        "default_rows": contract.synthetic.default_rows,
    }


def create_app(contracts_dir: Path | None = None) -> FastAPI:
    directory = contracts_dir or default_contracts_dir()
    contracts = load_contracts_dir(directory)

    app = FastAPI(
        title="Synthetic Data Service",
        version=__version__,
        description="Deterministic, contract-valid synthetic data generation.",
    )
    app.state.contracts_dir = directory
    app.state.contracts = contracts

    def get_contract(name: str) -> DataContract:
        contract = contracts.get(name)
        if contract is None:
            raise HTTPException(status_code=404, detail=f"Unknown contract '{name}'")
        return contract

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "contracts": len(contracts),
            "contracts_dir": str(directory),
        }

    @app.get("/contracts")
    def list_contracts() -> list[dict[str, Any]]:
        return [
            _contract_summary(contract)
            for contract in sorted(contracts.values(), key=lambda c: c.name)
        ]

    @app.get("/contracts/{name}")
    def get_contract_document(name: str) -> dict[str, Any]:
        return get_contract(name).model_dump(mode="json")

    @app.post("/generate/{name}")
    def generate_one(name: str, request: GenerateOneRequest) -> dict[str, Any]:
        return _generate_one(name, request)

    @app.get("/generate/{name}")
    def generate_one_get(
        name: str,
        rows: int | None = None,
        seed: int = 0,
        null_probability: float | None = None,
    ) -> dict[str, Any]:
        return _generate_one(
            name,
            GenerateOneRequest(rows=rows, seed=seed, null_probability=null_probability),
        )

    def _generate_one(name: str, request: GenerateOneRequest) -> dict[str, Any]:
        contract = get_contract(name)
        rows = request.rows if request.rows is not None else contract.synthetic.default_rows
        try:
            if contract.foreign_keys:
                # Generate the dependency closure so foreign keys are real, but
                # return only the requested dataset.
                batch = generate_related(
                    contracts,
                    name,
                    rows=rows,
                    seed=request.seed,
                    null_probability=request.null_probability,
                )
                dataset = batch.datasets[name]
            else:
                dataset = SyntheticGenerator(
                    contract,
                    seed=request.seed,
                    null_probability=request.null_probability,
                ).generate(rows)
        except DataPlatformError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return {
            "contract": name,
            "seed": request.seed,
            "rows": dataset,
            "row_count": len(dataset),
        }

    @app.post("/generate")
    def generate_batch(request: GenerateBatchRequest) -> dict[str, Any]:
        contract = get_contract(request.contract)
        default_rows = contract.synthetic.default_rows
        rows: int | dict[str, int]
        if request.rows is None:
            rows = default_rows
        elif isinstance(request.rows, int):
            rows = request.rows
        else:
            rows = {**dict.fromkeys(contracts, default_rows), **request.rows}
        try:
            batch = generate_related(
                contracts,
                request.contract,
                rows=rows,
                seed=request.seed,
                null_probability=request.null_probability,
            )
        except DataPlatformError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return {
            "contract": request.contract,
            "seed": batch.seed,
            "order": batch.order,
            "rows": batch.rows,
            "datasets": batch.datasets,
        }

    return app


app = create_app()
