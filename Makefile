PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin

.PHONY: bootstrap format check lint types ty test smoke api dagster up down

bootstrap:
	bash scripts/bootstrap.sh

format:
	$(BIN)/ruff format .

check: format
	$(BIN)/ruff format --check .
	$(BIN)/ruff check .
	$(BIN)/mypy
	$(BIN)/ty check

types:
	$(BIN)/mypy

ty:
	$(BIN)/ty check

test:
	$(BIN)/pytest -m "not docker and not slow"

smoke:
	$(BIN)/pytest tests/smoke -m smoke

api:
	$(BIN)/uvicorn synthetic_api.app:app --host 0.0.0.0 --port 8000

dagster:
	$(BIN)/dagster dev -m example_pipelines.definitions

up:
	docker compose up --build -d

down:
	docker compose down -v
