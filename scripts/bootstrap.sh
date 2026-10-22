#!/usr/bin/env bash
# Create the local virtualenv and install every workspace package in editable mode.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON="${PYTHON:-python3.12}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  PYTHON=python3
fi

"$PYTHON" -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install \
  -e "packages/data-platform-common[dev,s3,trino]" \
  -e packages/data-platform-datavault \
  -e services/synthetic-api \
  -e projects/example_pipelines \
  -e projects/test_project

echo
echo "Environment ready."
echo "  source .venv/bin/activate"
echo "  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64  # if Java is not on PATH"
echo "  pytest -m 'not docker and not slow'"
