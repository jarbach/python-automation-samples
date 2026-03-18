#!/usr/bin/env bash
# setup.sh — Create venv, install dependencies, and run tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Creating virtual environment..."
python3 -m venv .venv

echo "==> Activating virtual environment..."
# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> Installing requirements..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo "==> Running tests..."
python -m pytest tests/ -v --tb=short

echo ""
echo "==> Setup complete. Activate the environment with:"
echo "    source .venv/bin/activate"
