#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d ".venv" ]]; then
  python -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate || source .venv/Scripts/activate

python -m pip install --upgrade pip
python -m pip install -e .

if [[ ! -f ".env" ]]; then
  cp .env.example .env
fi

mkdir -p data/raw data/interim data/clean

echo "Bootstrap complete."
