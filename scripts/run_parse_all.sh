#!/usr/bin/env bash
set -euo pipefail

python -m audits_punishment.parse.chunk_audits
python -m audits_punishment.parse.chunk_decisions
