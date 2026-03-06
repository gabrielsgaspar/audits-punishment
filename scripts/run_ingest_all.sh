#!/usr/bin/env bash
set -euo pipefail

python -m audits_punishment.ingest.cgu_lottery
python -m audits_punishment.ingest.cgu_audits
python -m audits_punishment.ingest.cnj_datajud
python -m audits_punishment.ingest.stj_ckan
