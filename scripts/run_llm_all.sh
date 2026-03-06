#!/usr/bin/env bash
set -euo pipefail

python -m audits_punishment.llm.extract_audit_finding_cards
python -m audits_punishment.llm.score_matches
python -m audits_punishment.llm.extract_decision_outcomes
