# Audits Punishment

`audits-punishment` is a Python project to study whether public audit findings lead to real legal punishment.

## What this project is
- We build an end-to-end data and LLM pipeline linking:
- Municipal audits (with corruption and irregularity findings),
- Judicial case records and decision texts,
- Structured outcomes that indicate enforcement intensity.
- Core question: do audits increase punishment, and under what conditions?

## Why it matters
- Anti-corruption policy often assumes detection leads to sanctions.
- Existing evidence is stronger on detection and electoral accountability than on downstream legal enforcement.
- If enforcement is weak or delayed, deterrence may be limited even when audits reveal misconduct.

## What is new
- We explicitly build on existing audit corruption measures, including recent LLM-assisted audit-content measurement.
- Rather than competing with those measures, we use them as upstream inputs and focus on the enforcement margin.
- The novelty is systematic linkage from audit findings to judicial outcomes, with uncertainty-aware matching.

## High-level pipeline
1. Ingest public data from CGU, CNJ DataJud, and STJ open data.
2. Parse and chunk audit and decision texts into model-ready units.
3. Run LLM extraction into strict JSON schemas with evidence spans.
4. Link audit findings to judicial decisions using deterministic candidates + LLM scoring.
5. Build enforcement indices and analysis-ready panels for econometric work.

## Repository structure
- `data/`: raw, interim, and clean datasets.
- `docs/`: research design, ingestion plans, linking logic, and validation protocol.
- `src/audits_punishment/`: reusable package code for ingest, parse, LLM, and build steps.
- `scripts/`: convenience shell scripts to bootstrap and run staged pipelines.
- `tests/`: basic tests for paths and schemas.

## Quickstart
1. Create a virtual environment:
   - `python -m venv .venv`
   - `source .venv/bin/activate` (Linux/macOS) or `.venv\Scripts\Activate.ps1` (PowerShell)
2. Install in editable mode:
   - `pip install -e .`
3. Copy environment template:
   - `cp .env.example .env` (Linux/macOS) or `Copy-Item .env.example .env` (PowerShell)
4. Run bootstrap and staged scripts:
   - `bash scripts/bootstrap.sh`
   - `bash scripts/run_ingest_all.sh`
   - `bash scripts/run_parse_all.sh`
   - `bash scripts/run_llm_all.sh`

## Data ethics and handling
- The project uses public administrative and judicial sources.
- Even with public data, we apply careful handling:
- Preserve provenance and checksums,
- Minimize sensitive processing,
- Keep extraction logic transparent and auditable.
- Outputs should be interpreted as measurement products with documented uncertainty.
