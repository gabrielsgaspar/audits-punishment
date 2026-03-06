# Data Download Plan

## Objectives
- Implement deterministic ingest modules with reproducible outputs.
- Preserve source snapshots and metadata for every downloaded artifact.
- Separate raw source capture from parsed or cleaned datasets.

## Global conventions
- Raw outputs go under `data/raw/<source_name>/`.
- Each module writes a `manifest.parquet` and `manifest.csv` in its source folder.
- Each downloaded file stores SHA256 checksum and byte size.
- Module runs are append-only with `run_id` and UTC timestamp.

## Module: `ingest/cgu_lottery.py`
- Step 1: fetch the CGU page with municipality lottery lists.
- Step 2: parse tables or linked files for rounds and municipalities.
- Step 3: normalize municipality and UF values.
- Step 4: write `data/raw/cgu_lottery/rounds.parquet`.
- Step 5: append source snapshot metadata to manifest.
- Expected outputs:
- `data/raw/cgu_lottery/source_snapshot.html`
- `data/raw/cgu_lottery/rounds.parquet`
- `data/raw/cgu_lottery/manifest.parquet`

## Module: `ingest/cgu_audits.py`
- Step 1: map municipality-round pairs to report document URLs.
- Step 2: download PDFs with retry and polite user-agent headers.
- Step 3: write metadata table for report IDs and URL origins.
- Step 4: compute checksums and file sizes.
- Expected outputs:
- `data/raw/cgu_audits/pdfs/<report_id>.pdf`
- `data/raw/cgu_audits/reports.parquet`
- `data/raw/cgu_audits/manifest.parquet`

## Module: `ingest/cnj_datajud.py`
- Step 1: define endpoint and query windows (initially pilot range).
- Step 2: request process metadata pages with pagination control.
- Step 3: persist raw JSON responses per request batch.
- Step 4: flatten into canonical process and movement tables.
- Expected outputs:
- `data/raw/cnj_datajud/json/<request_id>.json`
- `data/raw/cnj_datajud/processes.parquet`
- `data/raw/cnj_datajud/movements.parquet`
- `data/raw/cnj_datajud/manifest.parquet`

## Module: `ingest/stj_ckan.py`
- Step 1: query CKAN package metadata for decision datasets.
- Step 2: resolve resource URLs and file formats.
- Step 3: download files with versioned filenames.
- Step 4: build a document catalog with publication metadata.
- Expected outputs:
- `data/raw/stj_ckan/resources/<resource_name>`
- `data/raw/stj_ckan/catalog.parquet`
- `data/raw/stj_ckan/manifest.parquet`

## Checksums and manifests
- Required fields in manifest:
- `run_id`, `module_name`, `source_url`, `retrieved_at_utc`.
- `local_path`, `sha256`, `content_length`, `http_status`.
- `parser_version`, `notes`.
- Manifests must be stable and append-only.

## Failure handling
- Retry transient network failures with exponential backoff.
- Persist failed URL attempts with error codes.
- Never overwrite existing raw files unless checksum differs and versioning is explicit.

## Completion criteria
- A full ingest run produces non-empty manifests for all four source modules.
- At least one pilot artifact exists per source in `data/raw`.
- End-of-run summary prints counts of successes, failures, and duplicates.

## CGU lottery ingestion details
- Raw outputs:
- `data/raw/cgu/lottery_lists/index_latest.html`
- `data/raw/cgu/lottery_lists/index_<timestamp>.html`
- `data/raw/cgu/lottery_lists/<round_file>.html|pdf|csv|doc*`
- `data/raw/cgu/lottery_lists/<round_file>_resource_*`
- Interim outputs:
- `data/interim/cgu/lottery_lists/lottery_rounds.parquet`
- `data/interim/cgu/lottery_lists/lottery_municipalities.parquet`
- `data/interim/cgu/lottery_lists/lottery_rounds_summary.parquet`
- Manifest:
- `data/interim/manifests/cgu_lottery_manifest.parquet`
- Report files:
- `data/interim/reports/cgu_lottery/run_report_<timestamp>.json`
- `data/interim/reports/cgu_lottery/run_report_latest.json`
- `data/interim/reports/cgu_lottery/latest.json`

### Manifest fields used by CGU lottery ingestor
- `source_url`, `final_url`, `local_path`
- `checksum_sha256`, `size_bytes`, `status_code`, `content_type`
- `downloaded_at_utc`, `download_status`
- `role`, `parse_status`, `parse_reason`, `round_id`

### Run report fields (key)
- Top level: `timestamp_utc`, `success`, `dry_run`
- `metrics`: rounds discovered, parsed, municipality count, failed rounds, download failures
- `health_checks`: rounds non-empty, download-failure ratio check, low-municipality warning flag
- `timing_seconds`: `download_index`, `parse_index`, `download_rounds`, `parse_rounds`, `write_outputs`
- `warnings`, `failed_rounds`, `artifacts`

## IBGE crosswalk module details
- Reference source used by the module:
- Official IBGE Localidades API (municipios endpoint):
- `https://servicodados.ibge.gov.br/api/v1/localidades/municipios`
- Caching behavior:
- Raw response is cached at `data/raw/reference/ibge_municipios.json`.
- Re-download only when `--force-download` is set.
- Defensive parsing note:
- IBGE nested fields can be null in some records.
- Parser is defensive and only relies on municipality `id`, `nome`, and UF `sigla` when safely available.
- Records missing UF are skipped and counted in run metrics.

### Normalized reference output
- `data/clean/reference/ibge_municipios.parquet`
- Fields:
- `ibge_municipality_code` (7-digit string)
- `municipality_name_official`
- `uf`
- `municipality_name_norm`
- `source_url`
- `pulled_at_utc`

### Crosswalk application output
- Input: `data/interim/cgu/lottery_lists/lottery_municipalities.parquet`
- Output: `data/clean/cgu/lottery_municipalities_with_ibge.parquet`
- Added fields:
- `ibge_municipality_code`
- `match_method`
- `match_score`
- `matched_name_official`
- `match_notes`

### Manual review workflow
- Candidate suggestions are written to:
- `data/interim/reports/ibge_crosswalk/manual_review_candidates.csv`
- Contains top 5 same-UF candidates for unresolved rows (`manual_needed` or `unmatched`).
- Columns:
- `municipality_name_clean`, `uf`, `suggested_ibge_code`, `suggested_name_official`, `score`, `rank`
- Run summary is written to:
- `data/interim/reports/ibge_crosswalk/<timestamp>_crosswalk_report.json`
- Manifest for downloads and outputs:
- `data/interim/manifests/ibge_crosswalk_manifest.parquet`

## CGU Audit Report Discovery (MVP)
- Goal:
- Discover and index likely CGU audit report pages/PDF URLs for sampled municipalities.
- Use strategy-based probing so portal changes are observable and debuggable.

### Strategies
- `eaud_api` strategy (API discovery from JS bundles):
- Starts from relatorios app pages and downloads referenced JS bundles such as
- `/static/relatorios/js/eaud-relatorios.min.js`.
- Extracts candidate `/api/...` endpoints from JS text and probes GET/POST search patterns.
- Caches first working endpoint at:
- `data/interim/cgu/audits/eaud_api_endpoint.json`
- This is needed because the public relatorios pages are JS-driven and often have no usable HTML form action.
- `auditoria` strategy:
- Starts at `https://auditoria.cgu.gov.br/`.
- Probes landing and query-style URLs for municipality/UF terms.
- Parses result/detail pages for report links and direct PDF links.
- `auto`:
- Runs `eaud_api` first, then `auditoria`, and merges candidates.

### Outputs
- Raw HTML snapshots:
- `data/raw/cgu/audits/html/<strategy>/<ibge>_<slug>_<urlhash>_<timestamp>.html`
- PDF probe downloads (high-confidence only):
- `data/raw/cgu/audits/pdfs/<ibge>/<candidate_id>_<timestamp>.pdf`
- Report index:
- `data/interim/cgu/audits/audit_reports_index.parquet`
- Run report:
- `data/interim/reports/cgu_audits/<timestamp>_report.json`
- Manifest:
- `data/interim/manifests/cgu_audits_manifest.parquet`

### Debug workflow with snapshots
- Inspect report breakdown by strategy and top failure reasons.
- Open example landing URLs from report for manual validation.
- Compare HTML snapshots across runs to diagnose selector drift.
- Use manifest checksums and paths to trace exactly which pages were visited.

## CGU Open Data Auditorias.csv ingestion
- Module:
- `python -m audits_punishment.ingest.cgu_audits_open_data --log-level INFO`
- Primary source:
- `https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/auditorias`
- `https://dadosabertos-download.cgu.gov.br/Auditorias/Auditorias.csv`
- Why this path:
- It is an official CGU open-data table with task-level metadata.
- It avoids fragile scraping of JS-only search pages for baseline indexing.

### Steps
- Download/cache `Auditorias.csv` with retries and user-agent headers.
- Parse CSV with encoding fallback (`utf-8-sig` then `latin-1`).
- Normalize columns to snake_case and parse `data_de_publicacao` with `dayfirst=True`.
- Optionally filter to Sorteio/FEF rows (`--only-sorteio`, default true).
- Match `(municipio, uf)` to IBGE municipality codes using existing crosswalk logic.
- Build linked municipality-audit outputs using IBGE exact join to lottery municipalities.
- Optionally probe `https://eaud.cgu.gov.br/relatorio/<task_id>` pages for PDF URLs.

### Outputs
- Raw:
- `data/raw/cgu/audits_open_data/Auditorias.csv`
- Optional detail snapshots:
- `data/raw/cgu/audits_open_data/html/<task_id>.html`
- Optional PDFs:
- `data/raw/cgu/audits_open_data/pdfs/<task_id>/<task_id>.pdf`
- Clean:
- `data/clean/cgu/audits_open_data/auditorias_with_ibge.parquet`
- `data/clean/cgu/audit_events.parquet`
- `data/clean/cgu/audit_events_long.parquet`
- Reports:
- `data/interim/reports/cgu_audits_open_data/<timestamp>_report.json`
- `data/interim/reports/cgu_audits_open_data/latest.json`
- `data/interim/reports/cgu_audits_open_data/manual_review_candidates.csv`
- Manifest:
- `data/interim/manifests/cgu_audits_open_data_manifest.parquet`

### Correctness gates
- Fail if `Auditorias.csv` has fewer than 1000 rows.
- Fail if IBGE mapping coverage is below 0.98.
- Always report:
- share of lottery municipalities with at least one linked audit.
- publication year distribution for linked audits.
