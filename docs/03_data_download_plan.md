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
