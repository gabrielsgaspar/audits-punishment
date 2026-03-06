"""Build consolidated manifests across ingestion outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from audits_punishment.utils.io import ensure_parent, write_parquet


def upsert_manifest_row(
    manifest_path: Path,
    row_dict: dict[str, Any],
    key_cols: list[str],
) -> pd.DataFrame:
    """Insert or update one manifest row keyed by key_cols and persist.

    The manifest format is inferred from file extension:
    - `.parquet`: parquet via pyarrow
    - anything else: CSV
    """
    row_df = pd.DataFrame([row_dict])

    if manifest_path.exists():
        if manifest_path.suffix.lower() == ".parquet":
            existing = pd.read_parquet(manifest_path)
        else:
            existing = pd.read_csv(manifest_path)
    else:
        existing = pd.DataFrame(columns=row_df.columns)

    for col in row_df.columns:
        if col not in existing.columns:
            existing[col] = pd.NA
    for col in existing.columns:
        if col not in row_df.columns:
            row_df[col] = pd.NA

    existing = existing[row_df.columns]
    merged = pd.concat([existing, row_df], ignore_index=True)

    # Keep the latest row for each key combination.
    merged = merged.drop_duplicates(subset=key_cols, keep="last")

    ensure_parent(manifest_path)
    if manifest_path.suffix.lower() == ".parquet":
        write_parquet(merged, manifest_path)
    else:
        merged.to_csv(manifest_path, index=False)

    return merged


def main() -> None:
    print("TODO: combine module manifests into a unified provenance table")


if __name__ == "__main__":
    main()
