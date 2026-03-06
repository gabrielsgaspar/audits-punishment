"""I/O helpers for reproducible pipelines."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pandas as pd


def ensure_dir(path: Path) -> Path:
    """Ensure a directory exists and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent(path: Path) -> None:
    """Create parent directory for a file path if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    """Return SHA256 checksum for bytes payload."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """Return SHA256 checksum for an existing file path."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Atomically write bytes to disk via temporary file + replace."""
    ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("wb") as fh:
        fh.write(data)
    os.replace(tmp_path, path)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame as parquet using pyarrow engine."""
    ensure_parent(path)
    df.to_parquet(path, engine="pyarrow", index=False)
