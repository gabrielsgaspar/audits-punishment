"""Path resolution helpers for project directories."""

from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    """Return repository root based on this file location."""
    return Path(__file__).resolve().parents[2]


def project_data_dir() -> Path:
    """Return data directory, honoring PROJECT_DATA_DIR if defined."""
    configured = os.getenv("PROJECT_DATA_DIR", "data")
    data_dir = Path(configured)
    if not data_dir.is_absolute():
        data_dir = repo_root() / data_dir
    return data_dir.resolve()


def raw_dir() -> Path:
    return project_data_dir() / "raw"


def interim_dir() -> Path:
    return project_data_dir() / "interim"


def clean_dir() -> Path:
    return project_data_dir() / "clean"


def docs_dir() -> Path:
    return repo_root() / "docs"


def ensure_data_dirs() -> None:
    """Create expected data directories if they are missing."""
    for directory in (raw_dir(), interim_dir(), clean_dir()):
        directory.mkdir(parents=True, exist_ok=True)
