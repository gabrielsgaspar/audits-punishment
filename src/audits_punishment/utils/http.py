"""HTTP helpers placeholder."""

from __future__ import annotations

from typing import Any


def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    """TODO: implement robust HTTP fetch with retries and structured errors."""
    raise NotImplementedError(f"TODO: fetch JSON from {url} with retry and manifest logging")
