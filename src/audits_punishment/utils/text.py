"""Text helpers."""

from __future__ import annotations

import re
import unicodedata


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace for stable text processing."""
    return re.sub(r"\s+", " ", text or "").strip()


def strip_accents(text: str) -> str:
    """Remove accents/diacritics while preserving base characters."""
    normed = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normed if not unicodedata.combining(ch))


def slugify(text: str, max_len: int = 80) -> str:
    """Create ASCII-ish slug for stable filenames."""
    cleaned = strip_accents(text or "").lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return (cleaned or "item")[:max_len]


def normalize_municipality_query(text: str) -> str:
    """Normalize municipality text for search/query terms."""
    value = strip_accents(text or "").upper().strip()
    value = re.sub(r"^[\s\-\–\—\•\*]+", "", value)
    value = value.replace("-", " ").replace("/", " ")
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    return normalize_whitespace(value)
