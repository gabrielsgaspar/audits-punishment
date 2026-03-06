"""Text helpers placeholder."""

from __future__ import annotations

import re


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace for stable text processing."""
    return re.sub(r"\s+", " ", text).strip()
