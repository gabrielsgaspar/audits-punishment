"""HTTP helpers with retries for robust ingestion."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


_TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


class FetchUrlError(RuntimeError):
    """Raised when a URL could not be fetched successfully."""

    def __init__(self, message: str, metadata: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.metadata = metadata or {}


@retry(
    retry=retry_if_exception_type((requests.RequestException, FetchUrlError)),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_url(
    url: str,
    *,
    method: str = "GET",
    timeout: int = 30,
    headers: dict[str, str] | None = None,
    sleep_seconds: float = 0.0,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: Any = None,
) -> tuple[bytes, dict[str, Any]]:
    """Fetch bytes from URL and return response metadata.

    Retries transient request errors and retryable status codes.
    """
    req_headers = dict(headers or {})
    if "User-Agent" not in req_headers:
        req_headers["User-Agent"] = os.getenv("USER_AGENT", "audits-punishment-research")
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    start = time.perf_counter()
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            timeout=timeout,
            headers=req_headers,
            allow_redirects=True,
            params=params,
            json=json_body,
            data=data,
        )
    except requests.RequestException as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        raise FetchUrlError(
            str(exc),
            metadata={
                "final_url": url,
                "status_code": -1,
                "content_type": "",
                "headers": {},
                "error": repr(exc),
                "elapsed_ms": elapsed_ms,
            },
        ) from exc

    if response.status_code in _TRANSIENT_STATUS_CODES:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        raise FetchUrlError(
            f"Transient HTTP status {response.status_code} for {url}",
            metadata={
                "final_url": response.url,
                "status_code": response.status_code,
                "content_type": response.headers.get("Content-Type", ""),
                "headers": dict(response.headers),
                "error": "",
                "elapsed_ms": elapsed_ms,
            },
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        raise FetchUrlError(
            str(exc),
            metadata={
                "final_url": response.url,
                "status_code": response.status_code,
                "content_type": response.headers.get("Content-Type", ""),
                "headers": dict(response.headers),
                "error": repr(exc),
                "elapsed_ms": elapsed_ms,
            },
        ) from exc

    metadata: dict[str, Any] = {
        "final_url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type", ""),
        "headers": dict(response.headers),
        "error": "",
        "elapsed_ms": (time.perf_counter() - start) * 1000.0,
    }
    return response.content, metadata


def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch JSON with robust retries and return a decoded dict."""
    content, _ = fetch_url(url, headers=headers)
    return json.loads(content.decode("utf-8"))


def filename_from_content_disposition(content_disposition: str | None) -> str | None:
    """Extract filename from Content-Disposition header when available."""
    if not content_disposition:
        return None
    match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)"?', content_disposition, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip().strip("\"'")
