"""Ingest CGU lottery rounds and municipality lists."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urldefrag, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from audits_punishment.build.build_manifests import upsert_manifest_row
from audits_punishment.config import get_settings
from audits_punishment.logging import setup_logging
from audits_punishment.paths import interim_dir, raw_dir
from audits_punishment.utils.http import fetch_url
from audits_punishment.utils.io import atomic_write_bytes, ensure_dir, sha256_bytes, sha256_file, write_parquet

INDEX_URL = (
    "https://www.gov.br/cgu/pt-br/assuntos/auditoria-e-fiscalizacao/"
    "programa-de-fiscalizacao-em-entes-federativos/edicoes-anteriores/municipios"
)
UF_CODES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}
ROUND_HINT_RE = re.compile(
    r"(sorteio|sorteios|sorteio publico|sorteio p[úu]blico|\b\d{1,3}\s*[ºo]\s*sorteio)",
    re.IGNORECASE,
)
MUNI_UF_RE = re.compile(r"(?P<mun>[A-Za-zÀ-ÿ0-9\s'`´\-\.]+?)\s*(?:/|-)\s*(?P<uf>[A-Z]{2})\b")
RESOURCE_EXTENSIONS = {".pdf", ".doc", ".docx", ".csv", ".xls", ".xlsx"}


@dataclass(slots=True)
class DownloadResult:
    url: str
    local_path: Path
    final_url: str
    status_code: int
    content_type: str
    checksum_sha256: str
    size_bytes: int
    downloaded_at_utc: str
    status: str


@dataclass(slots=True)
class RoundProcessResult:
    parse_status: str
    parse_reason: str
    municipalities: list[dict[str, str]]
    linked_resource_urls: list[str]
    primary_resource_type: str
    error_message: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slugify(text: str, max_len: int = 80) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")
    return (value or "item")[:max_len]


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _clean_municipality_name(name: str) -> str:
    return re.sub(r"[\s,;:\-.]+$", "", _clean_text(name))


def _round_id(round_label: str, url: str) -> str:
    return f"round_{sha256_bytes(f'{round_label}|{url}'.encode('utf-8'))[:12]}"


def _resource_type_from_extension(ext: str) -> str:
    if ext in {".html", ".htm"}:
        return "html"
    if ext == ".pdf":
        return "pdf"
    return "unknown"


def _guess_extension(content_type: str, url: str) -> str:
    path_ext = Path(urlparse(url).path).suffix.lower()
    if path_ext in {".html", ".htm", ".pdf", ".csv", ".xls", ".xlsx", ".doc", ".docx"}:
        return path_ext
    ctype = content_type.lower()
    if "html" in ctype:
        return ".html"
    if "pdf" in ctype:
        return ".pdf"
    if "csv" in ctype:
        return ".csv"
    return ".bin"


def _normalize_url(base_url: str, href: str) -> str:
    full = urljoin(base_url, href.strip())
    clean, _ = urldefrag(full)
    return clean.strip()


def _looks_like_round_link(anchor_text: str, href: str) -> bool:
    return bool(ROUND_HINT_RE.search(f"{anchor_text} {href}".lower()))


def _candidate_round_anchors(soup: BeautifulSoup) -> list:
    return list(soup.select(".accordion a[href], .card a[href], .cards a[href]")) + list(
        soup.find_all("a", href=True)
    )


def extract_round_links(index_html: str, source_page_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(index_html, "lxml")
    discovered_at = _utc_now_iso()
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for anchor in _candidate_round_anchors(soup):
        href = _clean_text(anchor.get("href", ""))
        text = _clean_text(anchor.get_text(" ", strip=True))
        if not href or not _looks_like_round_link(text, href):
            continue
        url = _normalize_url(source_page_url, href)
        if url in seen:
            continue
        seen.add(url)
        out.append(
            {
                "round_label": text or "round_link",
                "url": url,
                "source_page_url": source_page_url,
                "discovered_at": discovered_at,
            }
        )
    return out

def _parse_table_rows(soup: BeautifulSoup) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            text_cells = [_clean_text(c.get_text(" ", strip=True)) for c in cells]
            for cell in text_cells:
                match = MUNI_UF_RE.search(cell)
                if not match:
                    continue
                uf = match.group("uf").upper()
                if uf not in UF_CODES:
                    continue
                rows.append(
                    {
                        "municipality_name_raw": match.group("mun"),
                        "uf": uf,
                        "source_snippet": " | ".join(text_cells)[:400],
                        "parse_method": "table",
                        "confidence": "high",
                    }
                )
    return rows


def _parse_list_items(soup: BeautifulSoup) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for li in soup.find_all("li"):
        snippet = _clean_text(li.get_text(" ", strip=True))
        match = MUNI_UF_RE.search(snippet) if snippet else None
        if not match:
            continue
        uf = match.group("uf").upper()
        if uf not in UF_CODES:
            continue
        rows.append(
            {
                "municipality_name_raw": match.group("mun"),
                "uf": uf,
                "source_snippet": snippet[:400],
                "parse_method": "li",
                "confidence": "high",
            }
        )
    return rows


def _parse_text_regex(soup: BeautifulSoup) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for node in soup.find_all(["p", "div", "span"]):
        snippet = _clean_text(node.get_text(" ", strip=True))
        if not snippet:
            continue
        for match in MUNI_UF_RE.finditer(snippet):
            uf = match.group("uf").upper()
            if uf not in UF_CODES:
                continue
            rows.append(
                {
                    "municipality_name_raw": match.group("mun"),
                    "uf": uf,
                    "source_snippet": snippet[:400],
                    "parse_method": "regex",
                    "confidence": "medium",
                }
            )
    return rows


def extract_municipalities_from_html(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows = _parse_table_rows(soup) + _parse_list_items(soup) + _parse_text_regex(soup)
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        clean_name = _clean_municipality_name(row["municipality_name_raw"])
        if not clean_name:
            continue
        row["municipality_name_clean"] = clean_name
        deduped[(clean_name.lower(), row["uf"], row["parse_method"])] = row
    return list(deduped.values())


def extract_resource_links(html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        url = _normalize_url(page_url, anchor.get("href", ""))
        ext = Path(urlparse(url).path).suffix.lower()
        if ext not in RESOURCE_EXTENSIONS or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def process_round_html(round_url: str, html_text: str) -> RoundProcessResult:
    municipalities = extract_municipalities_from_html(html_text)
    resources = extract_resource_links(html_text, round_url)
    if municipalities:
        return RoundProcessResult("parsed", "", municipalities, resources, "html", "")
    if resources:
        first_ext = Path(urlparse(resources[0]).path).suffix.lower()
        return RoundProcessResult(
            "needs_pdf_parse",
            "round_links_to_external_resource_no_html_list",
            [],
            resources,
            _resource_type_from_extension(first_ext),
            "",
        )
    return RoundProcessResult("no_municipalities_extracted", "html_parsed_but_no_confident_matches", [], [], "html", "")


def _download_with_cache(
    url: str,
    destination: Path,
    *,
    headers: dict[str, str],
    force: bool,
) -> DownloadResult:
    now_iso = _utc_now_iso()
    if destination.exists() and not force:
        return DownloadResult(
            url=url,
            local_path=destination,
            final_url=url,
            status_code=200,
            content_type="cached/local",
            checksum_sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            downloaded_at_utc=now_iso,
            status="cached",
        )

    content, meta = fetch_url(url, headers=headers)
    atomic_write_bytes(destination, content)
    return DownloadResult(
        url=url,
        local_path=destination,
        final_url=str(meta.get("final_url", url)),
        status_code=int(meta.get("status_code", 0)),
        content_type=str(meta.get("content_type", "")),
        checksum_sha256=sha256_bytes(content),
        size_bytes=len(content),
        downloaded_at_utc=now_iso,
        status="downloaded",
    )


def _write_manifest_row(
    manifest_path: Path,
    result: DownloadResult,
    *,
    role: str,
    parse_status: str,
    parse_reason: str,
    round_id: str | None,
) -> None:
    upsert_manifest_row(
        manifest_path,
        {
            "source_url": result.url,
            "final_url": result.final_url,
            "local_path": str(result.local_path),
            "checksum_sha256": result.checksum_sha256,
            "size_bytes": result.size_bytes,
            "status_code": result.status_code,
            "content_type": result.content_type,
            "downloaded_at_utc": result.downloaded_at_utc,
            "download_status": result.status,
            "role": role,
            "parse_status": parse_status,
            "parse_reason": parse_reason,
            "round_id": round_id,
        },
        key_cols=["source_url", "local_path", "role"],
    )


def _build_dirs(outdir: Path | None) -> dict[str, Path]:
    if outdir is None:
        raw_base = raw_dir()
        interim_base = interim_dir()
    else:
        raw_base = outdir / "raw"
        interim_base = outdir / "interim"
    return {
        "raw_lottery": ensure_dir(raw_base / "cgu" / "lottery_lists"),
        "interim_lottery": ensure_dir(interim_base / "cgu" / "lottery_lists"),
        "manifest": ensure_dir(interim_base / "manifests"),
        "reports": ensure_dir(interim_base / "reports" / "cgu_lottery"),
    }

def _empty_rounds_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "round_id",
            "round_label",
            "round_url",
            "source_page_url",
            "discovered_at",
            "parse_status",
            "parse_reason",
            "parsed_municipalities_count",
            "raw_path",
            "primary_resource_type",
            "error_message",
            "download_ok",
        ]
    )


def _empty_municipalities_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "round_id",
            "round_label",
            "municipality_name_raw",
            "uf",
            "municipality_name_clean",
            "source_url",
            "source_snippet",
            "parse_method",
            "confidence",
            "discovered_at",
        ]
    )


def run_ingestion(
    outdir: Path | None,
    force: bool,
    max_rounds: int | None,
    dry_run: bool,
) -> tuple[bool, Path | None]:
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    dirs = _build_dirs(outdir)

    manifest_path = dirs["manifest"] / "cgu_lottery_manifest.parquet"
    rounds_path = dirs["interim_lottery"] / "lottery_rounds.parquet"
    municipalities_path = dirs["interim_lottery"] / "lottery_municipalities.parquet"
    summary_path = dirs["interim_lottery"] / "lottery_rounds_summary.parquet"

    stage_seconds = {k: 0.0 for k in ["download_index", "parse_index", "download_rounds", "parse_rounds", "write_outputs"]}
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    index_latest = dirs["raw_lottery"] / "index_latest.html"
    index_snapshot = dirs["raw_lottery"] / f"index_{run_timestamp}.html"

    failed_rounds: list[dict[str, str]] = []
    round_rows: list[dict[str, object]] = []
    municipality_rows: list[dict[str, object]] = []
    round_download_failures = 0

    try:
        t0 = perf_counter()
        if dry_run:
            index_content, _ = fetch_url(INDEX_URL, headers=headers)
            index_html = index_content.decode("utf-8", errors="ignore")
            index_result = None
        else:
            index_result = _download_with_cache(INDEX_URL, index_latest, headers=headers, force=force)
            if not index_snapshot.exists() or force:
                atomic_write_bytes(index_snapshot, index_latest.read_bytes())
            index_html = index_latest.read_text(encoding="utf-8", errors="ignore")
        stage_seconds["download_index"] = perf_counter() - t0

        if not dry_run and index_result is not None:
            _write_manifest_row(manifest_path, index_result, role="index_page", parse_status="downloaded", parse_reason="", round_id=None)
            upsert_manifest_row(
                manifest_path,
                {
                    "source_url": INDEX_URL,
                    "final_url": index_result.final_url,
                    "local_path": str(index_snapshot),
                    "checksum_sha256": sha256_file(index_snapshot),
                    "size_bytes": index_snapshot.stat().st_size,
                    "status_code": index_result.status_code,
                    "content_type": index_result.content_type,
                    "downloaded_at_utc": _utc_now_iso(),
                    "download_status": "snapshot",
                    "role": "index_snapshot",
                    "parse_status": "saved",
                    "parse_reason": "",
                    "round_id": None,
                },
                key_cols=["local_path", "role"],
            )

        t0 = perf_counter()
        discovered_rounds = extract_round_links(index_html, INDEX_URL)
        if max_rounds is not None:
            discovered_rounds = discovered_rounds[: max(0, max_rounds)]
        stage_seconds["parse_index"] = perf_counter() - t0

        for idx, round_item in enumerate(discovered_rounds, start=1):
            round_label = str(round_item["round_label"])
            round_url = str(round_item["url"])
            rid = _round_id(round_label, round_url)
            base_name = f"{idx:03d}_{_slugify(round_label)}_{rid[-6:]}"
            record: dict[str, object] = {
                "round_id": rid,
                "round_label": round_label,
                "round_url": round_url,
                "source_page_url": round_item["source_page_url"],
                "discovered_at": round_item["discovered_at"],
                "parse_status": "pending",
                "parse_reason": "",
                "parsed_municipalities_count": 0,
                "raw_path": "",
                "primary_resource_type": "unknown",
                "error_message": "",
                "download_ok": False,
            }

            ext = ".bin"
            local_path: Path | None = None
            html_text = ""
            result: DownloadResult | None = None

            tdl = perf_counter()
            try:
                existing_files = sorted(dirs["raw_lottery"].glob(f"{base_name}.*"))
                if existing_files and not force:
                    local_path = existing_files[0]
                    ext = local_path.suffix.lower()
                    result = _download_with_cache(round_url, local_path, headers=headers, force=False)
                    if ext in {".html", ".htm"}:
                        html_text = local_path.read_text(encoding="utf-8", errors="ignore")
                else:
                    content, meta = fetch_url(round_url, headers=headers)
                    ext = _guess_extension(str(meta.get("content_type", "")), round_url)
                    if ext in {".html", ".htm"}:
                        html_text = content.decode("utf-8", errors="ignore")
                    if not dry_run:
                        local_path = dirs["raw_lottery"] / f"{base_name}{ext}"
                        atomic_write_bytes(local_path, content)
                    result = DownloadResult(
                        url=round_url,
                        local_path=local_path if local_path is not None else Path(f"<dry-run>/{base_name}{ext}"),
                        final_url=str(meta.get("final_url", round_url)),
                        status_code=int(meta.get("status_code", 0)),
                        content_type=str(meta.get("content_type", "")),
                        checksum_sha256=sha256_bytes(content),
                        size_bytes=len(content),
                        downloaded_at_utc=_utc_now_iso(),
                        status="downloaded" if not dry_run else "dry_run",
                    )
                record["download_ok"] = True
            except Exception as exc:  # noqa: BLE001
                round_download_failures += 1
                reason = str(exc)[:500]
                record["parse_status"] = "failed"
                record["parse_reason"] = "download_failed"
                record["error_message"] = reason
                failed_rounds.append({"round_id": rid, "round_label": round_label, "round_url": round_url, "reason": reason})
            finally:
                stage_seconds["download_rounds"] += perf_counter() - tdl

            if not record["download_ok"]:
                round_rows.append(record)
                continue

            tpr = perf_counter()
            try:
                if ext in {".html", ".htm"}:
                    parsed = process_round_html(round_url, html_text)
                else:
                    parsed = RoundProcessResult(
                        parse_status="needs_pdf_parse",
                        parse_reason=f"non_html_resource:{ext}",
                        municipalities=[],
                        linked_resource_urls=[],
                        primary_resource_type=_resource_type_from_extension(ext),
                        error_message="",
                    )

                record["parse_status"] = parsed.parse_status
                record["parse_reason"] = parsed.parse_reason
                record["primary_resource_type"] = parsed.primary_resource_type
                record["parsed_municipalities_count"] = len(parsed.municipalities)
                record["raw_path"] = str(local_path) if local_path is not None else str(result.local_path)

                for row in parsed.municipalities:
                    municipality_rows.append(
                        {
                            "round_id": rid,
                            "round_label": round_label,
                            "municipality_name_raw": row["municipality_name_raw"],
                            "uf": row["uf"],
                            "municipality_name_clean": row["municipality_name_clean"],
                            "source_url": round_url,
                            "source_snippet": row["source_snippet"],
                            "parse_method": row["parse_method"],
                            "confidence": row["confidence"],
                            "discovered_at": round_item["discovered_at"],
                        }
                    )

                if not dry_run and result is not None:
                    _write_manifest_row(
                        manifest_path,
                        result,
                        role="round_page_or_resource",
                        parse_status=str(record["parse_status"]),
                        parse_reason=str(record["parse_reason"]),
                        round_id=rid,
                    )

                if ext in {".html", ".htm"} and parsed.linked_resource_urls:
                    for resource_url in parsed.linked_resource_urls:
                        if dry_run:
                            logger.info("[dry-run] would download resource: {}", resource_url)
                            continue
                        resource_ext = Path(urlparse(resource_url).path).suffix.lower() or ".bin"
                        resource_name = _slugify(Path(urlparse(resource_url).path).stem)
                        res_path = dirs["raw_lottery"] / f"{base_name}_resource_{resource_name}{resource_ext}"
                        try:
                            res_result = _download_with_cache(resource_url, res_path, headers=headers, force=force)
                            _write_manifest_row(
                                manifest_path,
                                res_result,
                                role="round_resource",
                                parse_status="downloaded",
                                parse_reason="linked_from_round_page",
                                round_id=rid,
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("Failed downloading linked resource {}: {}", resource_url, exc)
            except Exception as exc:  # noqa: BLE001
                reason = str(exc)[:500]
                record["parse_status"] = "failed"
                record["parse_reason"] = "parse_failed"
                record["error_message"] = reason
                failed_rounds.append({"round_id": rid, "round_label": round_label, "round_url": round_url, "reason": reason})
            finally:
                stage_seconds["parse_rounds"] += perf_counter() - tpr

            round_rows.append(record)

        rounds_df = pd.DataFrame(round_rows) if round_rows else _empty_rounds_df()
        municipalities_df = pd.DataFrame(municipality_rows) if municipality_rows else _empty_municipalities_df()
        if not municipalities_df.empty:
            municipalities_df = municipalities_df.drop_duplicates(
                subset=["round_id", "municipality_name_clean", "uf"],
                keep="first",
            ).reset_index(drop=True)

        rounds_summary_df = pd.DataFrame(
            [
                {
                    "round_id": row.get("round_id"),
                    "round_label": row.get("round_label"),
                    "url": row.get("round_url"),
                    "download_ok": bool(row.get("download_ok", False)),
                    "parse_status": row.get("parse_status"),
                    "municipality_count": int(row.get("parsed_municipalities_count", 0) or 0),
                    "primary_resource_type": row.get("primary_resource_type", "unknown"),
                    "error_message": row.get("error_message", ""),
                }
                for row in round_rows
            ]
        )

        rounds_discovered = len(discovered_rounds)
        rounds_parsed_successfully = int((rounds_df["parse_status"] == "parsed").sum()) if not rounds_df.empty else 0
        municipalities_extracted = len(municipalities_df)

        success = True
        warnings: list[str] = []
        if rounds_df.empty and (max_rounds != 0):
            success = False
            warnings.append("lottery_rounds.parquet would be empty")
        if rounds_discovered > 0 and (round_download_failures / rounds_discovered) > 0.5:
            success = False
            warnings.append("more than 50% of discovered rounds failed download")
        if municipalities_extracted < 10:
            warnings.append("municipality extraction yielded fewer than 10 rows; parsing may be degraded")
            logger.warning("municipality extraction yielded fewer than 10 rows; parsing may be degraded")

        if dry_run:
            logger.info("[dry-run] would write {}", rounds_path)
            logger.info("[dry-run] would write {}", municipalities_path)
            logger.info("[dry-run] would write {}", summary_path)
        else:
            tw = perf_counter()
            write_parquet(rounds_df, rounds_path)
            write_parquet(municipalities_df, municipalities_path)
            write_parquet(rounds_summary_df, summary_path)
            for output_path, role in [
                (rounds_path, "generated_rounds_parquet"),
                (municipalities_path, "generated_municipalities_parquet"),
                (summary_path, "generated_rounds_summary_parquet"),
            ]:
                upsert_manifest_row(
                    manifest_path,
                    {
                        "source_url": "generated",
                        "final_url": "generated",
                        "local_path": str(output_path),
                        "checksum_sha256": sha256_file(output_path),
                        "size_bytes": output_path.stat().st_size,
                        "status_code": 0,
                        "content_type": "application/x-parquet",
                        "downloaded_at_utc": _utc_now_iso(),
                        "download_status": "generated",
                        "role": role,
                        "parse_status": "generated",
                        "parse_reason": "",
                        "round_id": None,
                    },
                    key_cols=["local_path", "role"],
                )
            stage_seconds["write_outputs"] = perf_counter() - tw

        report = {
            "timestamp_utc": _utc_now_iso(),
            "success": success,
            "dry_run": dry_run,
            "metrics": {
                "rounds_discovered": rounds_discovered,
                "rounds_parsed_successfully": rounds_parsed_successfully,
                "municipalities_extracted": municipalities_extracted,
                "failed_rounds_count": len(failed_rounds),
                "round_download_failures": round_download_failures,
            },
            "health_checks": {
                "rounds_non_empty": not rounds_df.empty,
                "download_fail_ratio_ok": True if rounds_discovered == 0 else (round_download_failures / rounds_discovered) <= 0.5,
                "municipalities_low_warning": municipalities_extracted < 10,
            },
            "timing_seconds": {k: round(v, 3) for k, v in stage_seconds.items()},
            "warnings": warnings,
            "failed_rounds": failed_rounds,
            "artifacts": {
                "rounds_parquet": str(rounds_path),
                "municipalities_parquet": str(municipalities_path),
                "rounds_summary_parquet": str(summary_path),
                "manifest": str(manifest_path),
            },
        }

        report_path: Path | None = None
        if not dry_run:
            report_path = dirs["reports"] / f"run_report_{run_timestamp}.json"
            payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
            atomic_write_bytes(report_path, payload)
            atomic_write_bytes(dirs["reports"] / "run_report_latest.json", payload)
            atomic_write_bytes(dirs["reports"] / "latest.json", payload)

        print(
            "CGU lottery ingestion summary | "
            f"rounds_discovered={rounds_discovered} "
            f"rounds_parsed_successfully={rounds_parsed_successfully} "
            f"municipalities_extracted={municipalities_extracted} "
            f"failed_rounds={len(failed_rounds)} "
            f"dry_run={int(dry_run)}"
        )
        if report_path is not None:
            print(f"Report: {report_path}")
        return success, report_path
    except Exception:  # noqa: BLE001
        logger.exception("CGU lottery ingestion failed")
        fallback_path: Path | None = None
        if not dry_run:
            fallback_path = dirs["reports"] / datetime.now(timezone.utc).strftime("run_report_%Y%m%dT%H%M%SZ.json")
            payload = json.dumps(
                {
                    "timestamp_utc": _utc_now_iso(),
                    "success": False,
                    "dry_run": dry_run,
                    "error": "fatal_error_before_report_completion",
                    "timing_seconds": {k: round(v, 3) for k, v in stage_seconds.items()},
                },
                indent=2,
            ).encode("utf-8")
            atomic_write_bytes(fallback_path, payload)
            atomic_write_bytes(dirs["reports"] / "latest.json", payload)
        print("CGU lottery ingestion summary | fatal_error=1")
        if fallback_path is not None:
            print(f"Report: {fallback_path}")
        return False, fallback_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest CGU lottery municipality lists.")
    parser.add_argument("--outdir", type=Path, default=None, help="Optional base output dir (expects raw/ and interim/).")
    parser.add_argument("--force", action="store_true", help="Redownload files even when cached.")
    parser.add_argument("--max-rounds", type=int, default=None, help="Limit rounds for debug runs.")
    parser.add_argument("--log-level", type=str, default=None, help="Override log level.")
    parser.add_argument("--dry-run", action="store_true", help="Plan download/parse without writing files.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    settings = get_settings()
    setup_logging(args.log_level or settings.log_level)
    success, _ = run_ingestion(
        outdir=args.outdir,
        force=bool(args.force),
        max_rounds=args.max_rounds,
        dry_run=bool(args.dry_run),
    )
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
