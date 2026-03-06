"""Probe public CGU report download endpoints and index downloadable files."""

from __future__ import annotations

import argparse
import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from audits_punishment.build.build_manifests import upsert_manifest_row
from audits_punishment.config import get_settings
from audits_punishment.logging import setup_logging
from audits_punishment.paths import clean_dir, interim_dir, raw_dir
from audits_punishment.utils.http import FetchUrlError, fetch_url
from audits_punishment.utils.io import atomic_write_bytes, ensure_dir, sha256_bytes, sha256_file, write_parquet


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_id() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _normalize_col(name: str) -> str:
    val = (name or "").strip().lower()
    val = re.sub(r"[^a-z0-9]+", "_", val)
    return val.strip("_")


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for cand in candidates:
        key = _normalize_col(cand)
        if key in df.columns:
            return key
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_bytes(path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def _manifest_row(
    manifest_path: Path,
    *,
    item_type: str,
    source_url: str,
    local_path: Path,
    status: str,
    notes: str,
) -> None:
    if local_path.exists():
        digest = sha256_file(local_path)
        size = local_path.stat().st_size
    else:
        digest = ""
        size = 0
    upsert_manifest_row(
        manifest_path,
        {
            "item_type": item_type,
            "source_url": source_url,
            "local_path": str(local_path),
            "sha256": digest,
            "size_bytes": size,
            "created_at_utc": _utc_now_iso(),
            "status": status,
            "notes": notes,
        },
        key_cols=["item_type", "local_path"],
    )


def detect_file_signature(data: bytes, content_type: str) -> tuple[str, bool]:
    ctype = (content_type or "").lower()
    starts_pdf = data.startswith(b"%PDF")
    starts_zip = data.startswith(b"PK\x03\x04")
    starts_ole = data.startswith(bytes.fromhex("D0CF11E0"))

    if starts_pdf or "pdf" in ctype:
        return "pdf", True
    if starts_zip:
        return "docx", True
    if starts_ole or "msword" in ctype or "officedocument" in ctype:
        return "doc", True
    return "bin", False


def _resolve_input_table() -> Path:
    p1 = clean_dir() / "cgu" / "audit_events_long.parquet"
    p2 = clean_dir() / "cgu" / "audits_open_data" / "auditorias_with_ibge.parquet"
    if p1.exists():
        return p1
    if p2.exists():
        return p2
    raise FileNotFoundError(f"Missing input table. Expected one of: {p1} or {p2}")


def _prepare_probe_table(input_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(input_path).copy()
    df.columns = [_normalize_col(c) for c in df.columns]

    idtarefa_col = _pick_col(df, ["idtarefa", "id_tarefa", "id_da_tarefa"])
    idauditoria_col = _pick_col(df, ["idauditoria", "id_auditoria", "id_da_auditoria"])
    pub_col = _pick_col(df, ["publication_date", "data_publicacao", "data_de_publicacao"])
    uf_col = _pick_col(df, ["uf", "ufs"])
    muni_col = _pick_col(df, ["municipality_name_raw", "municipality_name_clean", "municipios", "municipio"])
    ibge_col = _pick_col(df, ["ibge_municipality_code", "ibge"])

    missing_required = [
        name
        for name, col in [
            ("idtarefa", idtarefa_col),
            ("publication_date", pub_col),
            ("uf", uf_col),
            ("municipality_name_raw", muni_col),
            ("ibge_municipality_code", ibge_col),
        ]
        if col is None
    ]
    if missing_required:
        raise RuntimeError(f"Input is missing required columns: {missing_required}")

    out = pd.DataFrame(
        {
            "idtarefa": df[idtarefa_col].astype("string").str.strip(),
            "idauditoria": df[idauditoria_col].astype("string").str.strip() if idauditoria_col else pd.NA,
            "publication_date": pd.to_datetime(df[pub_col], errors="coerce"),
            "uf": df[uf_col].astype("string").str.upper().str.strip(),
            "municipality_name_raw": df[muni_col].astype("string").str.strip(),
            "ibge_municipality_code": df[ibge_col].astype("string").str.zfill(7),
        }
    )
    out = out.loc[out["idtarefa"].notna() & out["idtarefa"].astype(str).str.strip().ne("")].copy()
    out = out.drop_duplicates(subset=["idtarefa"], keep="first")
    return out


def _sample_rows(df: pd.DataFrame, sample: int, seed: int) -> pd.DataFrame:
    if sample <= 0 or sample >= len(df):
        return df.copy()
    idx = list(df.index)
    rng = random.Random(seed)
    rng.shuffle(idx)
    return df.loc[idx[:sample]].copy()


def _download_candidate_urls(idtarefa: str, idauditoria: str | None, host: str) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    urls.append((f"download_idtarefa:{idtarefa}", f"https://{host}.cgu.gov.br/relatorios/download/{idtarefa}"))
    if idauditoria and str(idauditoria).strip() and str(idauditoria).strip() != str(idtarefa).strip():
        aid = str(idauditoria).strip()
        urls.append((f"download_idauditoria:{aid}", f"https://{host}.cgu.gov.br/relatorios/download/{aid}"))
    return urls


def _landing_candidate_urls(idtarefa: str, host: str) -> list[tuple[str, str]]:
    return [
        ("landing_relatorio", f"https://{host}.cgu.gov.br/relatorio/{idtarefa}"),
        ("landing_relatorios_relatorio", f"https://{host}.cgu.gov.br/relatorios/relatorio/{idtarefa}"),
    ]


def _fetch_with_retry(
    url: str,
    *,
    timeout_seconds: int,
    headers: dict[str, str],
    sleep_seconds: float,
    max_retries: int,
) -> tuple[bytes, dict[str, Any], str]:
    attempt = 0
    while True:
        try:
            content, meta = fetch_url(url, timeout=timeout_seconds, headers=headers)
            return content, meta, ""
        except FetchUrlError as exc:
            meta = exc.metadata or {}
            status = int(meta.get("status_code", -1))
            is_retryable = status >= 500 or status == -1
            if attempt >= max_retries or not is_retryable:
                return b"", meta, str(exc)
            time.sleep(sleep_seconds * (2 ** attempt))
            attempt += 1
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                return b"", {"status_code": -1, "content_type": "", "final_url": url, "elapsed_ms": 0.0}, str(exc)
            time.sleep(sleep_seconds * (2 ** attempt))
            attempt += 1


def run_probe(
    *,
    sample: int,
    seed: int,
    timeout_seconds: int,
    sleep_seconds: float,
    force: bool,
    hosts: list[str],
    max_per_host: int,
    log_level: str,
) -> tuple[bool, dict[str, Any]]:
    setup_logging(log_level)
    settings = get_settings()
    run_id = _run_id()

    downloads_base = ensure_dir(raw_dir() / "cgu" / "reports" / "downloads")
    html_base = ensure_dir(raw_dir() / "cgu" / "reports" / "html")
    interim_reports_dir = ensure_dir(interim_dir() / "cgu" / "reports")
    report_dir = ensure_dir(interim_dir() / "reports" / "cgu_reports_probe")
    manifest_dir = ensure_dir(interim_dir() / "manifests")

    attempts_path = interim_reports_dir / "report_download_attempts.parquet"
    index_path = interim_reports_dir / "report_downloads_index.parquet"
    report_path = report_dir / f"{run_id}_report.json"
    latest_path = report_dir / "latest.json"
    manifest_path = manifest_dir / "cgu_reports_probe_manifest.parquet"

    report: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": _utc_now_iso(),
        "success": False,
        "artifacts": {
            "download_attempts": str(attempts_path),
            "downloads_index": str(index_path),
            "report": str(report_path),
            "latest": str(latest_path),
            "downloads_dir": str(downloads_base),
            "manifest": str(manifest_path),
        },
        "warnings": [],
        "errors": [],
    }

    attempts: list[dict[str, Any]] = []
    index_rows: list[dict[str, Any]] = []

    try:
        input_path = _resolve_input_table()
        probe_df = _prepare_probe_table(input_path)
        sample_df = _sample_rows(probe_df, sample=sample, seed=seed)

        report["sample_size"] = int(sample)
        report["unique_idtarefa"] = int(len(sample_df))
        report["hosts"] = hosts
        report["input_path"] = str(input_path)

        file_headers = {
            "User-Agent": settings.user_agent,
            "Accept": "application/pdf,application/octet-stream,*/*",
        }
        html_headers = {
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml",
        }

        success_count = 0
        for i, row in enumerate(sample_df.itertuples(index=False), start=1):
            idtarefa = str(row.idtarefa)
            idauditoria = None if pd.isna(row.idauditoria) else str(row.idauditoria)
            host_successes = 0

            for host in hosts:
                if host_successes >= max_per_host:
                    break
                host_dir = ensure_dir(downloads_base / host)
                html_host_dir = ensure_dir(html_base / host)

                # Download-first strategy.
                for strategy, url in _download_candidate_urls(idtarefa, idauditoria, host):
                    out_path_base = host_dir / idtarefa
                    if (not force):
                        existing = list(host_dir.glob(f"{idtarefa}.*"))
                        if existing:
                            ext = existing[0].suffix.lstrip(".") or "bin"
                            attempts.append(
                                {
                                    "run_id": run_id,
                                    "idtarefa": idtarefa,
                                    "host": host,
                                    "url": url,
                                    "status_code": 200,
                                    "content_type": "cached",
                                    "bytes_len": existing[0].stat().st_size,
                                    "elapsed_ms": 0.0,
                                    "ok": True,
                                    "ext": ext,
                                    "sha256": sha256_file(existing[0]),
                                    "error": "",
                                }
                            )
                            index_rows.append(
                                {
                                    "idtarefa": idtarefa,
                                    "host": host,
                                    "url": url,
                                    "ext": ext,
                                    "sha256": sha256_file(existing[0]),
                                    "file_path": str(existing[0]),
                                    "downloaded_at_utc": _utc_now_iso(),
                                }
                            )
                            host_successes += 1
                            success_count += 1
                            break

                    t0 = time.perf_counter()
                    content, meta, err = _fetch_with_retry(
                        url,
                        timeout_seconds=timeout_seconds,
                        headers=file_headers,
                        sleep_seconds=sleep_seconds,
                        max_retries=2,
                    )
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    status_code = int(meta.get("status_code", -1))
                    content_type = str(meta.get("content_type", ""))
                    bytes_len = len(content)

                    ext, signature_ok = detect_file_signature(content, content_type)
                    ok = bool(status_code == 200 and bytes_len >= 10_000 and signature_ok)
                    sha = sha256_bytes(content) if ok else ""

                    attempts.append(
                        {
                            "run_id": run_id,
                            "idtarefa": idtarefa,
                            "host": host,
                            "url": str(meta.get("final_url", url)),
                            "status_code": status_code,
                            "content_type": content_type,
                            "bytes_len": bytes_len,
                            "elapsed_ms": elapsed_ms,
                            "ok": ok,
                            "ext": ext,
                            "sha256": sha,
                            "error": err,
                        }
                    )

                    if ok:
                        out_path = out_path_base.with_suffix(f".{ext}")
                        atomic_write_bytes(out_path, content)
                        _manifest_row(
                            manifest_path,
                            item_type="downloaded",
                            source_url=url,
                            local_path=out_path,
                            status="ok",
                            notes=f"download:{strategy}",
                        )
                        index_rows.append(
                            {
                                "idtarefa": idtarefa,
                                "host": host,
                                "url": str(meta.get("final_url", url)),
                                "ext": ext,
                                "sha256": sha,
                                "file_path": str(out_path),
                                "downloaded_at_utc": _utc_now_iso(),
                            }
                        )
                        host_successes += 1
                        success_count += 1
                        break

                if host_successes >= max_per_host:
                    continue

                # Optional HTML fallback for diagnostics if direct download failed.
                for strategy, hurl in _landing_candidate_urls(idtarefa, host):
                    content, meta, err = _fetch_with_retry(
                        hurl,
                        timeout_seconds=timeout_seconds,
                        headers=html_headers,
                        sleep_seconds=sleep_seconds,
                        max_retries=2,
                    )
                    status_code = int(meta.get("status_code", -1))
                    ctype = str(meta.get("content_type", ""))
                    attempts.append(
                        {
                            "run_id": run_id,
                            "idtarefa": idtarefa,
                            "host": host,
                            "url": str(meta.get("final_url", hurl)),
                            "status_code": status_code,
                            "content_type": ctype,
                            "bytes_len": len(content),
                            "elapsed_ms": float(meta.get("elapsed_ms", 0.0)),
                            "ok": False,
                            "ext": "html",
                            "sha256": "",
                            "error": err,
                        }
                    )
                    if content and status_code in {200, 403, 404}:
                        suffix = "" if status_code == 200 else "_error"
                        html_path = html_host_dir / f"{idtarefa}{suffix}.html"
                        atomic_write_bytes(html_path, content[:20_000] if status_code in {403, 404} else content)
                        _manifest_row(
                            manifest_path,
                            item_type="downloaded",
                            source_url=hurl,
                            local_path=html_path,
                            status="ok" if status_code == 200 else "failed",
                            notes=f"html_fallback:{strategy}:{status_code}",
                        )

            if i % 5 == 0:
                logger.info("processed {}/{}; downloads_ok={}", i, len(sample_df), success_count)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        attempts_df = pd.DataFrame(attempts)
        index_df = pd.DataFrame(index_rows).drop_duplicates(subset=["idtarefa", "host"], keep="last")

        status_hist = attempts_df["status_code"].value_counts(dropna=False).to_dict() if not attempts_df.empty else {}
        ctype_hist = attempts_df["content_type"].fillna("<NA>").value_counts(dropna=False).to_dict() if not attempts_df.empty else {}
        ext_hist = attempts_df["ext"].fillna("<NA>").value_counts(dropna=False).to_dict() if not attempts_df.empty else {}

        sample_n = int(len(sample_df))
        success_rate = float(len(index_df)) / max(sample_n, 1)
        report.update(
            {
                "download_success_count": int(len(index_df)),
                "download_success_rate": success_rate,
                "status_code_histogram": {str(k): int(v) for k, v in status_hist.items()},
                "content_type_histogram": {str(k): int(v) for k, v in ctype_hist.items()},
                "ext_histogram": {str(k): int(v) for k, v in ext_hist.items()},
                "median_bytes_len": float(attempts_df["bytes_len"].median()) if not attempts_df.empty else 0.0,
                "median_elapsed_ms": float(attempts_df["elapsed_ms"].median()) if not attempts_df.empty else 0.0,
                "top10_failed_urls": attempts_df.loc[~attempts_df["ok"], ["url", "status_code"]]
                .head(10)
                .to_dict(orient="records")
                if not attempts_df.empty
                else [],
            }
        )

        report["success"] = bool(
            (sample_n >= 10 and success_rate >= 0.5)
            or (sample_n < 10 and int(len(index_df)) >= 5)
        )

        return bool(report["success"]), report
    except Exception as exc:  # noqa: BLE001
        logger.exception("CGU report probe failed: {}", exc)
        report["errors"].append(str(exc))
        report["success"] = False
        return False, report
    finally:
        attempts_df = pd.DataFrame(attempts)
        index_df = pd.DataFrame(index_rows)

        write_parquet(attempts_df, attempts_path)
        write_parquet(index_df, index_path)

        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=attempts_path,
            status="ok",
            notes="report_download_attempts",
        )
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=index_path,
            status="ok",
            notes="report_downloads_index",
        )

        report["ended_at_utc"] = _utc_now_iso()
        _write_json(report_path, report)
        _write_json(latest_path, report)

        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=report_path,
            status="ok" if report.get("success") else "failed",
            notes="run_report",
        )
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=latest_path,
            status="ok" if report.get("success") else "failed",
            notes="latest_report",
        )


def _parse_hosts(hosts_arg: str) -> list[str]:
    allowed = {"eaud", "ecgu"}
    parsed = [h.strip().lower() for h in hosts_arg.split(",") if h.strip()]
    out = [h for h in parsed if h in allowed]
    return out or ["eaud"]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe direct CGU report downloads via e-Aud/e-CGU endpoints.")
    parser.add_argument("--sample", type=int, default=30)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--hosts", default="eaud", help="Comma-separated: eaud,ecgu")
    parser.add_argument("--max-per-host", type=int, default=1)
    parser.add_argument("--log-level", default=get_settings().log_level)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    hosts = _parse_hosts(str(args.hosts))
    ok, report = run_probe(
        sample=int(args.sample),
        seed=int(args.seed),
        timeout_seconds=int(args.timeout_seconds),
        sleep_seconds=float(args.sleep_seconds),
        force=bool(args.force),
        hosts=hosts,
        max_per_host=int(args.max_per_host),
        log_level=str(args.log_level),
    )
    print(
        "CGU report download probe | "
        f"success={report.get('success', False)} "
        f"sample={report.get('sample_size', 0)} "
        f"downloads_ok={report.get('download_success_count', 0)} "
        f"success_rate={float(report.get('download_success_rate', 0.0)):.3f}"
    )
    print(f"Report: {report.get('artifacts', {}).get('report', '')}")
    print(f"Latest: {report.get('artifacts', {}).get('latest', '')}")
    print(f"Attempts: {report.get('artifacts', {}).get('download_attempts', '')}")
    print(f"Downloads index: {report.get('artifacts', {}).get('downloads_index', '')}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
