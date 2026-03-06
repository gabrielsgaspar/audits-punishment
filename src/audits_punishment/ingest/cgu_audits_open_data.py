"""Ingest CGU Auditorias open-data CSV and link to lottery municipalities."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from audits_punishment.build.build_manifests import upsert_manifest_row
from audits_punishment.build.crosswalk_ibge import (
    build_crosswalk,
    ensure_seed_reference_files,
    load_alias_table,
    load_override_table,
    normalize_name,
)
from audits_punishment.config import get_settings
from audits_punishment.logging import setup_logging
from audits_punishment.paths import clean_dir, interim_dir, raw_dir
from audits_punishment.utils.http import FetchUrlError, fetch_url
from audits_punishment.utils.io import atomic_write_bytes, ensure_dir, sha256_bytes, sha256_file, write_parquet
from audits_punishment.utils.text import normalize_whitespace

AUDITORIAS_DATA_PAGE_URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/auditorias"
AUDITORIAS_CSV_URL = "https://dadosabertos-download.cgu.gov.br/Auditorias/Auditorias.csv"
EAUD_RELATORIOS_UI = "https://eaud.cgu.gov.br/relatorios"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_id() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


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


def _normalize_column_name(name: str) -> str:
    value = normalize_name(name or "").lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value.strip("_")


def _detect_delimiter_from_sample(sample_text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=";,\t|")
        if dialect.delimiter in {";", ",", "\t", "|"}:
            return dialect.delimiter
    except csv.Error:
        pass
    return ";"


def _read_csv_with_options(path: Path, *, encoding: str, sep: str) -> pd.DataFrame:
    kwargs: dict[str, Any] = {
        "encoding": encoding,
        "sep": sep,
        "engine": "python",
        "quotechar": '"',
        "escapechar": "\\",
    }
    try:
        return pd.read_csv(path, on_bad_lines="warn", **kwargs)
    except TypeError:
        # Backward compatibility for pandas versions without "warn".
        return pd.read_csv(path, on_bad_lines="skip", **kwargs)


def _read_csv_robust(path: Path) -> tuple[pd.DataFrame, str, str]:
    sample_bytes = path.read_bytes()[:50_000]
    sample_text = sample_bytes.decode("utf-8", errors="ignore")
    detected_delimiter = _detect_delimiter_from_sample(sample_text)

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            df = _read_csv_with_options(path, encoding=encoding, sep=detected_delimiter)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue

        # If parsing collapsed into one column while delimiter markers exist, retry with ';'.
        if (
            df.shape[1] == 1
            and detected_delimiter != ";"
            and ";" in sample_text
        ):
            try:
                df_retry = _read_csv_with_options(path, encoding=encoding, sep=";")
                if df_retry.shape[1] > df.shape[1]:
                    return df_retry, encoding, ";"
            except Exception:  # noqa: BLE001
                pass
        return df, encoding, detected_delimiter

    if last_error is not None:
        raise last_error
    raise RuntimeError("Could not parse CSV with supported encodings and delimiters.")


def _find_column(df: pd.DataFrame, aliases: list[str]) -> str:
    cols = {_normalize_column_name(c): c for c in df.columns}
    for alias in aliases:
        key = _normalize_column_name(alias)
        if key in cols:
            return cols[key]
    raise KeyError(f"Missing expected column. Tried aliases={aliases}")


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for cand in candidates:
        key = _normalize_column_name(cand)
        if key in df.columns:
            return key
    return None


def _fallback_location_from_siglas(out: pd.DataFrame) -> pd.DataFrame:
    siglas_col = pick_col(
        out,
        [
            "siglas_unidades_auditadas",
            "siglasunidadesauditadas",
        ],
    )
    if siglas_col is None:
        return out
    series = out[siglas_col].astype("string").fillna("").str.strip()
    # Conservative fallback: try extracting final "/UF" token if present.
    uf_guess = series.str.extract(r"/([A-Za-z]{2})\b", expand=False).astype("string")
    muni_guess = series.str.replace(r"\s*/[A-Za-z]{2}\b.*$", "", regex=True).str.strip()
    out["uf"] = out["uf"].where(out["uf"].astype("string").str.len() >= 2, uf_guess.str.upper())
    out["municipality_name_raw"] = out["municipality_name_raw"].where(
        out["municipality_name_raw"].astype("string").str.len() > 0,
        muni_guess,
    )
    return out


def _prepare_auditorias_df(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    original_cols = list(df.columns)
    rename_map = {c: _normalize_column_name(c) for c in df.columns}
    out = df.rename(columns=rename_map).copy()

    task_col = pick_col(out, ["idtarefa", "id_da_tarefa", "id_tarefa", "iddatarefa"])
    audit_col = pick_col(out, ["iddaauditoria", "id_da_auditoria", "idauditoria"])
    title_col = pick_col(out, ["titulodorelatorio", "titulo_do_relatorio", "titulo"])
    uf_col = pick_col(out, ["ufs", "uf", "sigla_uf", "estado", "siglaestado"])
    muni_col = pick_col(out, ["municipios", "municipio", "município", "cidade", "localidade"])
    date_col = pick_col(out, ["datapublicacao", "data_publicacao", "publicacao", "data_de_publicacao"])

    for col in [
        "id_da_tarefa",
        "id_da_auditoria",
        "titulo_do_relatorio",
        "uf",
        "municipality_name_raw",
        "municipality_name_norm",
        "publication_date",
    ]:
        if col not in out.columns:
            out[col] = pd.NA

    out["source_row_number"] = range(1, len(out) + 1)
    if task_col is not None:
        out["id_da_tarefa"] = out[task_col]
    if audit_col is not None:
        out["id_da_auditoria"] = out[audit_col]
    if title_col is not None:
        out["titulo_do_relatorio"] = out[title_col]
    if uf_col is not None:
        out["uf"] = out[uf_col]
    if muni_col is not None:
        out["municipality_name_raw"] = out[muni_col]
    if date_col is not None:
        out["publication_date"] = out[date_col]

    # Fallback only when explicit Municipios/UFs are not both available.
    if muni_col is None or uf_col is None:
        out = _fallback_location_from_siglas(out)

    out["id_da_tarefa"] = out["id_da_tarefa"].astype("string").str.strip()
    out["id_da_auditoria"] = out["id_da_auditoria"].astype("string").str.strip()
    out["titulo_do_relatorio"] = out["titulo_do_relatorio"].astype("string").str.strip()
    out["municipality_name_raw"] = (
        out["municipality_name_raw"].astype("string").fillna("").map(lambda x: normalize_whitespace(str(x)))
    )
    out["municipality_name_norm"] = out["municipality_name_raw"].map(normalize_name)
    out["municipality_name_clean"] = out["municipality_name_raw"]
    out["uf"] = out["uf"].astype("string").fillna("").str.strip().str.upper().str[:2]
    out["publication_date"] = pd.to_datetime(
        out["publication_date"], errors="coerce", dayfirst=True
    )
    out["data_de_publicacao"] = out["publication_date"]
    out["pulled_at_utc"] = _utc_now_iso()
    out["source_columns_original"] = ",".join(original_cols)
    meta = {
        "resolved_uf_col": uf_col,
        "resolved_muni_col": muni_col,
        "resolved_date_col": date_col,
        "resolved_task_col": task_col,
        "resolved_title_col": title_col,
    }
    return out, meta


def apply_only_sorteio_filter(
    df: pd.DataFrame,
    *,
    only_sorteio: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    group_col = pick_col(df, ["grupoatividade", "grupo_atividade", "grupo"])
    fef_col = pick_col(df, ["fef"])

    if group_col is not None:
        group_text = df[group_col].astype("string").fillna("").str.strip().str.lower()
        mask_group = group_text.str.contains("entes federativos", na=False)
    else:
        mask_group = pd.Series(False, index=df.index)

    if fef_col is not None:
        fef_text = df[fef_col].astype("string").fillna("").str.strip()
        mask_fef_nonnull = fef_text != ""
    else:
        mask_fef_nonnull = pd.Series(False, index=df.index)

    mask = mask_group | mask_fef_nonnull
    rows_matched = int(mask.sum())
    warning_messages: list[str] = []
    applied = False
    out = df

    if only_sorteio:
        if rows_matched > 0:
            out = df.loc[mask].copy()
            applied = True
        else:
            warning_messages.append(
                "only_sorteio requested but rule matched 0 rows; filter not applied."
            )
            out = df.copy()
    else:
        out = df.copy()

    return out, {
        "only_sorteio_requested": bool(only_sorteio),
        "sorteio_filter_column_used": "grupoatividade+fef_nonnull",
        "sorteio_filter_rule_used": "group_contains_entes_federativos OR fef_nonnull",
        "sorteio_rows_matched": rows_matched,
        "sorteio_filter_applied": applied,
        "resolved_grupo_col": group_col,
        "resolved_fef_col": fef_col,
        "warning_messages": warning_messages,
    }


def _ensure_edition_field(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str | None]:
    out = df.copy()
    existing = pick_col(
        out,
        [
            "edicao_programa_sorteio_fef",
            "edicao_sorteio_fef",
            "edicao_programa",
            "edicao",
        ],
    )
    if existing is not None:
        out["edicao_programa_sorteio_fef"] = out[existing].astype("string")
        return out, f"existing:{existing}", None

    fef_col = pick_col(out, ["fef"])
    if fef_col is not None:
        out["edicao_programa_sorteio_fef"] = out[fef_col].astype("string")
        return out, "derived_from_fef", "Optional column edicao_programa_sorteio_fef missing; derived from FEF."

    out["edicao_programa_sorteio_fef"] = pd.Series(pd.NA, index=out.index, dtype="string")
    return out, "missing", "Optional column edicao_programa_sorteio_fef missing and FEF unavailable."


def download_reference_csv(raw_csv_path: Path, force_download: bool, timeout_seconds: int) -> dict[str, Any]:
    if raw_csv_path.exists() and not force_download:
        return {
            "used_cache": True,
            "source_url": AUDITORIAS_CSV_URL,
            "rows": None,
        }
    settings = get_settings()
    content, _meta = fetch_url(
        AUDITORIAS_CSV_URL,
        timeout=timeout_seconds,
        headers={"User-Agent": settings.user_agent},
    )
    atomic_write_bytes(raw_csv_path, content)
    return {
        "used_cache": False,
        "source_url": AUDITORIAS_CSV_URL,
        "rows": None,
    }


def _build_ibge_mapping(
    audits_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    threshold: float,
    reference_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    alias_path, override_path = ensure_seed_reference_files(reference_dir)
    alias_df = load_alias_table(alias_path)
    override_df = load_override_table(override_path)
    mapped, manual = build_crosswalk(
        audits_df,
        reference_df,
        threshold=threshold,
        alias_df=alias_df,
        override_df=override_df,
    )
    return mapped, manual


def _extract_pdf_links(html_text: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html_text, "lxml")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = str(a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        hlow = href.lower()
        if hlow.endswith(".pdf"):
            links.append(full)
            continue
        if "/api/" in hlow and any(token in hlow for token in ["download", "arquivo", "pdf"]):
            links.append(full)

    for pattern in [
        r'https?://[^\"\'\s>]+\.pdf',
        r'\"(/api/[^\"\']*(?:download|arquivo|pdf)[^\"\']*)\"',
        r"'(/api/[^\"\']*(?:download|arquivo|pdf)[^\"\']*)'",
    ]:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
            links.append(urljoin(base_url, str(match)))

    out: list[str] = []
    seen: set[str] = set()
    for item in links:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _save_fetch_bytes(path: Path, content: bytes) -> None:
    atomic_write_bytes(path, content)


def _link_lottery_and_audits(
    lottery_df: pd.DataFrame,
    audits_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    lottery = lottery_df.copy()
    lottery["ibge_municipality_code"] = lottery["ibge_municipality_code"].astype("string").str.zfill(7)
    lottery["uf"] = lottery["uf"].astype("string").str.upper().str.strip()
    lottery["round_id"] = lottery["round_id"].astype("string")
    lottery["round_label"] = lottery["round_label"].astype("string")

    audits = audits_df.copy()
    audits["ibge_municipality_code"] = audits["ibge_municipality_code"].astype("string").str.zfill(7)
    audits["uf"] = audits["uf"].astype("string").str.upper().str.strip()

    lottery_keys = lottery[
        [c for c in ["ibge_municipality_code", "uf", "municipality_name_clean", "round_id", "round_label"] if c in lottery.columns]
    ].dropna(subset=["ibge_municipality_code", "uf"])
    desired_audit_cols = [
        "ibge_municipality_code",
        "uf",
        "municipality_name_raw",
        "municipality_name_clean",
        "id_da_tarefa",
        "id_da_auditoria",
        "titulo_do_relatorio",
        "publication_date",
        "data_de_publicacao",
        "grupoatividade",
        "linhaacao",
        "tiposervico",
        "fef",
        "edicao_programa_sorteio_fef",
    ]
    present_audit_cols = [c for c in desired_audit_cols if c in audits.columns]
    missing_optional = [c for c in desired_audit_cols if c not in audits.columns]
    audits_linkable = audits[present_audit_cols].dropna(subset=["ibge_municipality_code", "uf"]).copy()
    if "municipality_name_raw" not in audits_linkable.columns:
        fallback_muni_col = "municipality_name_clean" if "municipality_name_clean" in audits_linkable.columns else None
        audits_linkable["municipality_name_raw"] = (
            audits_linkable[fallback_muni_col] if fallback_muni_col else pd.Series(pd.NA, index=audits_linkable.index)
        )
    if "publication_date" not in audits_linkable.columns:
        if "data_de_publicacao" in audits_linkable.columns:
            audits_linkable["publication_date"] = audits_linkable["data_de_publicacao"]
        else:
            audits_linkable["publication_date"] = pd.NaT

    merged = lottery_keys.merge(
        audits_linkable,
        on=["ibge_municipality_code", "uf"],
        how="left",
        suffixes=("_lottery", "_audit"),
    )

    merged["eaud_relatorio_url"] = merged["id_da_tarefa"].map(
        lambda x: f"https://eaud.cgu.gov.br/relatorio/{x}" if pd.notna(x) and str(x).strip() else pd.NA
    )
    merged["eaud_relatorios_ui"] = EAUD_RELATORIOS_UI
    merged["link_rule_used"] = "ibge_exact"

    long_df = merged.rename(columns={"municipality_name_clean_lottery": "municipality_name"}).copy()
    if "municipality_name_raw" not in long_df.columns:
        if "municipality_name_raw_audit" in long_df.columns:
            long_df["municipality_name_raw"] = long_df["municipality_name_raw_audit"]
        elif "municipality_name_clean_audit" in long_df.columns:
            long_df["municipality_name_raw"] = long_df["municipality_name_clean_audit"]
        else:
            long_df["municipality_name_raw"] = pd.NA
    long_df["idtarefa"] = long_df["id_da_tarefa"] if "id_da_tarefa" in long_df.columns else pd.NA
    long_df["idauditoria"] = long_df["id_da_auditoria"] if "id_da_auditoria" in long_df.columns else pd.NA
    long_df["titulo"] = long_df["titulo_do_relatorio"] if "titulo_do_relatorio" in long_df.columns else pd.NA
    if "publication_date" not in long_df.columns:
        long_df["publication_date"] = long_df["data_de_publicacao"] if "data_de_publicacao" in long_df.columns else pd.NaT

    def _uniq_list(values: pd.Series) -> list[str]:
        out = []
        seen = set()
        for value in values.dropna().astype(str):
            value = value.strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    group_cols = ["ibge_municipality_code", "uf"]
    events = (
        long_df.groupby(group_cols, dropna=False)
        .agg(
            municipality_name=("municipality_name", "first"),
            lottery_round_ids=("round_id", _uniq_list),
            lottery_round_labels=("round_label", _uniq_list),
            municipality_name_raw=("municipality_name_raw", "first"),
            audit_task_id=("idtarefa", _uniq_list),
            audit_publication_date=("publication_date", lambda s: [x.isoformat() for x in s.dropna()]),
            audit_title=("titulo", _uniq_list),
            audit_audit_id=("idauditoria", _uniq_list),
            grupoatividade=("grupoatividade", _uniq_list) if "grupoatividade" in long_df.columns else ("municipality_name", lambda s: []),
            linhaacao=("linhaacao", _uniq_list) if "linhaacao" in long_df.columns else ("municipality_name", lambda s: []),
            tiposervico=("tiposervico", _uniq_list) if "tiposervico" in long_df.columns else ("municipality_name", lambda s: []),
            fef=("fef", _uniq_list) if "fef" in long_df.columns else ("municipality_name", lambda s: []),
            edition_program_sorteio_fef=("edicao_programa_sorteio_fef", _uniq_list)
            if "edicao_programa_sorteio_fef" in long_df.columns
            else ("municipality_name", lambda s: []),
        )
        .reset_index()
    )
    events["first_lottery_round"] = events["lottery_round_labels"].map(lambda x: x[0] if x else pd.NA)
    events["link_rule_used"] = "ibge_exact"

    lottery_unique = lottery_keys[["ibge_municipality_code", "uf"]].drop_duplicates()
    audits_unique = audits_linkable[["ibge_municipality_code", "uf"]].drop_duplicates()
    covered = lottery_unique.merge(audits_unique, on=["ibge_municipality_code", "uf"], how="inner")
    coverage_pct = float(len(covered)) / max(1, len(lottery_unique))

    date_dist = (
        audits_linkable["data_de_publicacao"]
        .dropna()
        .dt.to_period("Y")
        .astype(str)
        .value_counts()
        .sort_index()
        .to_dict()
    )
    stats = {
        "lottery_municipality_coverage_with_audits": coverage_pct,
        "num_lottery_municipalities": int(len(lottery_unique)),
        "num_lottery_municipalities_with_audit": int(len(covered)),
        "audit_publication_year_distribution": date_dist,
        "missing_optional_columns": missing_optional,
    }
    return events, long_df, stats


def run_pipeline(
    *,
    force_download: bool,
    force_rebuild: bool,
    only_sorteio: bool,
    download_pdfs: bool,
    max_rows: int | None,
    timeout_seconds: int,
    log_level: str,
) -> tuple[bool, dict[str, Any]]:
    setup_logging(log_level)
    run_id = _run_id()

    raw_base = ensure_dir(raw_dir() / "cgu" / "audits_open_data")
    raw_html_dir = ensure_dir(raw_base / "html")
    raw_pdf_dir = ensure_dir(raw_base / "pdfs")
    clean_reference_dir = ensure_dir(clean_dir() / "reference")
    clean_open_data_dir = ensure_dir(clean_dir() / "cgu" / "audits_open_data")
    clean_cgu_dir = ensure_dir(clean_dir() / "cgu")
    interim_open_data_dir = ensure_dir(interim_dir() / "cgu" / "audits_open_data")
    reports_dir = ensure_dir(interim_dir() / "reports" / "cgu_audits_open_data")
    manifests_dir = ensure_dir(interim_dir() / "manifests")

    raw_csv_path = raw_base / "Auditorias.csv"
    mapped_path = clean_open_data_dir / "auditorias_with_ibge.parquet"
    events_path = clean_cgu_dir / "audit_events.parquet"
    long_path = clean_cgu_dir / "audit_events_long.parquet"
    location_sample_path = interim_open_data_dir / "auditorias_location_sample.parquet"
    grupo_vc_path = interim_open_data_dir / "grupoatividade_value_counts.csv"
    fef_vc_path = interim_open_data_dir / "fef_value_counts.csv"
    manual_path = reports_dir / "manual_review_candidates.csv"
    report_path = reports_dir / f"{run_id}_report.json"
    latest_path = reports_dir / "latest.json"
    manifest_path = manifests_dir / "cgu_audits_open_data_manifest.parquet"

    report: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": _utc_now_iso(),
        "success": False,
        "source_urls": {
            "auditorias_data_page": AUDITORIAS_DATA_PAGE_URL,
            "auditorias_csv": AUDITORIAS_CSV_URL,
            "eaud_relatorios_ui": EAUD_RELATORIOS_UI,
        },
        "artifacts": {
            "raw_csv": str(raw_csv_path),
            "auditorias_with_ibge": str(mapped_path),
            "audit_events": str(events_path),
            "audit_events_long": str(long_path),
            "location_sample": str(location_sample_path),
            "grupoatividade_value_counts": str(grupo_vc_path),
            "fef_value_counts": str(fef_vc_path),
            "manual_review_candidates": str(manual_path),
            "report_json": str(report_path),
            "latest_json": str(latest_path),
            "manifest": str(manifest_path),
        },
        "warnings": [],
        "warning_messages": [],
        "errors": [],
    }

    try:
        if mapped_path.exists() and events_path.exists() and long_path.exists() and not force_rebuild:
            report["success"] = True
            msg = "Outputs already exist. Use --force-rebuild to recompute."
            report["warnings"].append(msg)
            report["warning_messages"].append(msg)
            return True, report

        dl_meta = download_reference_csv(raw_csv_path, force_download=force_download, timeout_seconds=timeout_seconds)
        _manifest_row(
            manifest_path,
            item_type="downloaded",
            source_url=AUDITORIAS_CSV_URL,
            local_path=raw_csv_path,
            status="ok",
            notes="auditorias_csv_cached" if dl_meta["used_cache"] else "auditorias_csv_downloaded",
        )

        csv_df, used_encoding, detected_delimiter = _read_csv_robust(raw_csv_path)
        total_rows_raw = int(len(csv_df))
        report["csv_rows_raw"] = total_rows_raw
        report["csv_detected_delimiter"] = detected_delimiter
        report["csv_used_encoding"] = used_encoding
        report["csv_num_columns"] = int(csv_df.shape[1])
        report["csv_columns_preview"] = [str(col) for col in list(csv_df.columns)[:15]]
        logger.info(
            "Auditorias CSV parsed | delimiter='{}' encoding='{}' num_columns={} columns_preview={}",
            detected_delimiter,
            used_encoding,
            int(csv_df.shape[1]),
            [str(col) for col in list(csv_df.columns)[:5]],
        )
        if int(csv_df.shape[1]) < 5:
            raise RuntimeError(
                "Auditorias.csv parse failure: parsed fewer than 5 columns "
                f"(delimiter='{detected_delimiter}', encoding='{used_encoding}', "
                f"num_columns={int(csv_df.shape[1])})."
            )
        if total_rows_raw < 1000:
            raise RuntimeError(
                f"Auditorias.csv has only {total_rows_raw} rows (<1000). Download likely failed or source changed."
            )

        parsed, parse_meta = _prepare_auditorias_df(csv_df)
        report.update(
            {
                "resolved_uf_col": parse_meta.get("resolved_uf_col"),
                "resolved_muni_col": parse_meta.get("resolved_muni_col"),
                "resolved_date_col": parse_meta.get("resolved_date_col"),
            }
        )
        share_missing_dates = float(parsed["publication_date"].isna().sum()) / max(1, len(parsed))
        report["share_publication_date_missing"] = share_missing_dates
        date_non_null = parsed["publication_date"].dropna()
        report["min_publication_date"] = (
            date_non_null.min().date().isoformat() if not date_non_null.empty else None
        )
        report["max_publication_date"] = (
            date_non_null.max().date().isoformat() if not date_non_null.empty else None
        )

        sample_df = parsed.copy().head(200)
        sample_df = sample_df.rename(
            columns={
                "id_da_tarefa": "IdTarefa",
                "titulo_do_relatorio": "Titulo",
            }
        )
        sample_cols = [
            col
            for col in [
                "IdTarefa",
                "Titulo",
                "publication_date",
                "uf",
                "municipality_name_raw",
                "municipality_name_norm",
            ]
            if col in sample_df.columns
        ]
        write_parquet(sample_df[sample_cols], location_sample_path)
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=location_sample_path,
            status="ok",
            notes="auditorias_location_sample",
        )

        # Write debug value-count artifacts before filtering.
        grupo_col = pick_col(parsed, ["grupoatividade", "grupo_atividade", "grupo"])
        if grupo_col is not None:
            parsed[grupo_col].astype("string").fillna("<NA>").value_counts(dropna=False).head(20).rename_axis(
                "grupoatividade"
            ).reset_index(name="count").to_csv(grupo_vc_path, index=False)
        else:
            pd.DataFrame({"grupoatividade": ["<missing_column>"], "count": [0]}).to_csv(grupo_vc_path, index=False)
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=grupo_vc_path,
            status="ok",
            notes="grupoatividade_value_counts_top20",
        )

        fef_col = pick_col(parsed, ["fef"])
        if fef_col is not None:
            parsed[fef_col].astype("string").fillna("<NA>").value_counts(dropna=False).head(20).rename_axis(
                "fef"
            ).reset_index(name="count").to_csv(fef_vc_path, index=False)
        else:
            pd.DataFrame({"fef": ["<missing_column>"], "count": [0]}).to_csv(fef_vc_path, index=False)
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=fef_vc_path,
            status="ok",
            notes="fef_value_counts_top20",
        )

        parsed, sort_meta = apply_only_sorteio_filter(parsed, only_sorteio=only_sorteio)
        report.update(sort_meta)
        report["warning_messages"].extend(sort_meta.get("warning_messages", []))
        report["warnings"].extend(sort_meta.get("warning_messages", []))

        parsed, edition_field_source, edition_warning = _ensure_edition_field(parsed)
        report["edition_field_source"] = edition_field_source
        if edition_warning:
            report["warning_messages"].append(edition_warning)
            report["warnings"].append(edition_warning)

        if max_rows is not None and max_rows >= 0:
            parsed = parsed.head(max_rows).copy()
        report["rows_after_filters"] = int(len(parsed))
        if (
            report.get("only_sorteio_requested", False)
            and int(report.get("sorteio_rows_matched", 0)) == 0
            and int(report["rows_after_filters"]) == 0
        ):
            raise RuntimeError(
                "rows_after_filters == 0 while only_sorteio was requested and filter matched 0; "
                "filter should have been skipped."
            )

        reference_path = clean_reference_dir / "ibge_municipios.parquet"
        if not reference_path.exists():
            raise FileNotFoundError(
                f"Missing IBGE reference file: {reference_path}. Run crosswalk module first."
            )
        reference_df = pd.read_parquet(reference_path)

        mapped, manual = _build_ibge_mapping(
            parsed,
            reference_df,
            threshold=92.0,
            reference_dir=clean_reference_dir,
        )

        rows_with_location_mask = (
            mapped["uf"].astype("string").str.len().fillna(0).astype(int).ge(2)
            & mapped["municipality_name_norm"].astype("string").str.strip().ne("")
        )
        rows_with_location = int(rows_with_location_mask.sum())
        mapped_rows = int(mapped.loc[rows_with_location_mask, "ibge_municipality_code"].notna().sum())
        coverage_rate = float(mapped_rows) / max(1, rows_with_location)
        report["ibge_mapping"] = {
            "coverage_rate": coverage_rate,
            "rows_with_location": rows_with_location,
            "mapped_rows": mapped_rows,
            "ibge_coverage_among_located": coverage_rate,
            "matched_counts": mapped["match_method"].value_counts(dropna=False).to_dict(),
            "manual_candidates_rows": int(len(manual)),
            "unique_municipalities": int(mapped[["municipality_name_clean", "uf"]].drop_duplicates().shape[0]),
        }

        if coverage_rate < 0.98:
            raise RuntimeError(
                "IBGE mapping coverage among located rows below threshold: "
                f"{coverage_rate:.4f} < 0.98. Review manual candidates."
            )

        write_parquet(mapped, mapped_path)
        manual.to_csv(manual_path, index=False)
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=mapped_path,
            status="ok",
            notes="auditorias_with_ibge",
        )
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=manual_path,
            status="ok",
            notes="manual_review_candidates",
        )

        lottery_path = clean_dir() / "cgu" / "lottery_municipalities_with_ibge.parquet"
        if not lottery_path.exists():
            raise FileNotFoundError(
                f"Missing lottery crosswalked data: {lottery_path}. Run crosswalk output first."
            )
        lottery_df = pd.read_parquet(lottery_path)

        events, long_df, link_stats = _link_lottery_and_audits(lottery_df, mapped)
        missing_optional_columns = list(link_stats.get("missing_optional_columns", []))
        report["missing_optional_columns"] = missing_optional_columns
        for col in missing_optional_columns:
            msg = f"Optional column missing in audits schema: {col}"
            report["warning_messages"].append(msg)
            report["warnings"].append(msg)

        if download_pdfs:
            pdf_url_by_task: dict[str, str] = {}
            pdf_sha_by_task: dict[str, str] = {}
            pdf_stats = Counter()
            for task_id in sorted({str(x).strip() for x in long_df["id_da_tarefa"].dropna().tolist() if str(x).strip()}):
                detail_url = f"https://eaud.cgu.gov.br/relatorio/{task_id}"
                html_path = raw_html_dir / f"{task_id}.html"
                try:
                    html_content, _meta = fetch_url(detail_url, timeout=timeout_seconds)
                    _save_fetch_bytes(html_path, html_content)
                    _manifest_row(
                        manifest_path,
                        item_type="downloaded",
                        source_url=detail_url,
                        local_path=html_path,
                        status="ok",
                        notes="eaud_detail_html",
                    )
                    html_text = html_content.decode("utf-8", errors="ignore")
                    links = _extract_pdf_links(html_text, detail_url)
                    if not links:
                        pdf_stats["no_pdf_link_found"] += 1
                        continue
                    pdf_url = links[0]
                    pdf_content, _pdf_meta = fetch_url(pdf_url, timeout=timeout_seconds)
                    pdf_path = ensure_dir(raw_pdf_dir / task_id) / f"{task_id}.pdf"
                    _save_fetch_bytes(pdf_path, pdf_content)
                    _manifest_row(
                        manifest_path,
                        item_type="downloaded",
                        source_url=pdf_url,
                        local_path=pdf_path,
                        status="ok",
                        notes="eaud_pdf",
                    )
                    pdf_url_by_task[task_id] = pdf_url
                    pdf_sha_by_task[task_id] = sha256_bytes(pdf_content)
                    pdf_stats["downloaded"] += 1
                except FetchUrlError as exc:
                    details = exc.metadata or {}
                    pdf_stats[f"fetch_error_status_{details.get('status_code', -1)}"] += 1
                    _manifest_row(
                        manifest_path,
                        item_type="downloaded",
                        source_url=detail_url,
                        local_path=html_path,
                        status="failed",
                        notes=f"eaud_detail_failed:{exc}",
                    )
                except Exception as exc:  # noqa: BLE001
                    pdf_stats["unexpected_error"] += 1
                    _manifest_row(
                        manifest_path,
                        item_type="downloaded",
                        source_url=detail_url,
                        local_path=html_path,
                        status="failed",
                        notes=f"eaud_detail_failed:{exc}",
                    )

            long_df["pdf_url"] = long_df["id_da_tarefa"].astype("string").map(lambda x: pdf_url_by_task.get(str(x), pd.NA))
            long_df["pdf_sha256"] = long_df["id_da_tarefa"].astype("string").map(lambda x: pdf_sha_by_task.get(str(x), pd.NA))
            report["pdf_download"] = dict(pdf_stats)
        else:
            long_df["pdf_url"] = pd.NA
            long_df["pdf_sha256"] = pd.NA

        long_desired_cols = [
            "ibge_municipality_code",
            "uf",
            "municipality_name_raw",
            "idtarefa",
            "idauditoria",
            "titulo",
            "publication_date",
            "grupoatividade",
            "linhaacao",
            "tiposervico",
            "fef",
            "edicao_programa_sorteio_fef",
            "eaud_relatorio_url",
            "eaud_relatorios_ui",
            "link_rule_used",
            "pdf_url",
            "pdf_sha256",
        ]
        long_present_cols = [c for c in long_desired_cols if c in long_df.columns]
        long_missing_cols = [c for c in long_desired_cols if c not in long_df.columns]
        if long_missing_cols:
            report["warning_messages"].append(
                f"audit_events_long missing optional selected columns: {', '.join(long_missing_cols)}"
            )
        long_df = long_df[long_present_cols].copy()

        events_desired_cols = [
            "ibge_municipality_code",
            "uf",
            "municipality_name",
            "municipality_name_raw",
            "lottery_round_ids",
            "lottery_round_labels",
            "first_lottery_round",
            "audit_task_id",
            "audit_publication_date",
            "audit_title",
            "audit_audit_id",
            "grupoatividade",
            "linhaacao",
            "tiposervico",
            "fef",
            "edition_program_sorteio_fef",
            "link_rule_used",
        ]
        events_present_cols = [c for c in events_desired_cols if c in events.columns]
        events = events[events_present_cols].copy()

        write_parquet(events, events_path)
        write_parquet(long_df, long_path)
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=events_path,
            status="ok",
            notes="audit_events_wide",
        )
        _manifest_row(
            manifest_path,
            item_type="generated",
            source_url="generated",
            local_path=long_path,
            status="ok",
            notes="audit_events_long",
        )

        report["audit_events_long_rows"] = int(len(long_df))
        report["audit_events_rows"] = int(len(events))
        report["linking"] = link_stats
        coverage_for_success = float(
            report.get("ibge_mapping", {}).get("ibge_coverage_among_located", 0.0)
        )
        report["success"] = bool(
            int(report.get("rows_after_filters", 0)) > 0
            and coverage_for_success >= 0.98
            and int(report.get("audit_events_long_rows", 0)) > 0
        )
        if not report["success"]:
            report["warning_messages"].append(
                "Success criteria not met: requires rows_after_filters>0, "
                "ibge_coverage_among_located>=0.98, and audit_events_long_rows>0."
            )
        return bool(report["success"]), report
    except Exception as exc:  # noqa: BLE001
        logger.exception("CGU open data auditorias ingestion failed: {}", exc)
        report["errors"].append(str(exc))
        report["success"] = False
        return False, report
    finally:
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


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest CGU Auditorias open data and link to lottery municipalities.")
    parser.add_argument("--force-download", action="store_true", help="Re-download Auditorias.csv")
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild outputs even if they exist")
    parser.add_argument(
        "--only-sorteio",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep only records tagged as Sorteio/FEF",
    )
    parser.add_argument(
        "--download-pdfs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Attempt detail-page PDF discovery/download",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional row cap for debug")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--log-level", default=get_settings().log_level, help="Log level (INFO, DEBUG, etc)")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    ok, report = run_pipeline(
        force_download=bool(args.force_download),
        force_rebuild=bool(args.force_rebuild),
        only_sorteio=bool(args.only_sorteio),
        download_pdfs=bool(args.download_pdfs),
        max_rows=args.max_rows,
        timeout_seconds=int(args.timeout_seconds),
        log_level=str(args.log_level),
    )

    mapping = report.get("ibge_mapping", {})
    coverage = float(mapping.get("ibge_coverage_among_located", mapping.get("coverage_rate", 0.0)))
    print(
        "CGU open-data auditorias summary | "
        f"success={report.get('success', False)} "
        f"rows_raw={report.get('csv_rows_raw', 0)} "
        f"rows_filtered={report.get('rows_after_filters', 0)} "
        f"ibge_coverage={coverage:.4f}"
    )
    print(f"Report: {report.get('artifacts', {}).get('report_json', '')}")
    print(f"Latest: {report.get('artifacts', {}).get('latest_json', '')}")
    print(f"Manifest: {report.get('artifacts', {}).get('manifest', '')}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
