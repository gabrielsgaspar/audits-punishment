"""Build and apply IBGE municipality crosswalk to CGU lottery data."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger
from rapidfuzz import fuzz, process
from rapidfuzz.distance import Levenshtein

from audits_punishment.config import get_settings
from audits_punishment.logging import setup_logging
from audits_punishment.paths import clean_dir, interim_dir, raw_dir
from audits_punishment.utils.http import fetch_url
from audits_punishment.utils.io import atomic_write_bytes, ensure_dir, sha256_file, write_parquet

# Official IBGE Localidades API municipalities endpoint used as reference source.
IBGE_REFERENCE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _manifest_upsert(manifest_path: Path, row: dict[str, Any]) -> None:
    if manifest_path.exists():
        df = pd.read_parquet(manifest_path)
    else:
        df = pd.DataFrame(columns=row.keys())
    incoming = pd.DataFrame([row])
    for col in incoming.columns:
        if col not in df.columns:
            df[col] = pd.NA
    for col in df.columns:
        if col not in incoming.columns:
            incoming[col] = pd.NA
    merged = pd.concat([df[incoming.columns], incoming], ignore_index=True)
    merged = merged.drop_duplicates(subset=["local_path", "item_type"], keep="last")
    write_parquet(merged, manifest_path)


def as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def normalize_name(name: str) -> str:
    value = (name or "").upper().strip()
    value = re.sub(r"^[\s\-\–\—\•\*]+", "", value)
    value = value.replace("-", " ").replace("/", " ")
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\bD\s+", "D ", value)
    return value


def download_reference_table(raw_reference_path: Path, force_download: bool) -> tuple[Path, str]:
    settings = get_settings()
    if raw_reference_path.exists() and not force_download:
        return raw_reference_path, IBGE_REFERENCE_URL
    content, _meta = fetch_url(IBGE_REFERENCE_URL, headers={"User-Agent": settings.user_agent})
    atomic_write_bytes(raw_reference_path, content)
    return raw_reference_path, IBGE_REFERENCE_URL


def _extract_uf_sigla(item: dict[str, Any]) -> str:
    paths = [
        ("microrregiao", "mesorregiao", "UF", "sigla"),
        ("regiao-imediata", "regiao-intermediaria", "UF", "sigla"),
        ("regiao_imediata", "regiao_intermediaria", "UF", "sigla"),
        ("UF", "sigla"),
        ("uf", "sigla"),
    ]
    for path in paths:
        cur: Any = item
        for key in path[:-1]:
            cur = as_dict(cur).get(key)
        value = as_dict(cur).get(path[-1])
        if isinstance(value, str):
            uf = value.strip().upper()
            if len(uf) == 2:
                return uf
    sigla_uf = item.get("siglaUF")
    if isinstance(sigla_uf, str) and len(sigla_uf.strip()) == 2:
        return sigla_uf.strip().upper()
    return ""


def parse_reference_payload(payload: list[dict[str, Any]], source_url: str) -> tuple[pd.DataFrame, dict[str, int]]:
    rows: list[dict[str, str]] = []
    pulled_at = _utc_now_iso()
    skipped_missing_uf = 0
    total_records = 0
    for item in payload:
        total_records += 1
        item = as_dict(item)
        code = str(item.get("id", "")).zfill(7)
        name = str(item.get("nome", "")).strip()
        uf = _extract_uf_sigla(item)
        if not uf:
            skipped_missing_uf += 1
            continue
        if len(code) != 7 or not name:
            continue
        rows.append(
            {
                "ibge_municipality_code": code,
                "municipality_name_official": name,
                "uf": uf,
                "municipality_name_norm": normalize_name(name),
                "source_url": source_url,
                "pulled_at_utc": pulled_at,
            }
        )
    df = pd.DataFrame(rows).drop_duplicates(subset=["ibge_municipality_code"])
    stats = {
        "total_records": total_records,
        "parsed_ok": int(len(df)),
        "skipped_missing_uf": skipped_missing_uf,
    }
    return df, stats


def load_reference_table(raw_reference_path: Path, source_url: str) -> tuple[pd.DataFrame, dict[str, int]]:
    payload = json.loads(raw_reference_path.read_text(encoding="utf-8"))
    return parse_reference_payload(payload, source_url)


def _append_note(existing: str, note: str) -> str:
    if not note:
        return existing
    if not existing:
        return note
    return f"{existing}; {note}"


def _to_key_text(value: Any) -> str:
    return str(value or "").strip()


def load_alias_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["uf", "name_norm_from", "name_norm_to", "note", "source"])
    df = pd.read_csv(path)
    for col in ["uf", "name_norm_from", "name_norm_to", "note", "source"]:
        if col not in df.columns:
            df[col] = ""
    df["uf"] = df["uf"].astype(str).str.upper().str.strip()
    df["name_norm_from"] = df["name_norm_from"].astype(str).map(normalize_name)
    df["name_norm_to"] = df["name_norm_to"].astype(str).map(normalize_name)
    return df


def load_override_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["uf", "municipality_name_clean", "ibge_municipality_code", "note"])
    df = pd.read_csv(path)
    for col in ["uf", "municipality_name_clean", "ibge_municipality_code", "note"]:
        if col not in df.columns:
            df[col] = ""
    df["uf"] = df["uf"].astype(str).str.upper().str.strip()
    df["municipality_name_clean"] = df["municipality_name_clean"].astype(str).map(_to_key_text)
    df["ibge_municipality_code"] = df["ibge_municipality_code"].astype(str).str.zfill(7)
    return df


def build_crosswalk(
    lottery_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    threshold: float,
    alias_df: pd.DataFrame | None = None,
    override_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = lottery_df.copy()
    df["uf"] = df["uf"].astype(str).str.upper().str.strip()
    df["municipality_name_clean"] = df["municipality_name_clean"].astype(str).map(_to_key_text)
    df["name_exact"] = df["municipality_name_clean"].astype(str).str.upper().str.strip()
    df["name_norm_raw"] = df["municipality_name_clean"].astype(str).map(normalize_name)
    df["name_norm"] = df["name_norm_raw"]
    df["alias_applied"] = False
    ref = reference_df.copy()
    ref["name_exact"] = ref["municipality_name_official"].astype(str).str.upper().str.strip()
    ref["ibge_municipality_code"] = ref["ibge_municipality_code"].astype(str).str.zfill(7)

    code_to_name = dict(zip(ref["ibge_municipality_code"], ref["municipality_name_official"]))
    exact_map = {
        (row["uf"], row["name_exact"]): (row["ibge_municipality_code"], row["municipality_name_official"])
        for _, row in ref.iterrows()
    }
    norm_map = {
        (row["uf"], row["municipality_name_norm"]): (
            row["ibge_municipality_code"],
            row["municipality_name_official"],
        )
        for _, row in ref.iterrows()
    }
    alias_map: dict[tuple[str, str], tuple[str, str]] = {}
    if alias_df is not None and not alias_df.empty:
        for _, row in alias_df.iterrows():
            alias_map[(str(row["uf"]).upper().strip(), normalize_name(str(row["name_norm_from"])))] = (
                normalize_name(str(row["name_norm_to"])),
                str(row.get("note", "")),
            )
    override_map: dict[tuple[str, str], tuple[str, str]] = {}
    if override_df is not None and not override_df.empty:
        for _, row in override_df.iterrows():
            override_map[(str(row["uf"]).upper().strip(), _to_key_text(row["municipality_name_clean"]))] = (
                str(row["ibge_municipality_code"]).zfill(7),
                str(row.get("note", "")),
            )

    merged = df.copy()
    merged["ibge_municipality_code"] = pd.NA
    merged["match_method"] = "unmatched"
    merged["match_score"] = 0.0
    merged["matched_name_official"] = pd.NA
    merged["match_notes"] = ""

    # Alias stage before matching.
    for idx, row in merged.iterrows():
        key = (row["uf"], row["name_norm"])
        alias_hit = alias_map.get(key)
        if alias_hit is None:
            continue
        before = row["name_norm"]
        after = alias_hit[0]
        merged.at[idx, "name_norm"] = after
        merged.at[idx, "alias_applied"] = True
        merged.at[idx, "match_notes"] = _append_note(
            str(merged.at[idx, "match_notes"]),
            f"alias_applied:{before}->{after}",
        )

    # Override stage first.
    for idx, row in merged.iterrows():
        ov = override_map.get((row["uf"], row["municipality_name_clean"]))
        if ov is None:
            continue
        code, note = ov
        merged.at[idx, "ibge_municipality_code"] = code
        merged.at[idx, "matched_name_official"] = code_to_name.get(code, pd.NA)
        merged.at[idx, "match_method"] = "override"
        merged.at[idx, "match_score"] = 1.0
        merged.at[idx, "match_notes"] = _append_note(
            str(merged.at[idx, "match_notes"]),
            f"override_applied:{note}" if note else "override_applied",
        )

    # Exact name stage.
    for idx, row in merged.loc[merged["ibge_municipality_code"].isna()].iterrows():
        hit = exact_map.get((row["uf"], row["name_exact"]))
        if hit is None:
            continue
        merged.at[idx, "ibge_municipality_code"] = hit[0]
        merged.at[idx, "matched_name_official"] = hit[1]
        merged.at[idx, "match_method"] = "exact"
        merged.at[idx, "match_score"] = 1.0

    # Normalized exact stage.
    for idx, row in merged.loc[merged["ibge_municipality_code"].isna()].iterrows():
        hit = norm_map.get((row["uf"], row["name_norm"]))
        if hit is None:
            continue
        merged.at[idx, "ibge_municipality_code"] = hit[0]
        merged.at[idx, "matched_name_official"] = hit[1]
        merged.at[idx, "match_method"] = "normalized_exact"
        merged.at[idx, "match_score"] = 1.0

    manual_rows: list[dict[str, Any]] = []
    still_unresolved = merged["ibge_municipality_code"].isna()
    for idx, row in merged.loc[still_unresolved].iterrows():
        uf = row["uf"]
        pool = ref.loc[ref["uf"] == uf, ["ibge_municipality_code", "municipality_name_official", "municipality_name_norm"]]
        if pool.empty:
            merged.at[idx, "match_method"] = "unmatched"
            merged.at[idx, "match_notes"] = "no_reference_for_uf"
            continue
        choices = pool["municipality_name_norm"].tolist()
        scored = process.extract(row["name_norm"], choices, scorer=fuzz.WRatio, limit=5)
        if not scored:
            merged.at[idx, "match_method"] = "unmatched"
            merged.at[idx, "match_notes"] = "no_candidates"
            continue
        best_name, best_score, _ = scored[0]
        second_score = scored[1][1] if len(scored) > 1 else 0.0
        best_row = pool.loc[pool["municipality_name_norm"] == best_name].iloc[0]
        edit_distance = Levenshtein.distance(str(row["name_norm"]), str(best_name))
        strict_ok = best_score >= threshold
        relaxed_ok = best_score >= 85 and (best_score - second_score) >= 10 and edit_distance <= 2
        if strict_ok:
            merged.at[idx, "ibge_municipality_code"] = best_row["ibge_municipality_code"]
            merged.at[idx, "matched_name_official"] = best_row["municipality_name_official"]
            merged.at[idx, "match_method"] = "fuzzy_strict"
            merged.at[idx, "match_score"] = float(best_score) / 100.0
            merged.at[idx, "match_notes"] = _append_note(
                str(merged.at[idx, "match_notes"]),
                f"fuzzy_strict:score={best_score:.2f}",
            )
        elif relaxed_ok:
            merged.at[idx, "ibge_municipality_code"] = best_row["ibge_municipality_code"]
            merged.at[idx, "matched_name_official"] = best_row["municipality_name_official"]
            merged.at[idx, "match_method"] = "fuzzy_relaxed"
            merged.at[idx, "match_score"] = float(best_score) / 100.0
            merged.at[idx, "match_notes"] = _append_note(
                str(merged.at[idx, "match_notes"]),
                f"fuzzy_relaxed:score={best_score:.2f},margin={(best_score-second_score):.2f},edit_distance={edit_distance}",
            )
        else:
            merged.at[idx, "match_method"] = "manual_needed"
            merged.at[idx, "match_score"] = float(best_score) / 100.0
            merged.at[idx, "match_notes"] = _append_note(
                str(merged.at[idx, "match_notes"]),
                "below_threshold_or_low_margin_or_edit_distance",
            )
            for rank, (cand_name, cand_score, _) in enumerate(scored, start=1):
                cand_row = pool.loc[pool["municipality_name_norm"] == cand_name].iloc[0]
                manual_rows.append(
                    {
                        "municipality_name_clean": row["municipality_name_clean"],
                        "uf": uf,
                        "suggested_ibge_code": cand_row["ibge_municipality_code"],
                        "suggested_name_official": cand_row["municipality_name_official"],
                        "score": float(cand_score),
                        "rank": rank,
                    }
                )

    merged = merged.drop(columns=["name_exact", "name_norm_raw", "name_norm"], errors="ignore")
    merged["ibge_municipality_code"] = merged["ibge_municipality_code"].astype("string")
    return merged, pd.DataFrame(manual_rows)


def write_report(report_path: Path, payload: dict[str, Any]) -> None:
    atomic_write_bytes(report_path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def ensure_seed_reference_files(reference_dir: Path) -> tuple[Path, Path]:
    alias_path = reference_dir / "municipality_name_aliases.csv"
    override_path = reference_dir / "ibge_crosswalk_overrides.csv"
    if not alias_path.exists():
        pd.DataFrame(
            [
                {
                    "uf": "TO",
                    "name_norm_from": "SAO VALERIO DA NATIVIDADE",
                    "name_norm_to": "SAO VALERIO",
                    "note": "historic name",
                    "source": "project_seed",
                }
            ]
        ).to_csv(alias_path, index=False)
    if not override_path.exists():
        pd.DataFrame(
            [
                {
                    "uf": "BA",
                    "municipality_name_clean": "- Boninau",
                    "ibge_municipality_code": "2904001",
                    "note": "CGU list typo/formatting; intended Boninal",
                }
            ]
        ).to_csv(override_path, index=False)
    return alias_path, override_path


def apply_crosswalk_to_lottery(
    threshold: float,
    force_download: bool,
    force_rebuild: bool,
    expect_zero_manual: bool = False,
) -> tuple[bool, dict[str, Any]]:
    raw_reference_dir = ensure_dir(raw_dir() / "reference")
    clean_reference_dir = ensure_dir(clean_dir() / "reference")
    clean_cgu_dir = ensure_dir(clean_dir() / "cgu")
    reports_dir = ensure_dir(interim_dir() / "reports" / "ibge_crosswalk")
    manifests_dir = ensure_dir(interim_dir() / "manifests")

    raw_reference_path = raw_reference_dir / "ibge_municipios.json"
    clean_reference_path = clean_reference_dir / "ibge_municipios.parquet"
    lottery_input_path = interim_dir() / "cgu" / "lottery_lists" / "lottery_municipalities.parquet"
    lottery_output_path = clean_cgu_dir / "lottery_municipalities_with_ibge.parquet"
    manual_review_path = reports_dir / "manual_review_candidates.csv"
    report_path = reports_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_crosswalk_report.json"
    manifest_path = manifests_dir / "ibge_crosswalk_manifest.parquet"

    if not lottery_input_path.exists():
        raise FileNotFoundError(f"Missing input file: {lottery_input_path}")
    if lottery_output_path.exists() and not force_rebuild:
        logger.info("Output already exists and --force-rebuild not set: {}", lottery_output_path)
        existing = pd.read_parquet(lottery_output_path)
        counts = (
            existing["match_method"].value_counts(dropna=False).to_dict()
            if "match_method" in existing.columns
            else {}
        )
        return True, {
            "coverage_rate": float((existing["ibge_municipality_code"].notna().sum()) / max(len(existing), 1))
            if "ibge_municipality_code" in existing.columns
            else 0.0,
            "matched_exact": int(counts.get("exact", 0)),
            "matched_normalized_exact": int(counts.get("normalized_exact", 0)),
            "matched_override": int(counts.get("override", 0)),
            "matched_fuzzy": int(counts.get("fuzzy_strict", 0) + counts.get("fuzzy_relaxed", 0)),
            "matched_fuzzy_relaxed": int(counts.get("fuzzy_relaxed", 0)),
            "matched_alias_applied": int(
                existing.loc[existing["ibge_municipality_code"].notna(), "match_notes"]
                .astype(str)
                .str.contains("alias_applied:")
                .sum()
            )
            if "match_notes" in existing.columns
            else 0,
            "manual_needed": int(counts.get("manual_needed", 0)),
            "unmatched": int(counts.get("unmatched", 0)),
        }
    ref_path, source_url = download_reference_table(raw_reference_path, force_download=force_download)
    _manifest_upsert(
        manifest_path,
        {
            "item_type": "downloaded",
            "source_url": source_url,
            "local_path": str(ref_path),
            "sha256": sha256_file(ref_path),
            "size_bytes": ref_path.stat().st_size,
            "created_at_utc": _utc_now_iso(),
            "status": "ok",
            "notes": "ibge_municipios_reference",
        },
    )

    reference_df, reference_stats = load_reference_table(ref_path, source_url)
    logger.info(
        "IBGE reference parse stats | total_records={} parsed_ok={} skipped_missing_uf={}",
        reference_stats["total_records"],
        reference_stats["parsed_ok"],
        reference_stats["skipped_missing_uf"],
    )
    if reference_stats["skipped_missing_uf"] > 0:
        logger.warning(
            "IBGE reference had records without UF and they were skipped: {}",
            reference_stats["skipped_missing_uf"],
        )
    if reference_stats["parsed_ok"] < 5000:
        raise RuntimeError(
            "IBGE reference parsing produced fewer than 5000 municipalities; "
            "reference source/structure may have changed."
        )
    alias_path, override_path = ensure_seed_reference_files(clean_reference_dir)
    alias_df = load_alias_table(alias_path)
    override_df = load_override_table(override_path)
    write_parquet(reference_df, clean_reference_path)
    _manifest_upsert(
        manifest_path,
        {
            "item_type": "generated",
            "source_url": source_url,
            "local_path": str(clean_reference_path),
            "sha256": sha256_file(clean_reference_path),
            "size_bytes": clean_reference_path.stat().st_size,
            "created_at_utc": _utc_now_iso(),
            "status": "ok",
            "notes": "normalized_ibge_reference",
        },
    )

    lottery_df = pd.read_parquet(lottery_input_path)
    result_df, manual_df = build_crosswalk(
        lottery_df,
        reference_df,
        threshold=threshold,
        alias_df=alias_df,
        override_df=override_df,
    )
    write_parquet(result_df, lottery_output_path)
    manual_df.to_csv(manual_review_path, index=False)

    for path, note in [(lottery_output_path, "lottery_with_ibge"), (manual_review_path, "manual_candidates")]:
        _manifest_upsert(
            manifest_path,
            {
                "item_type": "generated",
                "source_url": "generated",
                "local_path": str(path),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
                "created_at_utc": _utc_now_iso(),
                "status": "ok",
                "notes": note,
            },
        )

    counts = result_df["match_method"].value_counts(dropna=False).to_dict()
    matched_alias_applied = int(
        result_df.loc[result_df["ibge_municipality_code"].notna(), "match_notes"]
        .astype(str)
        .str.contains("alias_applied:")
        .sum()
    )
    matched_override = int(counts.get("override", 0))
    matched_fuzzy_relaxed = int(counts.get("fuzzy_relaxed", 0))
    matched_fuzzy_total = int(counts.get("fuzzy_strict", 0) + counts.get("fuzzy_relaxed", 0))
    manual_needed_count = int(counts.get("manual_needed", 0))
    unmatched_count = int(counts.get("unmatched", 0))
    unmatched_top = (
        result_df.loc[result_df["match_method"].isin(["manual_needed", "unmatched"])]
        .groupby(["municipality_name_clean", "uf"], dropna=False)
        .size()
        .sort_values(ascending=False)
        .head(30)
        .reset_index(name="count")
        .to_dict(orient="records")
    )
    report_payload = {
        "reference_parse_stats": reference_stats,
        "total_rows_input": int(len(result_df)),
        "matched_override": matched_override,
        "matched_alias_applied": matched_alias_applied,
        "matched_exact": int(counts.get("exact", 0)),
        "matched_normalized_exact": int(counts.get("normalized_exact", 0)),
        "matched_fuzzy": matched_fuzzy_total,
        "matched_fuzzy_relaxed": matched_fuzzy_relaxed,
        "manual_needed": manual_needed_count,
        "unmatched": unmatched_count,
        "coverage_rate": float((result_df["ibge_municipality_code"].notna().sum()) / max(len(result_df), 1)),
        "number_of_unique_municipalities": int(result_df[["municipality_name_clean", "uf"]].drop_duplicates().shape[0]),
        "top_unmatched_names": unmatched_top,
        "artifacts": {
            "reference_parquet": str(clean_reference_path),
            "output_parquet": str(lottery_output_path),
            "manual_review_candidates": str(manual_review_path),
            "manifest": str(manifest_path),
            "report_json": str(report_path),
            "alias_file": str(alias_path),
            "override_file": str(override_path),
        },
    }
    write_report(report_path, report_payload)
    _manifest_upsert(
        manifest_path,
        {
            "item_type": "generated",
            "source_url": "generated",
            "local_path": str(report_path),
            "sha256": sha256_file(report_path),
            "size_bytes": report_path.stat().st_size,
            "created_at_utc": _utc_now_iso(),
            "status": "ok",
            "notes": "crosswalk_report",
        },
    )
    if manual_needed_count > 0 and unmatched_count == 0:
        logger.warning(
            "manual_needed > 0 while unmatched == 0; check manual_review_candidates.csv for deterministic cleanup"
        )
    if expect_zero_manual and manual_needed_count > 0:
        logger.error(
            "--expect-zero-manual was set but manual_needed is {}",
            manual_needed_count,
        )
        return False, report_payload
    return True, report_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and apply IBGE municipality crosswalk.")
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--threshold", type=float, default=92.0)
    parser.add_argument("--log-level", type=str, default=None)
    parser.add_argument("--expect-zero-manual", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(args.log_level or settings.log_level)
    try:
        ok, report = apply_crosswalk_to_lottery(
            threshold=float(args.threshold),
            force_download=bool(args.force_download),
            force_rebuild=bool(args.force_rebuild),
            expect_zero_manual=bool(args.expect_zero_manual),
        )
        print(
            "IBGE crosswalk summary | "
            f"coverage={report['coverage_rate']:.3f} "
            f"override={report['matched_override']} "
            f"alias_applied={report['matched_alias_applied']} "
            f"exact={report['matched_exact']} "
            f"norm_exact={report['matched_normalized_exact']} "
            f"fuzzy={report['matched_fuzzy']} "
            f"fuzzy_relaxed={report['matched_fuzzy_relaxed']} "
            f"manual_needed={report['manual_needed']} "
            f"unmatched={report['unmatched']}"
        )
        if "artifacts" in report:
            print(f"Output: {report['artifacts'].get('output_parquet', '')}")
            print(f"Manual review: {report['artifacts'].get('manual_review_candidates', '')}")
            print(f"Manifest: {report['artifacts'].get('manifest', '')}")
            print(f"Report: {report['artifacts'].get('report_json', '')}")
        raise SystemExit(0 if ok else 1)
    except Exception as exc:  # noqa: BLE001
        logger.exception("IBGE crosswalk failed: {}", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
