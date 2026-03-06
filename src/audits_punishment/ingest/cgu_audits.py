"""Discover and index CGU audit report candidates (MVP, diagnostics-first)."""

from __future__ import annotations

import argparse
import json
import re
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from audits_punishment.build.build_manifests import upsert_manifest_row
from audits_punishment.config import get_settings
from audits_punishment.logging import setup_logging
from audits_punishment.paths import clean_dir, interim_dir, raw_dir
from audits_punishment.utils.http import fetch_url
from audits_punishment.utils.io import atomic_write_bytes, ensure_dir, sha256_bytes, sha256_file, write_parquet
from audits_punishment.utils.text import normalize_municipality_query, normalize_whitespace, slugify

GOVBR_ENTRY_URL = "https://www.gov.br/gestao/pt-br/acesso-a-informacao/auditorias/relatorios-da-cgu"
AUDITORIA_ENTRY_URL = "https://auditoria.cgu.gov.br/"
API_ENDPOINT_CACHE_FILE = "eaud_api_endpoint.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash_id(text: str, prefix: str = "id") -> str:
    return f"{prefix}_{sha256_bytes(text.encode('utf-8'))[:16]}"


def _base_origin(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _extract_api_paths(js_text: str) -> list[str]:
    found = set(re.findall(r'"/api/[^"]+"', js_text) + re.findall(r"'/api/[^']+'", js_text))
    return sorted(x.strip("\"'") for x in found)


def _parse_json_candidates(node: Any) -> list[dict[str, Any]]:
    if isinstance(node, list):
        out: list[dict[str, Any]] = []
        for x in node:
            if isinstance(x, dict):
                out.append(x)
            else:
                out.extend(_parse_json_candidates(x))
        return out
    if isinstance(node, dict):
        out: list[dict[str, Any]] = []
        for key in ["items", "resultados", "results", "data", "content", "relatorios"]:
            if key in node:
                out.extend(_parse_json_candidates(node[key]))
        if {"id", "titulo"} & set(node.keys()):
            out.append(node)
        return out
    return []


def _write_df_with_fallback(df: pd.DataFrame, parquet_path: Path) -> Path:
    try:
        write_parquet(df, parquet_path)
        return parquet_path
    except Exception:
        csv_path = parquet_path.with_suffix(".csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        return csv_path


def write_endpoint_cache(cache_path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(cache_path.parent)
    atomic_write_bytes(cache_path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


@dataclass(slots=True)
class ReportCandidate:
    candidate_id: str
    municipality_name: str
    uf: str
    ibge_municipality_code: str
    source_strategy: str
    landing_url: str
    pdf_url: str | None
    title_text: str | None
    date_text: str | None
    notes: str
    confidence: str
    discovered_at_utc: str
    normalized_muni_name: str
    query_terms_used: str


@dataclass(slots=True)
class StrategyOutput:
    candidates: list[ReportCandidate]
    failures: list[str]


@dataclass(slots=True)
class FetchAttempt:
    run_id: str
    strategy: str
    municipality_ibge: str
    url: str
    status_code: int
    content_type: str
    final_url: str
    bytes_len: int
    elapsed_ms: float
    ok: bool
    error: str


@dataclass(slots=True)
class DiscoveryContext:
    run_id: str
    force: bool
    sleep_seconds: float
    max_pages_per_muni: int
    debug: bool
    endpoint_cache_path: Path
    raw_html_dir: Path
    raw_json_dir: Path
    raw_pdf_dir: Path
    manifest_path: Path
    fetch_attempts: list[FetchAttempt]
    debug_snippets: list[dict[str, str]]

    def fetch_bytes(
        self,
        *,
        strategy: str,
        ibge: str,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> tuple[bytes, dict[str, Any]]:
        t0 = perf_counter()
        try:
            content, meta = fetch_url(
                url,
                method=method,
                sleep_seconds=self.sleep_seconds,
                params=params,
                json_body=json_body,
            )
            elapsed = (perf_counter() - t0) * 1000.0
            self.fetch_attempts.append(
                FetchAttempt(
                    run_id=self.run_id,
                    strategy=strategy,
                    municipality_ibge=ibge,
                    url=url,
                    status_code=int(meta.get("status_code", 0)),
                    content_type=str(meta.get("content_type", "")),
                    final_url=str(meta.get("final_url", url)),
                    bytes_len=len(content),
                    elapsed_ms=elapsed,
                    ok=True,
                    error="",
                )
            )
            if self.debug and len(self.debug_snippets) < 5:
                snippet = content[:2048].decode("utf-8", errors="ignore")
                self.debug_snippets.append({"url": str(meta.get("final_url", url)), "snippet": snippet})
            return content, meta
        except Exception as exc:  # noqa: BLE001
            elapsed = (perf_counter() - t0) * 1000.0
            self.fetch_attempts.append(
                FetchAttempt(
                    run_id=self.run_id,
                    strategy=strategy,
                    municipality_ibge=ibge,
                    url=url,
                    status_code=0,
                    content_type="",
                    final_url=url,
                    bytes_len=0,
                    elapsed_ms=elapsed,
                    ok=False,
                    error=str(exc),
                )
            )
            raise

    def snapshot_html(self, *, strategy: str, ibge: str, muni_slug: str, url: str) -> tuple[str, str]:
        now_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        url_hash = sha256_bytes(url.encode("utf-8"))[:12]
        strategy_dir = ensure_dir(self.raw_html_dir / strategy)
        stable = strategy_dir / f"{ibge}_{muni_slug}_{url_hash}_latest.html"
        stamped = strategy_dir / f"{ibge}_{muni_slug}_{url_hash}_{now_token}.html"
        if stable.exists() and not self.force:
            content = stable.read_bytes()
            final_url = url
            source = "cache"
        else:
            content, meta = self.fetch_bytes(strategy=strategy, ibge=ibge, url=url)
            final_url = str(meta.get("final_url", url))
            atomic_write_bytes(stable, content)
            source = "network"
        atomic_write_bytes(stamped, content)
        upsert_manifest_row(
            self.manifest_path,
            {
                "item_type": "downloaded",
                "source_url": url,
                "local_path": str(stamped),
                "sha256": sha256_bytes(content),
                "size_bytes": len(content),
                "created_at_utc": _utc_now_iso(),
                "status": "ok",
                "notes": f"html_snapshot:{strategy}:{source}",
            },
            key_cols=["local_path", "item_type"],
        )
        return content.decode("utf-8", errors="ignore"), final_url

    def snapshot_json(self, *, ibge: str, query_hash: str, source_url: str, content: bytes) -> Path:
        out_dir = ensure_dir(self.raw_json_dir / ibge)
        path = out_dir / f"{query_hash}.json"
        atomic_write_bytes(path, content)
        upsert_manifest_row(
            self.manifest_path,
            {
                "item_type": "downloaded",
                "source_url": source_url,
                "local_path": str(path),
                "sha256": sha256_bytes(content),
                "size_bytes": len(content),
                "created_at_utc": _utc_now_iso(),
                "status": "ok",
                "notes": "json_search_response",
            },
            key_cols=["local_path", "item_type"],
        )
        return path


class AuditDiscoveryStrategy:
    name: str

    def discover_reports(self, ctx: DiscoveryContext, muni_name: str, uf: str, ibge: str) -> StrategyOutput:
        raise NotImplementedError

def _parse_landing_html_for_pdf(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    out: list[str] = []
    for anchor in soup.find_all("a", href=True):
        full = urljoin(base_url, anchor.get("href", ""))
        if full.lower().endswith(".pdf") or ("/api/" in full.lower() and "download" in full.lower()):
            out.append(full)
    return list(dict.fromkeys(out))


def parse_candidates_from_json_items(
    *,
    items: list[dict[str, Any]],
    base_url: str,
    strategy_name: str,
    municipality_name: str,
    uf: str,
    ibge: str,
    query_terms_used: str,
) -> list[ReportCandidate]:
    muni_norm = normalize_municipality_query(municipality_name)
    out: list[ReportCandidate] = []
    for item in items:
        rid = str(item.get("id") or item.get("relatorioId") or item.get("codigo") or "").strip()
        title = str(item.get("titulo") or item.get("relatorio") or item.get("nome") or "").strip() or None
        date_text = str(item.get("dataPublicacao") or item.get("data") or "").strip() or None
        pdf_val = item.get("pdfUrl") or item.get("urlPdf")
        pdf_url = urljoin(base_url, str(pdf_val)) if pdf_val else None
        landing = item.get("landingUrl") or item.get("url") or (f"{base_url}/relatorio/{rid}" if rid else base_url)
        landing = urljoin(base_url, str(landing))
        title_norm = normalize_municipality_query(str(title or ""))
        confidence = "high" if muni_norm and muni_norm in title_norm else "medium"
        cid = _hash_id(f"{landing}|{pdf_url}|{municipality_name}|{strategy_name}", prefix="cand")
        out.append(
            ReportCandidate(
                candidate_id=cid,
                municipality_name=municipality_name,
                uf=uf,
                ibge_municipality_code=ibge,
                source_strategy=strategy_name,
                landing_url=landing,
                pdf_url=pdf_url,
                title_text=title,
                date_text=date_text,
                notes="json_result",
                confidence=confidence,
                discovered_at_utc=_utc_now_iso(),
                normalized_muni_name=muni_norm,
                query_terms_used=query_terms_used,
            )
        )
    return out


class EaudRelatoriosApiStrategy(AuditDiscoveryStrategy):
    name = "eaud_api"

    def discover_api_endpoint(self, ctx: DiscoveryContext, ibge: str, base_url: str) -> dict[str, Any] | None:
        attempted: list[str] = []
        if ctx.endpoint_cache_path.exists() and not ctx.force:
            try:
                cached = json.loads(ctx.endpoint_cache_path.read_text(encoding="utf-8"))
                if cached.get("status") == "found":
                    return cached
            except Exception:
                pass

        pages = [f"{base_url}/relatorios", f"{base_url}/relatorios/pesquisa"]
        js_urls: set[str] = {
            f"{base_url}/static/relatorios/js/eaud-relatorios.min.js",
            f"{base_url}/static/relatorios/js/libs.js",
        }
        api_paths: set[str] = set()

        for page in pages:
            attempted.append(page)
            try:
                html, final_url = ctx.snapshot_html(strategy=self.name, ibge=ibge, muni_slug="bootstrap", url=page)
            except Exception:
                continue
            soup = BeautifulSoup(html, "lxml")
            for script in soup.find_all("script", src=True):
                js_urls.add(urljoin(final_url, script.get("src", "")))

        for js_url in sorted(js_urls):
            attempted.append(js_url)
            try:
                content, _ = ctx.fetch_bytes(strategy=self.name, ibge=ibge, url=js_url)
                api_paths.update(_extract_api_paths(content.decode("utf-8", errors="ignore")))
            except Exception:
                continue

        param_keys = ["q", "texto", "termo", "search", "textoPesquisa"]
        for path in sorted(api_paths):
            endpoint = urljoin(base_url, path)
            for key in param_keys:
                attempted.append(f"GET {endpoint}?{key}=...")
                try:
                    content, meta = ctx.fetch_bytes(
                        strategy=self.name,
                        ibge=ibge,
                        url=endpoint,
                        method="GET",
                        params={key: "ITAPAGE CE"},
                    )
                    if "json" not in str(meta.get("content_type", "")).lower():
                        continue
                    parsed = json.loads(content.decode("utf-8", errors="ignore"))
                    if _parse_json_candidates(parsed):
                        payload = {
                            "run_id": ctx.run_id,
                            "status": "found",
                            "base_url": base_url,
                            "endpoint": endpoint,
                            "method": "GET",
                            "params_schema_guess": {"query_param": key},
                            "attempted": attempted[:200],
                        }
                        write_endpoint_cache(ctx.endpoint_cache_path, payload)
                        return payload
                except Exception:
                    pass

                attempted.append(f"POST {endpoint} {{textoPesquisa:...}}")
                try:
                    content, meta = ctx.fetch_bytes(
                        strategy=self.name,
                        ibge=ibge,
                        url=endpoint,
                        method="POST",
                        json_body={"textoPesquisa": "ITAPAGE CE"},
                    )
                    if "json" not in str(meta.get("content_type", "")).lower():
                        continue
                    parsed = json.loads(content.decode("utf-8", errors="ignore"))
                    if _parse_json_candidates(parsed):
                        payload = {
                            "run_id": ctx.run_id,
                            "status": "found",
                            "base_url": base_url,
                            "endpoint": endpoint,
                            "method": "POST",
                            "params_schema_guess": {"json_key": "textoPesquisa"},
                            "attempted": attempted[:200],
                        }
                        write_endpoint_cache(ctx.endpoint_cache_path, payload)
                        return payload
                except Exception:
                    pass

        payload = {
            "run_id": ctx.run_id,
            "status": "not_found",
            "base_url": base_url,
            "endpoint": None,
            "method": None,
            "params_schema_guess": None,
            "attempted": attempted[:200],
            "notes": "No working /api endpoint identified from JS probes",
        }
        write_endpoint_cache(ctx.endpoint_cache_path, payload)
        return None

    def search_reports(self, ctx: DiscoveryContext, ibge: str, endpoint_info: dict[str, Any], query: str) -> list[dict[str, Any]]:
        endpoint = str(endpoint_info["endpoint"])
        qhash = _hash_id(f"{ctx.run_id}|{ibge}|{query}|{endpoint}", prefix="query")
        if endpoint_info["method"] == "GET":
            key = endpoint_info["params_schema_guess"]["query_param"]
            content, _ = ctx.fetch_bytes(strategy=self.name, ibge=ibge, url=endpoint, method="GET", params={key: query})
        else:
            key = endpoint_info["params_schema_guess"]["json_key"]
            content, _ = ctx.fetch_bytes(strategy=self.name, ibge=ibge, url=endpoint, method="POST", json_body={key: query})
        ctx.snapshot_json(ibge=ibge, query_hash=qhash, source_url=endpoint, content=content)
        parsed = json.loads(content.decode("utf-8", errors="ignore"))
        return _parse_json_candidates(parsed)

    def discover_reports(self, ctx: DiscoveryContext, muni_name: str, uf: str, ibge: str) -> StrategyOutput:
        failures: list[str] = []
        try:
            _html, final_url = ctx.snapshot_html(strategy=self.name, ibge=ibge, muni_slug=slugify(muni_name), url=GOVBR_ENTRY_URL)
            base_url = _base_origin(final_url)
        except Exception as exc:
            write_endpoint_cache(
                ctx.endpoint_cache_path,
                {"run_id": ctx.run_id, "status": "not_found", "attempted": [GOVBR_ENTRY_URL], "notes": f"bootstrap_failed:{exc}"},
            )
            return StrategyOutput(candidates=[], failures=[f"bootstrap_failed:{exc}"])

        endpoint_info = self.discover_api_endpoint(ctx, ibge, base_url)
        if endpoint_info is None:
            return StrategyOutput(candidates=[], failures=["no_search_endpoint"])

        queries = [f"{muni_name}/{uf}", f"{muni_name} {uf}", muni_name]
        candidates: list[ReportCandidate] = []
        for q in queries:
            try:
                items = self.search_reports(ctx, ibge=ibge, endpoint_info=endpoint_info, query=q)
                if not items:
                    failures.append("empty_json_results")
                    continue
                candidates.extend(
                    parse_candidates_from_json_items(
                        items=items,
                        base_url=base_url,
                        strategy_name=self.name,
                        municipality_name=muni_name,
                        uf=uf,
                        ibge=ibge,
                        query_terms_used=q,
                    )
                )
            except Exception as exc:
                failures.append(f"api_query_failed:{exc}")

        out: list[ReportCandidate] = []
        for cand in candidates:
            if cand.pdf_url:
                out.append(cand)
                continue
            try:
                detail_html, _ = ctx.snapshot_html(strategy=self.name, ibge=ibge, muni_slug=slugify(muni_name), url=cand.landing_url)
                pdf_links = _parse_landing_html_for_pdf(detail_html, cand.landing_url)
                if pdf_links:
                    cand.pdf_url = pdf_links[0]
                    cand.notes = f"{cand.notes};pdf_from_detail"
                else:
                    for path in _extract_api_paths(detail_html):
                        candidate_url = urljoin(base_url, path)
                        if "download" in candidate_url.lower() or "arquivo" in candidate_url.lower() or ".pdf" in candidate_url.lower():
                            cand.pdf_url = candidate_url
                            cand.notes = f"{cand.notes};pdf_api_candidate"
                            break
            except Exception:
                failures.append("detail_follow_failed")
            out.append(cand)

        dedup: dict[str, ReportCandidate] = {}
        for cand in out:
            dedup[cand.candidate_id] = cand
        return StrategyOutput(candidates=list(dedup.values()), failures=failures)


class AuditoriaStrategy(AuditDiscoveryStrategy):
    name = "auditoria"

    def discover_reports(self, ctx: DiscoveryContext, muni_name: str, uf: str, ibge: str) -> StrategyOutput:
        failures: list[str] = []
        query = quote_plus(f"{muni_name} {uf}")
        urls = [AUDITORIA_ENTRY_URL, f"{AUDITORIA_ENTRY_URL}?q={query}", f"{AUDITORIA_ENTRY_URL}busca?termo={query}"]
        out: list[ReportCandidate] = []
        for i, url in enumerate(urls):
            if i >= ctx.max_pages_per_muni:
                break
            try:
                html, final_url = ctx.snapshot_html(strategy=self.name, ibge=ibge, muni_slug=slugify(muni_name), url=url)
            except Exception as exc:
                failures.append(f"auditoria_fetch_failed:{exc}")
                continue
            soup = BeautifulSoup(html, "lxml")
            for anchor in soup.find_all("a", href=True):
                href = normalize_whitespace(anchor.get("href", ""))
                full = urljoin(final_url, href)
                text = normalize_whitespace(anchor.get_text(" ", strip=True))
                tnorm = normalize_municipality_query(f"{text} {full}")
                mnorm = normalize_municipality_query(muni_name)
                if mnorm not in tnorm and uf.upper() not in tnorm:
                    continue
                pdf_url = full if full.lower().endswith(".pdf") else None
                cid = _hash_id(f"{full}|{pdf_url}|{muni_name}|{self.name}", prefix="cand")
                out.append(ReportCandidate(candidate_id=cid, municipality_name=muni_name, uf=uf, ibge_municipality_code=ibge, source_strategy=self.name, landing_url=full, pdf_url=pdf_url, title_text=text or None, date_text=None, notes="legacy_html_parse", confidence="medium", discovered_at_utc=_utc_now_iso(), normalized_muni_name=mnorm, query_terms_used=f"{muni_name} {uf}"))
        if not out:
            failures.append("no_matches")
        return StrategyOutput(candidates=out, failures=failures)


def _select_muni(df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    cols = ["municipality_name_clean", "uf", "ibge_municipality_code"]
    data = df[cols].dropna().drop_duplicates()
    if data.empty:
        return data
    return data.sample(n=min(n, len(data)), random_state=seed).reset_index(drop=True)


def _download_pdf_probe(ctx: DiscoveryContext, candidate: ReportCandidate) -> bool:
    if not candidate.pdf_url or candidate.confidence != "high":
        return False
    ibge_dir = ensure_dir(ctx.raw_pdf_dir / candidate.ibge_municipality_code)
    ext = Path(urlparse(candidate.pdf_url).path).suffix.lower() or ".pdf"
    path = ibge_dir / f"{candidate.candidate_id}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}{ext}"
    try:
        content, _ = ctx.fetch_bytes(strategy=candidate.source_strategy, ibge=candidate.ibge_municipality_code, url=candidate.pdf_url)
        atomic_write_bytes(path, content)
        upsert_manifest_row(ctx.manifest_path, {"item_type": "downloaded", "source_url": candidate.pdf_url, "local_path": str(path), "sha256": sha256_bytes(content), "size_bytes": len(content), "created_at_utc": _utc_now_iso(), "status": "ok", "notes": "pdf_probe_download"}, key_cols=["local_path", "item_type"])
        return True
    except Exception as exc:
        upsert_manifest_row(ctx.manifest_path, {"item_type": "downloaded", "source_url": candidate.pdf_url, "local_path": str(path), "sha256": "", "size_bytes": 0, "created_at_utc": _utc_now_iso(), "status": "failed", "notes": f"pdf_probe_failed:{exc}"}, key_cols=["local_path", "item_type"])
        return False

def _resolve_dirs() -> dict[str, Path]:
    raw_base = ensure_dir(raw_dir() / "cgu" / "audits")
    interim_base = ensure_dir(interim_dir() / "cgu" / "audits")
    report_dir = ensure_dir(interim_dir() / "reports" / "cgu_audits")
    manifest_dir = ensure_dir(interim_dir() / "manifests")
    return {
        "raw_html_dir": ensure_dir(raw_base / "html"),
        "raw_json_dir": ensure_dir(raw_base / "json"),
        "raw_pdf_dir": ensure_dir(raw_base / "pdfs"),
        "interim_audits_dir": interim_base,
        "report_dir": report_dir,
        "manifest_dir": manifest_dir,
    }


def run_discovery(
    *,
    run_id: str,
    sample: int,
    seed: int,
    force: bool,
    max_pages_per_muni: int,
    sleep_seconds: float,
    strategy: str,
    debug: bool,
    dirs_override: dict[str, Path] | None = None,
    input_path_override: Path | None = None,
    strategies_override: list[AuditDiscoveryStrategy] | None = None,
) -> tuple[bool, dict[str, Any]]:
    dirs = dirs_override or _resolve_dirs()
    input_path = input_path_override or (clean_dir() / "cgu" / "lottery_municipalities_with_ibge.parquet")
    manifest_path = dirs["manifest_dir"] / "cgu_audits_manifest.parquet"
    endpoint_cache_path = dirs["interim_audits_dir"] / API_ENDPOINT_CACHE_FILE
    fetch_attempts_path = dirs["interim_audits_dir"] / "fetch_attempts.parquet"
    report_path = dirs["report_dir"] / f"{run_id}_report.json"
    latest_report_path = dirs["report_dir"] / "latest.json"
    candidates_path = dirs["interim_audits_dir"] / "audit_report_candidates.parquet"
    index_path = dirs["interim_audits_dir"] / "audit_reports_index.parquet"

    run_state: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": _utc_now_iso(),
        "ended_at_utc": None,
        "success": False,
        "exception": "",
        "traceback": "",
        "num_municipalities_probed": 0,
        "strategy_breakdown": {},
        "failure_reasons": {},
        "status_code_histogram": {},
        "content_type_histogram": {},
        "sample_urls": [],
        "discovered_endpoint": None,
        "debug_html_snippets": [],
        "artifacts": {},
    }

    ctx = DiscoveryContext(
        run_id=run_id,
        force=force,
        sleep_seconds=sleep_seconds,
        max_pages_per_muni=max_pages_per_muni,
        debug=debug,
        endpoint_cache_path=endpoint_cache_path,
        raw_html_dir=dirs["raw_html_dir"],
        raw_json_dir=dirs["raw_json_dir"],
        raw_pdf_dir=dirs["raw_pdf_dir"],
        manifest_path=manifest_path,
        fetch_attempts=[],
        debug_snippets=[],
    )

    all_candidates: list[ReportCandidate] = []
    dedup_candidates: list[ReportCandidate] = []

    try:
        if not input_path.exists():
            raise FileNotFoundError(
                f"Doctor check failed: missing {input_path}. Run IBGE crosswalk first."
            )

        input_df = pd.read_parquet(input_path)
        sample_df = _select_muni(input_df, sample, seed)
        run_state["num_municipalities_probed"] = int(len(sample_df))

        if strategies_override is not None:
            chosen = strategies_override
        else:
            strategy_map: dict[str, list[AuditDiscoveryStrategy]] = {
                "auto": [EaudRelatoriosApiStrategy(), AuditoriaStrategy()],
                "eaud_api": [EaudRelatoriosApiStrategy()],
                "auditoria": [AuditoriaStrategy()],
            }
            chosen = strategy_map.get(strategy, strategy_map["auto"])

        for _, row in sample_df.iterrows():
            muni = str(row["municipality_name_clean"])
            uf = str(row["uf"]).upper().strip()
            ibge = str(row["ibge_municipality_code"]).zfill(7)
            for strat in chosen:
                out = strat.discover_reports(ctx, muni_name=muni, uf=uf, ibge=ibge)
                all_candidates.extend(out.candidates)
                bucket = run_state["strategy_breakdown"].setdefault(
                    strat.name,
                    {"candidates_found": 0, "candidates_with_pdf": 0, "errors": 0},
                )
                bucket["candidates_found"] += len(out.candidates)
                bucket["candidates_with_pdf"] += sum(1 for c in out.candidates if c.pdf_url)
                bucket["errors"] += len(out.failures)
                for reason in out.failures:
                    run_state["failure_reasons"][reason] = run_state["failure_reasons"].get(reason, 0) + 1

        dedup_map: dict[str, ReportCandidate] = {}
        for cand in all_candidates:
            dedup_map[cand.candidate_id] = cand
        dedup_candidates = list(dedup_map.values())

        for cand in dedup_candidates:
            _download_pdf_probe(ctx, cand)

        run_state["success"] = bool(len(dedup_candidates) >= 5 or sum(1 for c in dedup_candidates if c.pdf_url) >= 1)

    except Exception as exc:
        run_state["success"] = False
        run_state["exception"] = str(exc)
        run_state["traceback"] = traceback.format_exc()
        logger.exception("CGU audits discovery failed")

    finally:
        fetch_df = pd.DataFrame([asdict(x) for x in ctx.fetch_attempts])
        if fetch_df.empty:
            fetch_df = pd.DataFrame(columns=["run_id", "strategy", "municipality_ibge", "url", "status_code", "content_type", "final_url", "bytes_len", "elapsed_ms", "ok", "error"])
        saved_fetch_path = _write_df_with_fallback(fetch_df, fetch_attempts_path)

        all_df = pd.DataFrame([asdict(c) for c in all_candidates])
        if all_df.empty:
            all_df = pd.DataFrame(columns=["candidate_id", "municipality_name", "uf", "ibge_municipality_code", "source_strategy", "landing_url", "pdf_url", "title_text", "date_text", "notes", "confidence", "discovered_at_utc", "normalized_muni_name", "query_terms_used"])
        write_parquet(all_df.rename(columns={"candidate_id": "report_candidate_id"}), candidates_path)

        idx_df = pd.DataFrame([asdict(c) for c in dedup_candidates])
        if idx_df.empty:
            idx_df = all_df.copy()
        write_parquet(idx_df.rename(columns={"candidate_id": "report_candidate_id"}), index_path)

        if not endpoint_cache_path.exists():
            write_endpoint_cache(
                endpoint_cache_path,
                {
                    "run_id": run_id,
                    "status": "not_found",
                    "attempted": [],
                    "notes": "EAUD API strategy did not write endpoint cache",
                },
            )

        try:
            endpoint_payload = json.loads(endpoint_cache_path.read_text(encoding="utf-8"))
        except Exception:
            endpoint_payload = {"status": "unreadable"}

        run_state["ended_at_utc"] = _utc_now_iso()
        run_state["status_code_histogram"] = fetch_df["status_code"].value_counts(dropna=False).to_dict()
        run_state["content_type_histogram"] = fetch_df["content_type"].fillna("").value_counts(dropna=False).head(30).to_dict()
        run_state["sample_urls"] = fetch_df["final_url"].dropna().astype(str).head(20).tolist()
        run_state["discovered_endpoint"] = endpoint_payload
        run_state["debug_html_snippets"] = ctx.debug_snippets if debug else []
        run_state["num_candidates_found_total"] = int(len(dedup_candidates))
        run_state["num_candidates_with_pdf"] = int(idx_df["pdf_url"].notna().sum()) if "pdf_url" in idx_df.columns else 0
        run_state["artifacts"] = {
            "report_json": str(report_path),
            "latest_json": str(latest_report_path),
            "fetch_attempts": str(saved_fetch_path),
            "endpoint_cache": str(endpoint_cache_path),
            "audit_report_candidates": str(candidates_path),
            "audit_reports_index": str(index_path),
            "raw_html_dir": str(dirs["raw_html_dir"]),
            "raw_json_dir": str(dirs["raw_json_dir"]),
        }
        if run_state["num_candidates_found_total"] == 0:
            run_state["message"] = "No candidates found. Inspect fetch_attempts and eaud_api_endpoint cache."

        payload = json.dumps(run_state, ensure_ascii=False, indent=2).encode("utf-8")
        atomic_write_bytes(report_path, payload)
        atomic_write_bytes(latest_report_path, payload)

        for path, note in [
            (saved_fetch_path, "fetch_attempts"),
            (report_path, "run_report"),
            (latest_report_path, "run_report_latest"),
            (endpoint_cache_path, "eaud_api_endpoint_cache"),
            (candidates_path, "audit_report_candidates"),
            (index_path, "audit_reports_index"),
        ]:
            status = "ok" if path.exists() else "failed"
            checksum = sha256_file(path) if path.exists() else ""
            size = path.stat().st_size if path.exists() else 0
            upsert_manifest_row(
                manifest_path,
                {
                    "item_type": "generated",
                    "source_url": "generated",
                    "local_path": str(path),
                    "sha256": checksum,
                    "size_bytes": size,
                    "created_at_utc": _utc_now_iso(),
                    "status": status,
                    "notes": note,
                },
                key_cols=["local_path", "item_type"],
            )

    return bool(run_state["success"]), run_state


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover and index CGU audit report candidates (MVP).")
    parser.add_argument("--sample", type=int, default=25)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-pages-per-muni", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--strategy", type=str, choices=["auto", "eaud_api", "auditoria"], default="auto")
    parser.add_argument("--log-level", type=str, default=None)
    parser.add_argument("--debug", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    settings = get_settings()
    setup_logging(args.log_level or settings.log_level)

    dirs = _resolve_dirs()
    print(f"resolved_data_dir={clean_dir().parent}")
    success, state = run_discovery(
        run_id=run_id,
        sample=int(args.sample),
        seed=int(args.seed),
        force=bool(args.force),
        max_pages_per_muni=int(args.max_pages_per_muni),
        sleep_seconds=float(args.sleep_seconds),
        strategy=str(args.strategy),
        debug=bool(args.debug),
        dirs_override=dirs,
    )

    print(f"report_path={state['artifacts']['report_json']}")
    print(f"endpoint_cache_path={state['artifacts']['endpoint_cache']}")
    print(f"fetch_attempts_path={state['artifacts']['fetch_attempts']}")
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
