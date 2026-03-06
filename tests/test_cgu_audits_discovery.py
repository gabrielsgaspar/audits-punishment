from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from audits_punishment.ingest.cgu_audits import (
    AuditDiscoveryStrategy,
    _extract_api_paths,
    parse_candidates_from_json_items,
    run_discovery,
)


def test_extract_api_paths_from_js_fixture() -> None:
    js = """
    const API_A = "/api/relatorios/pesquisar";
    const API_B = '/api/relatorios/download';
    """
    paths = _extract_api_paths(js)
    assert "/api/relatorios/pesquisar" in paths
    assert "/api/relatorios/download" in paths


def test_parse_candidates_from_json_items_fixture() -> None:
    items = [{"id": 1234, "titulo": "Relatorio de Auditoria - Itapage CE", "pdfUrl": "/files/1234.pdf"}]
    candidates = parse_candidates_from_json_items(
        items=items,
        base_url="https://auditoria.cgu.gov.br",
        strategy_name="eaud_api",
        municipality_name="Itapage",
        uf="CE",
        ibge="2306306",
        query_terms_used="Itapage CE",
    )
    assert len(candidates) == 1
    assert candidates[0].pdf_url == "https://auditoria.cgu.gov.br/files/1234.pdf"


class BrokenStrategy(AuditDiscoveryStrategy):
    name = "broken"

    def discover_reports(self, ctx, muni_name: str, uf: str, ibge: str):  # type: ignore[override]
        raise RuntimeError("simulated_failure")


def _mk_dirs(tmp_path: Path) -> dict[str, Path]:
    return {
        "raw_html_dir": (tmp_path / "raw" / "cgu" / "audits" / "html"),
        "raw_json_dir": (tmp_path / "raw" / "cgu" / "audits" / "json"),
        "raw_pdf_dir": (tmp_path / "raw" / "cgu" / "audits" / "pdfs"),
        "interim_audits_dir": (tmp_path / "interim" / "cgu" / "audits"),
        "report_dir": (tmp_path / "interim" / "reports" / "cgu_audits"),
        "manifest_dir": (tmp_path / "interim" / "manifests"),
    }


def test_run_writes_report_even_on_exception(tmp_path: Path) -> None:
    input_path = tmp_path / "clean" / "cgu" / "lottery_municipalities_with_ibge.parquet"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"municipality_name_clean": "Itapage", "uf": "CE", "ibge_municipality_code": "2306306"}]
    ).to_parquet(input_path, index=False)

    ok, state = run_discovery(
        run_id="20260306T999999Z",
        sample=1,
        seed=123,
        force=True,
        max_pages_per_muni=1,
        sleep_seconds=0.0,
        strategy="auto",
        debug=False,
        dirs_override=_mk_dirs(tmp_path),
        input_path_override=input_path,
        strategies_override=[BrokenStrategy()],
    )
    assert ok is False
    report_path = Path(state["artifacts"]["report_json"])
    latest_path = Path(state["artifacts"]["latest_json"])
    fetch_path = Path(state["artifacts"]["fetch_attempts"])
    assert report_path.exists()
    assert latest_path.exists()
    assert fetch_path.exists()


def test_endpoint_cache_written_when_not_found(tmp_path: Path) -> None:
    input_path = tmp_path / "clean" / "cgu" / "lottery_municipalities_with_ibge.parquet"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"municipality_name_clean": "Itapage", "uf": "CE", "ibge_municipality_code": "2306306"}]
    ).to_parquet(input_path, index=False)

    ok, state = run_discovery(
        run_id="20260306T888888Z",
        sample=1,
        seed=123,
        force=True,
        max_pages_per_muni=1,
        sleep_seconds=0.0,
        strategy="auto",
        debug=False,
        dirs_override=_mk_dirs(tmp_path),
        input_path_override=input_path,
        strategies_override=[],
    )
    assert ok is False
    cache_path = Path(state["artifacts"]["endpoint_cache"])
    assert cache_path.exists()
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert payload["status"] == "not_found"
