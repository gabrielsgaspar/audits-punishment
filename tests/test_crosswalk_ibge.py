from __future__ import annotations

import pandas as pd

from audits_punishment.build.crosswalk_ibge import (
    build_crosswalk,
    normalize_name,
    parse_reference_payload,
)


def _reference_df(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    data = []
    for code, name, uf in rows:
        data.append(
            {
                "ibge_municipality_code": code,
                "municipality_name_official": name,
                "uf": uf,
                "municipality_name_norm": normalize_name(name),
                "source_url": "test",
                "pulled_at_utc": "2026-03-06T00:00:00Z",
            }
        )
    return pd.DataFrame(data)


def test_normalize_name_strips_bullets_and_accents() -> None:
    assert normalize_name("- Itapage") == "ITAPAGE"
    assert normalize_name("• Sao Joao d'Alianca/GO") == "SAO JOAO D ALIANCA GO"


def test_parse_reference_payload_skips_missing_uf_when_nested_null() -> None:
    payload = [
        {"id": 1234567, "nome": "Foo", "microrregiao": None},
        {"id": 3550308, "nome": "Sao Paulo", "microrregiao": {"mesorregiao": {"UF": {"sigla": "SP"}}}},
    ]
    df, stats = parse_reference_payload(payload, source_url="test")
    assert stats["total_records"] == 2
    assert stats["parsed_ok"] == 1
    assert stats["skipped_missing_uf"] == 1
    assert df["ibge_municipality_code"].tolist() == ["3550308"]


def test_alias_application_maps_historic_name() -> None:
    lottery = pd.DataFrame([{"municipality_name_clean": "Sao Valerio da Natividade", "uf": "TO"}])
    reference = _reference_df([("1720499", "Sao Valerio", "TO")])
    alias_df = pd.DataFrame(
        [
            {
                "uf": "TO",
                "name_norm_from": "SAO VALERIO DA NATIVIDADE",
                "name_norm_to": "SAO VALERIO",
                "note": "historic name",
                "source": "test",
            }
        ]
    )
    out, manual = build_crosswalk(lottery, reference, threshold=92.0, alias_df=alias_df)
    assert manual.empty
    assert out.loc[0, "ibge_municipality_code"] == "1720499"
    assert out.loc[0, "match_method"] == "normalized_exact"
    assert "alias_applied:" in str(out.loc[0, "match_notes"])


def test_override_application_for_known_typo() -> None:
    lottery = pd.DataFrame([{"municipality_name_clean": "- Boninau", "uf": "BA"}])
    reference = _reference_df([("2904001", "Boninal", "BA")])
    override_df = pd.DataFrame(
        [
            {
                "uf": "BA",
                "municipality_name_clean": "- Boninau",
                "ibge_municipality_code": "2904001",
                "note": "CGU list typo/formatting; intended Boninal",
            }
        ]
    )
    out, manual = build_crosswalk(
        lottery,
        reference,
        threshold=92.0,
        override_df=override_df,
    )
    assert manual.empty
    assert out.loc[0, "ibge_municipality_code"] == "2904001"
    assert out.loc[0, "match_method"] == "override"
    assert out.loc[0, "match_score"] == 1.0


def test_relaxed_fuzzy_acceptance_rule() -> None:
    lottery = pd.DataFrame([{"municipality_name_clean": "Boninla", "uf": "BA"}])
    reference = _reference_df(
        [
            ("2904001", "Boninal", "BA"),
            ("2923100", "Olindina", "BA"),
        ]
    )
    out, _manual = build_crosswalk(lottery, reference, threshold=95.0)
    assert out.loc[0, "match_method"] == "fuzzy_relaxed"
    assert out.loc[0, "ibge_municipality_code"] == "2904001"
