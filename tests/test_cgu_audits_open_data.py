from __future__ import annotations

from pathlib import Path

import pandas as pd

from audits_punishment.ingest.cgu_audits_open_data import (
    apply_only_sorteio_filter,
    _ensure_edition_field,
    _link_lottery_and_audits,
    _normalize_column_name,
    _prepare_auditorias_df,
    _read_csv_robust,
    pick_col,
)


def test_prepare_auditorias_df_normalizes_core_fields_plural_headers() -> None:
    raw = pd.DataFrame(
        [
            {
                "IdTarefa": "123",
                "Titulo": "Relatório Teste",
                "DataPublicacao": "06/03/2026",
                "UFs": " ce ",
                "Municipios": " São  João d'Aliança ",
                "EdicaoProgramaSorteioFEF": "SORTEIO 10",
            }
        ]
    )
    out, meta = _prepare_auditorias_df(raw)
    assert meta["resolved_uf_col"] == "ufs"
    assert meta["resolved_muni_col"] == "municipios"
    assert meta["resolved_date_col"] == "datapublicacao"
    assert out.loc[0, "id_da_tarefa"] == "123"
    assert out.loc[0, "uf"] == "CE"
    assert out.loc[0, "municipality_name_raw"] == "São João d'Aliança"
    assert out.loc[0, "municipality_name_norm"] == "SAO JOAO D ALIANCA"
    assert str(out.loc[0, "publication_date"].date()) == "2026-03-06"


def test_apply_only_sorteio_filter_uses_group_or_fef_nonnull() -> None:
    df = pd.DataFrame(
        {
            "grupoatividade": [
                "Fiscalização de Entes Federativos",
                "Outra coisa",
                "Outra coisa",
            ],
            "fef": [
                pd.NA,
                "V06",
                pd.NA,
            ],
        }
    )
    out, meta = apply_only_sorteio_filter(df, only_sorteio=True)
    assert len(out) == 2
    assert meta["sorteio_filter_applied"] is True
    assert meta["sorteio_rows_matched"] == 2


def test_apply_only_sorteio_filter_fef_coded_values_are_nonnull_match() -> None:
    df = pd.DataFrame(
        {
            "grupoatividade": ["sem match", "sem match", "sem match"],
            "fef": ["V06", "034", ""],
        }
    )
    out, meta = apply_only_sorteio_filter(df, only_sorteio=True)
    assert len(out) == 2
    assert meta["sorteio_rows_matched"] == 2
    assert meta["sorteio_filter_rule_used"] == "group_contains_entes_federativos OR fef_nonnull"


def test_link_lottery_and_audits_builds_urls() -> None:
    lottery = pd.DataFrame(
        [
            {
                "ibge_municipality_code": "2306306",
                "uf": "CE",
                "municipality_name_clean": "Itapage",
                "round_id": "r1",
                "round_label": "1o sorteio",
            }
        ]
    )
    audits = pd.DataFrame(
        [
            {
                "ibge_municipality_code": "2306306",
                "uf": "CE",
                "municipality_name_clean": "Itapage",
                "id_da_tarefa": "9001",
                "id_da_auditoria": "77",
                "titulo_do_relatorio": "Auditoria Itapage",
                "data_de_publicacao": pd.Timestamp("2020-01-15"),
                "edicao_programa_sorteio_fef": "SORTEIO 1",
            }
        ]
    )
    events, long_df, stats = _link_lottery_and_audits(lottery, audits)
    assert len(events) == 1
    assert len(long_df) == 1
    assert long_df.loc[0, "eaud_relatorio_url"] == "https://eaud.cgu.gov.br/relatorio/9001"
    assert stats["num_lottery_municipalities_with_audit"] == 1


def test_missing_edicao_column_is_derived_from_fef_and_link_still_works() -> None:
    audits = pd.DataFrame(
        [
            {
                "ibge_municipality_code": "2306306",
                "uf": "CE",
                "municipality_name_raw": "Itapage",
                "id_da_tarefa": "9001",
                "id_da_auditoria": "77",
                "titulo_do_relatorio": "Auditoria Itapage",
                "publication_date": pd.Timestamp("2020-01-15"),
                "grupoatividade": "Fiscalização de Entes Federativos",
                "linhaacao": "Linha A",
                "tiposervico": "Tipo X",
                "fef": "V06",
            }
        ]
    )
    audits2, source, warning = _ensure_edition_field(audits)
    assert source == "derived_from_fef"
    assert warning is not None
    assert "edicao_programa_sorteio_fef" in audits2.columns

    lottery = pd.DataFrame(
        [
            {
                "ibge_municipality_code": "2306306",
                "uf": "CE",
                "municipality_name_clean": "Itapage",
                "round_id": "r1",
                "round_label": "1o sorteio",
            }
        ]
    )
    events, long_df, _stats = _link_lottery_and_audits(lottery, audits2)
    assert len(events) == 1
    assert len(long_df) == 1
    assert "eaud_relatorio_url" in long_df.columns


def test_normalize_column_name_handles_accents_and_symbols() -> None:
    assert _normalize_column_name("Edição Programa Sorteio / FEF") == "edicao_programa_sorteio_fef"


def test_read_csv_robust_semicolon_delimiter(tmp_path: Path) -> None:
    csv_text = (
        "Id da Tarefa;UF;Município;Data de Publicação;Título do Relatório\n"
        "1001;CE;Itapajé;06/03/2026;Relatório A\n"
    )
    path = tmp_path / "auditorias_semicolon.csv"
    path.write_text(csv_text, encoding="utf-8")
    df, encoding, delimiter = _read_csv_robust(path)
    assert df.shape[1] > 1
    assert delimiter == ";"
    assert encoding in {"utf-8-sig", "latin-1"}


def test_read_csv_robust_comma_delimiter(tmp_path: Path) -> None:
    csv_text = (
        "Id da Tarefa,UF,Município,Data de Publicação,Título do Relatório\n"
        "1002,SP,São Paulo,06/03/2026,Relatório B\n"
    )
    path = tmp_path / "auditorias_comma.csv"
    path.write_text(csv_text, encoding="utf-8")
    df, encoding, delimiter = _read_csv_robust(path)
    assert df.shape[1] > 1
    assert delimiter == ","
    assert encoding in {"utf-8-sig", "latin-1"}


def test_pick_col_resolves_ufs_municipios_datapublicacao() -> None:
    df = pd.DataFrame(columns=["ufs", "municipios", "datapublicacao", "idtarefa"])
    assert pick_col(df, ["ufs", "uf"]) == "ufs"
    assert pick_col(df, ["municipios", "municipio"]) == "municipios"
    assert pick_col(df, ["datapublicacao", "data_publicacao"]) == "datapublicacao"
