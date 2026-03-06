from __future__ import annotations

from audits_punishment.ingest.cgu_lottery import (
    extract_municipalities_from_html,
    extract_round_links,
    process_round_html,
)


def test_extract_round_links_from_index_fixture() -> None:
    html = """
    <html>
      <body>
        <div class="accordion"><a href="/cgu/sorteio-40#section">40o Sorteio - Municipios</a></div>
        <div class="card"><a href="https://example.org/sorteio-41">41o sorteio publico</a></div>
        <a href="/not-related">Not related</a>
      </body>
    </html>
    """
    rounds = extract_round_links(html, "https://www.gov.br/cgu/index")
    assert len(rounds) == 2
    assert rounds[0]["url"] == "https://www.gov.br/cgu/sorteio-40"
    assert rounds[1]["url"] == "https://example.org/sorteio-41"


def test_extract_municipality_from_round_fixture() -> None:
    html = """
    <html>
      <body>
        <h2>Lista de municipios</h2>
        <ul>
          <li>Sao Paulo/SP</li>
          <li>Entrada invalida</li>
        </ul>
      </body>
    </html>
    """
    municipalities = extract_municipalities_from_html(html)
    assert len(municipalities) == 1
    row = municipalities[0]
    assert row["municipality_name_clean"] == "Sao Paulo"
    assert row["uf"] == "SP"
    assert row["parse_method"] == "li"


def test_process_round_html_pdf_only_fixture() -> None:
    html = """
    <html>
      <body>
        <p>Lista no arquivo abaixo.</p>
        <a href="/files/lista_sorteio.pdf">Baixar PDF</a>
      </body>
    </html>
    """
    result = process_round_html("https://example.org/round", html)
    assert result.parse_status == "needs_pdf_parse"
    assert result.primary_resource_type == "pdf"
    assert len(result.municipalities) == 0


def test_process_round_html_table_fixture() -> None:
    html = """
    <html>
      <body>
        <table>
          <tr><th>Municipio/UF</th></tr>
          <tr><td>Campinas/SP</td></tr>
        </table>
      </body>
    </html>
    """
    result = process_round_html("https://example.org/round", html)
    assert result.parse_status == "parsed"
    assert len(result.municipalities) == 1
    assert result.municipalities[0]["municipality_name_clean"] == "Campinas"
