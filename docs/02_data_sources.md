# Data Sources

## Source 1: CGU lottery rounds and municipalities
- Purpose: identify audit exposure cohorts and randomization rounds.
- Public page: https://www.gov.br/cgu/pt-br/assuntos/auditoria-e-fiscalizacao/programa-de-fiscalizacao-em-entes-federativos/edicoes-anteriores/municipios
- Expected fields:
- Lottery round number or identifier.
- Municipality name.
- Municipality state (UF).
- Municipality code when available (IBGE or equivalent).
- Draw date and publication references.
- Treatment relevance: defines audit assignment and timing.

## Source 2: CGU audit report documents
- Purpose: obtain detailed findings to measure irregularities and corruption-related signals.
- Expected fields:
- Audit report ID.
- Municipality and round linkage keys.
- Audit period and publication date.
- PDF file URL or archive path.
- Report sections and finding-level textual content.
- Monetary values, programs, or sectors where extractable.
- Notes:
- Reports may vary in layout across years.
- Parsing strategy must preserve page anchors for evidence spans.

## Source 3: CNJ DataJud API
- Public page: https://www.cnj.jus.br/sistemas/datajud/api-publica/
- Purpose: structured judicial process metadata and movements.
- Expected fields:
- Process/case identifiers.
- Court and tribunal identifiers.
- Procedural class and subject tags.
- Movement dates and event types.
- Party entities and role metadata (when public).
- Geography/court jurisdiction markers.
- Notes:
- API constraints and response schemas can change over time.
- We will capture retrieval date and endpoint version in manifests.

## Source 4: STJ open data portal
- Portal: https://dadosabertos.web.stj.jus.br/
- Decisions dataset example: https://dadosabertos.web.stj.jus.br/dataset/integras-de-decisoes-terminativas-e-acordaos-do-diario-da-justica
- Purpose: decision texts and higher-court outcomes.
- Expected fields:
- Decision document identifier.
- Case reference fields.
- Decision date and publication date.
- Full decision text.
- Relator/minister metadata when available.
- Procedural type and outcome indicators when available.

## Cross-source linkage fields we expect to construct
- Municipality-time keys.
- Standardized person/entity name representations.
- Case number normalization.
- Temporal windows from audit publication to judicial events.
- Confidence-weighted match IDs.

## Provenance and reproducibility
- Every download writes a manifest row with source URL, timestamp, file hash, and module version.
- Raw files remain immutable.
- Transformations create interim artifacts with explicit parent references.

## Data availability and legal context
- All listed sources are public or public API/document portals.
- Public availability does not remove responsibilities for careful handling.
- We avoid publishing unnecessary personally identifying fragments.
