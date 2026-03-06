# Audits → Punishment: Does Enforcement Deter Corruption and Improve Development?

## One-sentence summary
Build an end-to-end, reproducible pipeline that links Brazilian municipal anti-corruption audits to judicial processes and decisions, constructs measures of enforcement (certainty, severity, speed), and estimates the causal effect of punishment on future corruption, procurement behavior, public service delivery, and political selection.

## Core research questions
1) **Deterrence:** Conditional on corruption detection in an audit, does higher ex post punishment reduce future corruption and improve outcomes?
2) **Which enforcement margin matters most?** Certainty vs severity vs speed.
3) **Mechanisms:** Discipline (behavior change) vs selection (politician entry/exit, reelection).
4) **Complementarity:** How do punishment and electoral accountability interact?

## Key context / design constraints
- Audit selection was historically lottery-based, but the CGU program evolved over time and includes non-lottery selection modes in some later cycles. The main causal design must use cohorts/rounds where lottery selection is defensible and documented, with careful balance tests.
- Ricardo Dahis (with coauthors) has already created an LLM-based measure of corruption from CGU audit texts. This project does **not** aim to “redo” that contribution. We build on it and focus on **enforcement**: linking findings to legal processes and extracting judicial outcomes from decision text.

## Main data sources (Brazil)
- **CGU audit lottery lists** (selected municipalities per sorteio) + **audit reports PDFs**.
- **CNJ DataJud API**: judicial process metadata across courts (filings, movements, status, class/subject).
- **STJ Open Data (CKAN)**: downloadable corpora of full decision texts (and metadata) published in the Diário da Justiça.
- Outcomes (choose primary set early): elections (TSE), education (SAEB/INEP), health (DataSUS), fiscal/transfers, procurement (optional extension).

## Pipeline overview (high-level)
1) **Ingest**: download lottery lists, audit PDFs, DataJud metadata, STJ decision texts.
2) **Parse/Chunk**: extract text (PDF→text), standardize, chunk into “findings” and “decision sections”.
3) **Audit finding cards**: LLM extraction of linking fields (program, entities, amounts, dates, contractors).
4) **Candidate case retrieval**: deterministic search rules in DataJud for process candidates for each finding.
5) **LLM match scoring**: probabilistic linking of finding ↔ process(es).
6) **Decision outcome extraction**: LLM extraction from STJ text (uphold/overturn, sanctions, procedural grounds, timing).
7) **Enforcement indices**: municipality-time measures (certainty, severity, speed).
8) **Econometrics**: main design + diagnostics; robustness; heterogeneity; external validity.

## Deliverables
- Reproducible data lake: raw → interim → clean datasets with manifests.
- Validated LLM extraction modules with gold-labeled samples + error metrics.
- Linked audit→case→decision graph with match probabilities.
- Final analysis datasets and a paper draft.

## Operating principles
- Everything is scripted, deterministic, and versioned (manifests, checksums).
- LLM outputs must be schema-constrained JSON + evidence spans.
- Keep a decision log: what we decided, why, and what risks remain.