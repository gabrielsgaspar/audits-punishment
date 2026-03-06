# LLM Pipeline Design

## Goals
- Convert unstructured audit and judicial text into reliable structured records.
- Make extraction transparent through strict schemas and evidence spans.
- Preserve enough metadata to audit model behavior and rerun deterministically.

## Core principles
- Schema-first extraction: every LLM output must validate against a Pydantic model.
- Evidence spans required: each key field should reference a quote and location metadata.
- Chunk before inference: avoid long-context drift by bounded chunk windows.
- Calibrate confidence: keep probabilities or confidence labels for downstream sensitivity.
- Version everything: prompt version, model, temperature, and schema version.

## Main schema families
- `AuditFindingCard`: finding type, severity, actors, programs, amounts, dates, evidence.
- `MatchScore`: candidate link between finding and decision with confidence and rationale.
- `DecisionOutcome`: outcome class, legal stage, adverse/neutral signal, evidence.

## Chunking strategy
- Audit chunks:
- Segment by headings when available.
- Fall back to paragraph windows with overlap.
- Preserve page number and character offsets.
- Decision chunks:
- Segment by formal decision sections or paragraph windows.
- Preserve publication date and document identifiers.

## Prompt design
- Use task-specific prompt files in `src/audits_punishment/llm/prompts/`.
- Require JSON-only outputs matching schema fields.
- Instruct model to abstain when evidence is weak.
- Avoid legal interpretation beyond text-supported classification.

## Runtime plan
- Batch processing by source document.
- Validate every model response.
- Route invalid responses to retry with stricter format reminder.
- Persist both raw LLM response and validated parsed object.

## Validation loop
- Build a gold-labeled pilot set.
- Compare extraction labels and spans against human annotations.
- Track precision, recall, calibration, and abstention rates.
- Tune prompt wording and chunk size before full-scale runs.

## Error controls
- Hard fail on schema-breaking outputs.
- Soft fail on low-confidence outputs with explicit null fields.
- Capture token usage and latency for cost/performance tradeoffs.

## Deliverables
- `data/interim/audit_finding_cards.parquet`
- `data/interim/match_scores.parquet`
- `data/interim/decision_outcomes.parquet`
- Run metadata tables for reproducibility.
