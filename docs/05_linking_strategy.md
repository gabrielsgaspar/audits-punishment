# Linking Strategy

## Objective
- Link audit findings to judicial decisions with transparent uncertainty handling.
- Avoid overconfident one-to-one assumptions in noisy administrative data.

## Two-stage approach
1. Deterministic candidate generation.
2. LLM-assisted semantic match scoring.

## Stage 1: deterministic candidate generation
- Build candidate pairs using conservative rules:
- Municipality overlap within defined time window.
- Shared normalized names of entities or officials when available.
- Program/sector keyword overlap from audit findings and case subjects.
- Court jurisdiction consistency checks.
- Candidate generation should prioritize recall with bounded explosion.

## Stage 2: LLM match scoring
- For each candidate pair, evaluate semantic relation strength.
- Model output includes:
- Score in [0,1].
- Match type (direct, probable, weak, none).
- Short rationale.
- Evidence spans from both source documents.

## Confidence thresholds
- High-confidence link: score >= 0.85 and evidence on core actors/events.
- Medium-confidence link: 0.65 <= score < 0.85.
- Low-confidence/ambiguous: score < 0.65.
- Primary econometric sample uses high-confidence links.
- Robustness analyses include medium-confidence with weights.

## Handling multiple matches
- One finding can map to multiple case records.
- One case can map to multiple findings.
- We keep many-to-many edges in link graph.
- For outcome aggregation:
- Report both max-score link and weighted multi-link summaries.
- Flag clusters with conflicting outcomes for manual review.

## Temporal rules
- Default window starts at audit publication date.
- Pre-audit decisions are excluded from causal link claims.
- Long-tail enforcement windows are analyzed separately.

## Conflict resolution
- If deterministic keys conflict with LLM score, keep pair but downgrade confidence.
- If multiple high-confidence matches exist, retain all and model at edge level.
- No forced deduplication without explicit legal identity evidence.

## Auditing and diagnostics
- Track score distributions by source and year.
- Sample manual audits of links near threshold boundaries.
- Maintain an exceptions file for known systematic false positives.

## Outputs
- `data/interim/link_candidates.parquet`
- `data/interim/match_scores.parquet`
- `data/clean/link_graph.parquet`
