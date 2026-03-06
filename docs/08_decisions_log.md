# Decisions Log

## Purpose
- Record major project decisions and rationale.
- Keep a transparent history of alternatives and known risks.
- Support reproducibility and team alignment.

## Template
- Date:
- Decision:
- Rationale:
- Alternatives considered:
- Risks:
- Next steps:

## Entry 1
- Date: 2026-03-06
- Decision: Use CGU lottery cohorts as the primary design backbone.
- Rationale:
- Lottery cohorts provide the clearest quasi-random structure for treatment timing.
- They align naturally with municipality-level panels and event-study timing.
- Alternatives considered:
- Pure observational design on audit severity only.
- Court-level cross-section without cohort timing.
- Risks:
- Randomness quality may vary across eras and program revisions.
- Cohort metadata may be incomplete for some rounds.
- Next steps:
- Build diagnostics for balance and pre-trends by period.

## Entry 2
- Date: 2026-03-06
- Decision: Build on Dahis et al.-style LLM audit measures instead of competing with them.
- Rationale:
- Existing work already advances audit-content measurement.
- Comparative advantage here is downstream enforcement linkage and punishment estimation.
- Alternatives considered:
- Rebuild a full alternative corruption-measure pipeline from scratch.
- Restrict analysis to non-LLM hand-coded finding measures only.
- Risks:
- Dependence on upstream measure assumptions.
- Compatibility issues across schema variants.
- Next steps:
- Define adapters so multiple audit-finding inputs can feed the same enforcement pipeline.

## Entry 3
- Date: 2026-03-06
- Decision: Start MVP with ingestion and chunking before full extraction/linking.
- Rationale:
- Early bottleneck risk is data availability and parse reliability.
- Stable raw/interim layers reduce wasted LLM iteration.
- Alternatives considered:
- Build full LLM extraction first on ad hoc sample files.
- Start with econometric modeling using manually curated subset only.
- Risks:
- Slower short-term visible results.
- Potential rework if source schemas shift after ingest.
- Next steps:
- Implement deterministic ingestors and manifests.
- Add chunk quality checks and pilot validation set.
