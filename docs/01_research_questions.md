# Research Questions and Testable Hypotheses

## Scope
- We structure the agenda around four main questions (Q1-Q4).
- Each question is translated into measurable outcomes and falsifiable hypotheses.
- Outcomes are designed for panel econometrics and event-study analysis.

## Q1: Do audits increase punishment?
- Hypothesis H1: Municipalities exposed to audit findings experience higher rates of adverse judicial outcomes linked to public misconduct.
- Outcome O1.1: Count of conviction-like or sanction-like decision outcomes per municipality-year.
- Outcome O1.2: Binary indicator for any punishment event within k years after audit publication.
- Outcome O1.3: Severity-weighted punishment index.
- Estimand: Intent-to-treat effect of lottery-based audit exposure on punishment outcomes.

## Q2: Does finding severity predict enforcement intensity?
- Hypothesis H2: Higher-severity findings are linked to stronger and faster enforcement.
- Outcome O2.1: Time-to-first adverse decision from audit publication date.
- Outcome O2.2: Probability of punishment conditional on at least one linked case.
- Outcome O2.3: Enforcement intensity score combining outcome type and legal stage.
- Key regressor: LLM-structured finding severity and legal relevance tags.

## Q3: Which institutional channels mediate punishment?
- Hypothesis H3: Effects vary by court level, case class, and institutional capacity.
- Outcome O3.1: Split by state/federal court pathway where identifiable.
- Outcome O3.2: Split by decision type (procedural dismissal vs merits decision).
- Outcome O3.3: Interaction terms with baseline judiciary congestion indicators.
- Mechanism tests: differential effects by local legal infrastructure and audit scope.

## Q4: How robust are results to linkage uncertainty?
- Hypothesis H4: Core conclusions persist under conservative matching thresholds.
- Outcome O4.1: Main treatment estimates under strict, medium, and permissive match cutoffs.
- Outcome O4.2: Bounds using unmatched and ambiguous cases as adverse scenarios.
- Outcome O4.3: Sensitivity curves over confidence threshold grids.
- We report uncertainty propagation, not only point estimates.

## Measurement definitions
- Punishment event: decision outcome classified as sanction, conviction, condemnation, or equivalent adverse legal result.
- Neutral event: procedural movement without clear adverse consequence.
- Non-punishment event: dismissal, acquittal, or annulment where applicable.
- Severity: schema-based ordinal level inferred from finding content and evidence.

## Identification framing
- Primary design leverages lottery cohorts and timing variation.
- Secondary analyses use event-study timing around audit release.
- Robustness checks include placebo timing and pre-trend diagnostics.

## Data products needed for Q1-Q4
- Audit-finding cards with evidence spans.
- Decision-outcome records with standardized labels.
- Match-score tables with confidence and rationale.
- Municipality-time panel with exposure, outcomes, and controls.

## Decision rules for publication-grade analysis
- Pre-register threshold rules before full-scale inference.
- Freeze extraction prompts and schema versions.
- Report model/version metadata for every LLM-derived field.
- Keep a transparent exclusions ledger.
