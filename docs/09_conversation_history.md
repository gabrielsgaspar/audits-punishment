# Conversation History

## Narrative of project evolution
- The starting idea was to study whether anti-corruption audits produce meaningful punishment, not only political effects.
- Early framing centered on linking municipal audit findings to judicial outcomes using modern NLP and LLM tooling.
- During scoping, we recognized that existing literature already includes strong audit-based corruption measures.
- In particular, we surfaced that Dahis and related work already push forward LLM-enabled measurement on audit content.
- That changed the objective from "new audit measure" toward "new enforcement pipeline".

## Key conceptual shift
- Instead of competing on finding extraction alone, the project pivots to downstream legal enforcement.
- The central contribution becomes: mapping findings to punishment trajectories through courts and tribunals.
- This makes the causal question sharper: audits -> enforcement outcomes.

## Methodological concern raised
- We discussed whether audit randomness is stable over time.
- Concern: lottery assignment procedures and program scope may not be constant across periods.
- Implication: identification strategy needs explicit diagnostics by era and cohort.
- Result: econometrics plan now includes balance checks, pre-trend tests, and period-specific robustness.

## Pipeline direction selected
- Build deterministic ingestion for CGU lottery cohorts, CGU audits, CNJ DataJud, and STJ datasets.
- Standardize parsing/chunking before scaling LLM extraction.
- Use schema-constrained LLM outputs with evidence spans.
- Apply confidence-aware linking to connect audit findings and decision outcomes.
- Produce enforcement indices for causal analysis.

## Execution strategy
- We chose a docs-driven repository structure first.
- The purpose is to make design assumptions explicit before implementing heavy data pulls.
- This also supports iterative work with Codex by keeping module boundaries and requirements clear.
- The current scaffold includes placeholders and tests so implementation can proceed in disciplined stages.

## Current status
- Project root is set up for Python 3.11 with src-layout packaging.
- Docs define research questions, source mapping, download plans, linking logic, and validation protocol.
- Initial modules are intentionally minimal with TODO-oriented entry points.
- Next phase is implementing ingestion modules and first pilot runs.
