# Decision Outcome Prompt

Task:
- Read a judicial decision chunk.
- Extract structured outcome fields for enforcement analysis.
- Return JSON matching the `DecisionOutcome` schema.

Rules:
- Classify outcome as adverse, neutral, non_punishment, or unknown.
- Cite evidence spans for outcome statements.
- Avoid legal speculation beyond explicit text.
