# Match Score Prompt

Task:
- Evaluate whether an audit finding and judicial decision refer to the same underlying misconduct episode.
- Return valid JSON matching the `MatchScore` schema.

Rules:
- Use labels: direct, probable, weak, none.
- Score must be between 0 and 1.
- Include short rationale and text evidence from both sides.
