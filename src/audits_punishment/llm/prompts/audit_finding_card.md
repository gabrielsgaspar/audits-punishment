# Audit Finding Card Prompt

Task:
- Read one audit text chunk.
- Extract one or more finding cards.
- Return valid JSON that matches the `AuditFindingCard` schema.

Rules:
- If evidence is weak, lower confidence and explain in summary.
- Include exact supporting quote snippets in evidence spans.
- Do not infer facts absent from the chunk.
