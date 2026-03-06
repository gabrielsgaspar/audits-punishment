# Validation and Error Framework

## Validation goals
- Quantify quality of extraction and linkage.
- Detect systematic bias by geography, time, and case type.
- Propagate uncertainty into final causal estimates.

## Gold labeling plan
- Build a stratified sample of audit findings and judicial decisions.
- Label extraction targets manually:
- Finding severity and category.
- Decision outcome class.
- Evidence span correctness.
- Label candidate links as true/false/uncertain.
- Use dual annotation for a subset to estimate inter-annotator agreement.

## Metrics
- Extraction:
- Field-level precision, recall, F1.
- Span overlap quality and exact-match rates for key fields.
- Calibration plots for confidence values.
- Linking:
- Precision@k and recall@k on candidate ranking.
- ROC/PR curves for match score thresholds.
- Error decomposition by source and year.

## Stress tests
- OCR noise and truncated text injection.
- Removed headings and altered formatting.
- Adversarial entity-name ambiguity.
- Court acronym and abbreviation variants.
- Temporal boundary perturbation tests.

## Bias checks
- Compare error rates across regions and court systems.
- Compare performance across older vs newer documents.
- Test whether lower-resource municipalities show higher mismatch rates.
- Monitor language-style sensitivity in legal text segments.

## Uncertainty propagation
- Keep confidence values in all intermediate outputs.
- Build weighted enforcement indices under threshold sets.
- Report point estimates with sensitivity bands.
- Present lower-bound and upper-bound estimates using strict/permissive links.

## Error logging standards
- Store every failed extraction attempt with reason code.
- Keep parsing and validation exceptions in structured tables.
- Include module name, schema version, and model identifier.

## Governance
- Freeze validation protocol before scaling to full corpus.
- Track all threshold changes in decision log.
- Recompute baseline metrics after major prompt/model updates.
