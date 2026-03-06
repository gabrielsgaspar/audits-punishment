# Econometrics Plan

## Objective
- Estimate whether audit exposure and audit severity causally affect legal punishment outcomes.
- Prioritize credible identification tied to lottery-based audit cohorts.

## Main identification options
- Option A: lottery-cohort intent-to-treat design.
- Option B: event-study around audit publication timing.
- Option C: severity-based heterogeneous treatment effects.
- Option D: IV sketches using lottery assignment intensity as instrument for measured finding severity.

## Preferred baseline: lottery cohorts
- Use municipality-level exposure from CGU lottery rounds.
- Compare exposed vs not-yet-exposed municipalities within temporal windows.
- Include fixed effects for municipality and time.
- Cluster inference at municipality or lottery-cohort level as appropriate.

## Key outcomes
- Punishment indicator in municipality-year.
- Count of adverse decision outcomes.
- Severity-weighted enforcement index.
- Time-to-first punishment after audit.

## Audit randomness diagnostics over time
- Check covariate balance by round and era.
- Test pre-treatment trends in legal outcomes.
- Evaluate changes in eligibility rules or program scope.
- Document potential deviations from strict random assignment.

## Event-study sketch
- Relative time bins around audit publication.
- Estimate dynamic treatment effects with leads and lags.
- Diagnose anticipation and delayed enforcement patterns.
- Report joint pre-trend tests.

## IV sketch
- First stage: lottery exposure predicts extracted audit severity/findings intensity.
- Second stage: predicted severity affects punishment outcomes.
- Interpretation depends on exclusion restrictions and stable measurement.
- Use as robustness, not sole identification strategy.

## Controls and covariates
- Municipality demographics and fiscal capacity.
- Judiciary congestion or court resource proxies.
- State-year trends and institutional controls.
- Baseline corruption risk proxies where available.

## Robustness package
- Alternative confidence thresholds for match links.
- Excluding ambiguous links and high-noise text sources.
- Placebo outcomes and placebo timing tests.
- Alternative aggregation rules for many-to-many links.

## Reporting standards
- Separate design-based and model-based assumptions.
- Include uncertainty due to extraction and matching.
- Provide replication scripts with fixed seeds and frozen manifests.
