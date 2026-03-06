from audits_punishment.llm.schemas import AuditFindingCard, DecisionOutcome, EvidenceSpan, MatchScore


def test_audit_finding_card_schema() -> None:
    span = EvidenceSpan(
        source_doc_id="audit_doc_1",
        quote="Irregular expenditure was identified.",
        page=3,
        start_char=120,
        end_char=154,
    )
    card = AuditFindingCard(
        finding_id="F-001",
        audit_report_id="R-2020-001",
        municipality="Example City",
        state="SP",
        finding_type="procurement_irregularity",
        severity_level="high",
        summary="Finding indicates severe procurement issues.",
        amount_brl=125000.0,
        confidence=0.91,
        evidence=[span],
    )
    assert card.severity_level == "high"
    assert card.evidence[0].page == 3


def test_match_score_schema() -> None:
    score = MatchScore(
        finding_id="F-001",
        decision_id="D-889",
        score=0.84,
        label="probable",
        rationale="Names and timeline align with moderate ambiguity.",
        evidence=[
            EvidenceSpan(source_doc_id="audit_doc_1", quote="Mayor X approved the contract."),
            EvidenceSpan(source_doc_id="decision_doc_1", quote="The contract approval is under review."),
        ],
    )
    assert 0.0 <= score.score <= 1.0
    assert score.label in {"direct", "probable", "weak", "none"}


def test_decision_outcome_schema() -> None:
    outcome = DecisionOutcome(
        decision_id="D-889",
        case_id="0001234-56.2020.8.26.0001",
        court="TJ-SP",
        decision_date="2023-09-12",
        outcome_label="adverse",
        outcome_summary="The decision imposed sanctions and confirmed liability.",
        confidence=0.88,
        evidence=[
            EvidenceSpan(source_doc_id="decision_doc_1", quote="Condeno o requerido...", page=7),
        ],
    )
    assert outcome.outcome_label == "adverse"
    assert outcome.confidence > 0.5
