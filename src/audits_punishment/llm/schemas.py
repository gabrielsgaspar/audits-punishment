"""Schema definitions for LLM extraction outputs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class EvidenceSpan(BaseModel):
    """Text evidence anchor used to justify extracted fields."""

    source_doc_id: str
    quote: str = Field(min_length=1)
    chunk_id: str | None = None
    page: int | None = None
    start_char: int | None = None
    end_char: int | None = None


class AuditFindingCard(BaseModel):
    """Structured representation of a single audit finding."""

    finding_id: str
    audit_report_id: str
    municipality: str
    state: str
    finding_type: str
    severity_level: Literal["low", "medium", "high"]
    summary: str
    amount_brl: float | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[EvidenceSpan] = Field(default_factory=list)


class MatchScore(BaseModel):
    """Scored candidate match between a finding and a judicial decision."""

    finding_id: str
    decision_id: str
    score: float = Field(ge=0.0, le=1.0)
    label: Literal["direct", "probable", "weak", "none"]
    rationale: str
    evidence: list[EvidenceSpan] = Field(default_factory=list)


class DecisionOutcome(BaseModel):
    """Structured outcome extracted from judicial decision text."""

    decision_id: str
    case_id: str
    court: str
    decision_date: str
    outcome_label: Literal["adverse", "neutral", "non_punishment", "unknown"]
    outcome_summary: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[EvidenceSpan] = Field(default_factory=list)
