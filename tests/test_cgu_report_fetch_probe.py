from __future__ import annotations

from audits_punishment.ingest.cgu_report_fetch_probe import detect_file_signature


def test_detect_file_signature_pdf() -> None:
    data = b"%PDF-1.7\n" + (b"0" * 20000)
    ext, ok = detect_file_signature(data, "application/pdf")
    assert ext == "pdf"
    assert ok is True


def test_detect_file_signature_docx_zip() -> None:
    data = b"PK\x03\x04" + (b"0" * 20000)
    ext, ok = detect_file_signature(data, "application/octet-stream")
    assert ext == "docx"
    assert ok is True


def test_detect_file_signature_doc_ole() -> None:
    data = bytes.fromhex("D0CF11E0") + (b"0" * 20000)
    ext, ok = detect_file_signature(data, "application/msword")
    assert ext == "doc"
    assert ok is True
