"""A held PDF: the correspondence notes in the text its extraction gave (`notes`).

The note is read from the extracted text, never from the PDF again. 109 of 120 PDF texts
sampled on beast on 2026-10-23 print an email; nearly all of them in a note.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from study_schema.models.paper_parse import MetadataSource

from ingestion_workflow.models.metadata import CorrespondingAuthor

from .common import authors
from .notes import from_text


def from_pdf_text(text_path: Path) -> List[CorrespondingAuthor]:
    return authors(from_text(text_path.read_text(encoding="utf-8", errors="replace")),
                   MetadataSource.pdf)
