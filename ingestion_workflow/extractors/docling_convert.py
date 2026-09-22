"""Docling PDF conversion, isolated so the heavy import stays lazy.

Structure comes from Docling, characters come from the PDF's own text layer.
That split is not cosmetic: on real publisher PDFs Docling silently drops
leading minus signs (turning a peak at -37 into one at 37) and renders decimal
points as colons (4.63 -> "4 : 63"), while PyPDFium reads the same glyphs
correctly. Wrong coordinates are worse than no coordinates, so every table cell
is re-read from the text layer.

OCR is off: these are born-digital PDFs, and OCR is both slower and worse on
numeric table cells than the embedded text layer.
"""

from __future__ import annotations

import functools
import logging
import re
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_MINUS_TOKENS = (
    "−",  # minus sign
    "‐",  # hyphen
    "‑",  # non-breaking hyphen
    "‒",  # figure dash
    "–",  # en dash
    "—",  # em dash
    "―",  # horizontal bar
    "⁃",  # hyphen bullet
    "⁻",  # superscript minus
    "₋",  # subscript minus
    "﹣",  # small hyphen-minus
    "－",  # fullwidth hyphen-minus
)

# Docling emits GLYPH<0> where a leading minus failed to map.
_MINUS_GLYPH = re.compile(r"GLYPH<0>\s*(?=\d)")
_DASH_BEFORE_DIGIT = re.compile(r"-(\s+)(?=\d)")

# Docling's cell bbox is drawn around the glyphs it recognised, so a minus sign
# it failed to read falls just outside it. Widening the query box to the left
# recovers the sign; the result is only accepted when it adds exactly a sign
# character, so the pad cannot pull in a neighbouring cell's content.
_SIGN_PAD = 3.0
_SIGN_CHARS = ("-", "+") + _MINUS_TOKENS


def normalize_text_tokens(text: Any) -> Any:
    """Replace Docling GLYPH tokens and typographic dashes with plain ASCII."""
    if not isinstance(text, str):
        return text

    cleaned = _MINUS_GLYPH.sub("-", text)
    for token in _MINUS_TOKENS:
        cleaned = cleaned.replace(token, "-")
    return _DASH_BEFORE_DIGIT.sub("-", cleaned)


@functools.lru_cache(maxsize=1)
def _converter() -> Any:
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import (
        TableFormerMode,
        ThreadedPdfPipelineOptions,
    )
    from docling.document_converter import DocumentConverter, PdfFormatOption

    pdf_options = ThreadedPdfPipelineOptions(
        do_ocr=False,
        do_formula_enrichment=False,
        do_code_enrichment=False,
        generate_page_images=False,
        generate_picture_images=False,
        generate_table_images=False,
    )
    pdf_options.table_structure_options.mode = TableFormerMode.ACCURATE

    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options)}
    )


def convert_pdf(pdf_path: Path) -> Any:
    """Convert a PDF into a Docling document with table cells re-read from the text layer."""
    document = _converter().convert(str(pdf_path)).document
    repaired, checked = _restore_cells_from_text_layer(document, pdf_path)
    if checked:
        logger.debug(
            "%s: corrected %d/%d table cells from the PDF text layer",
            pdf_path.name,
            repaired,
            checked,
        )
    return document


def _restore_cells_from_text_layer(document: Any, pdf_path: Path) -> tuple[int, int]:
    """Replace each table cell's text with the PDF text layer's version.

    Returns (cells corrected, cells checked).
    """
    try:
        import pypdfium2 as pdfium
    except ImportError:
        logger.warning("pypdfium2 is not installed; table cell text left as Docling read it.")
        return (0, 0)

    repaired = 0
    checked = 0
    pdf = pdfium.PdfDocument(str(pdf_path))
    text_pages: dict[int, Any] = {}
    page_heights: dict[int, float] = {}

    try:
        for table in document.tables:
            provenance = (getattr(table, "prov", None) or [None])[0]
            page_no = getattr(provenance, "page_no", None)
            if page_no is None or not 1 <= page_no <= len(pdf):
                continue

            if page_no not in text_pages:
                page = pdf[page_no - 1]
                page_heights[page_no] = page.get_size()[1]
                text_pages[page_no] = page.get_textpage()

            for cell in getattr(getattr(table, "data", None), "table_cells", []) or []:
                checked += 1
                replacement = _cell_text_from_layer(
                    cell, text_pages[page_no], page_heights[page_no]
                )
                if replacement and replacement != cell.text:
                    cell.text = replacement
                    repaired += 1
    finally:
        for text_page in text_pages.values():
            text_page.close()
        pdf.close()

    return (repaired, checked)


def _cell_text_from_layer(cell: Any, text_page: Any, page_height: float) -> Optional[str]:
    """Read one cell's text out of the PDF text layer, by bounding box."""
    bbox = getattr(cell, "bbox", None)
    if bbox is None:
        return None

    if str(getattr(bbox, "coord_origin", "")).upper().endswith("TOPLEFT"):
        bottom, top = page_height - bbox.b, page_height - bbox.t
    else:
        bottom, top = bbox.b, bbox.t

    exact = _bounded_text(text_page, bbox.l, bottom, bbox.r, top)
    if not exact:
        return None

    padded = _bounded_text(text_page, bbox.l - _SIGN_PAD, bottom, bbox.r, top)
    if padded and padded != exact and padded.endswith(exact):
        prefix = padded[: -len(exact)]
        if prefix in _SIGN_CHARS:
            return padded

    return exact


def _bounded_text(
    text_page: Any, left: float, bottom: float, right: float, top: float
) -> Optional[str]:
    try:
        return text_page.get_text_bounded(
            left=left, bottom=bottom - 1, right=right + 1, top=top + 1
        ).strip()
    except Exception:
        return None


__all__ = ["convert_pdf", "normalize_text_tokens"]
