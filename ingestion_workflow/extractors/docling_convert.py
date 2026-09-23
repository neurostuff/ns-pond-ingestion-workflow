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
import os
import re
import subprocess
import sys
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


# One warm Docling converter reserves ~1.4 GiB; the headroom leaves room for a
# document with an unusually large page batch.
_MIN_FREE_GPU_MIB = 2500


#: Result of the torch probe, which costs an interpreter start.
_TORCH_CUDA_OK: Optional[tuple[bool, str]] = None

_PROBE = (
    "import torch;"
    "torch.zeros(1).cuda();"
    "print('ok')"
)


def _torch_can_use_cuda() -> tuple[bool, str]:
    """Whether torch can actually allocate on a GPU, asked in a subprocess.

    `nvidia-smi` reports the hardware; it says nothing about whether this
    torch build can drive it. A torch compiled for a newer CUDA than the
    installed driver lists every device and then refuses to initialise, so
    the two have to be asked separately.

    In a subprocess for the reason the caller is: initialising CUDA in this
    process would poison forked children.
    """
    global _TORCH_CUDA_OK
    if _TORCH_CUDA_OK is None:
        try:
            done = subprocess.run(
                [sys.executable, "-c", _PROBE],
                capture_output=True, text=True, timeout=120,
            )
            ok = done.returncode == 0 and "ok" in done.stdout
            _TORCH_CUDA_OK = (ok, (done.stderr or "").strip().splitlines()[-1] if not ok else "")
        except (OSError, subprocess.SubprocessError) as exc:
            _TORCH_CUDA_OK = (False, f"{type(exc).__name__}: {exc}")
    return _TORCH_CUDA_OK


def usable_cuda_devices() -> list[int]:
    """Indices of CUDA devices with enough free memory for a Docling worker.

    Probed with ``nvidia-smi`` rather than torch on purpose. The workers are
    forked or spawned from whichever process calls this, and a parent that has
    already initialised CUDA poisons forked children ("Cannot re-initialize
    CUDA in forked subprocess"). A subprocess probe keeps the parent clean.
    Devices busy with someone else's job are skipped rather than contended for.

    Hardware alone is not enough: a torch built for a newer CUDA than the
    driver lists every device and then refuses to initialise, so Docling falls
    back to the CPU silently and runs an order of magnitude slower. Returning
    no device when torch cannot use one makes that visible and stops workers
    being handed a GPU they cannot reach.
    """
    try:
        completed = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if completed.returncode != 0:
        return []

    usable = []
    for index, line in enumerate(completed.stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            free_mib = int(line)
        except ValueError:
            continue
        if free_mib >= _MIN_FREE_GPU_MIB:
            usable.append(index)

    if usable:
        ok, why = _torch_can_use_cuda()
        if not ok:
            logger.warning(
                "nvidia-smi reports %d usable GPU(s) but torch cannot use them, "
                "so Docling will run on the CPU: %s",
                len(usable),
                why or "unknown",
            )
            return []
    return usable


def cuda_device_count() -> int:
    """How many CUDA devices are free enough to run a Docling worker on."""
    return len(usable_cuda_devices())


def _select_device() -> str:
    """Pick this process's Docling device.

    Each worker is pinned to a single GPU by ``CUDA_VISIBLE_DEVICES`` before it
    gets here, so there is at most one device to choose and no way for two
    workers to collide on the same card.
    """
    try:
        import torch
    except ImportError:
        return "cpu"
    try:
        if torch.cuda.is_available() and torch.cuda.device_count() > 0:
            return "cuda:0"
    except Exception:  # pragma: no cover - driver/build mismatch
        logger.warning("CUDA probe failed; Docling will run on CPU.", exc_info=True)
    return "cpu"


@functools.lru_cache(maxsize=1)
def _converter() -> Any:
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import (
        AcceleratorOptions,
        TableFormerMode,
        ThreadedPdfPipelineOptions,
    )
    from docling.document_converter import DocumentConverter, PdfFormatOption

    device = _select_device()
    pdf_options = ThreadedPdfPipelineOptions(
        do_ocr=False,
        do_formula_enrichment=False,
        do_code_enrichment=False,
        generate_page_images=False,
        generate_picture_images=False,
        generate_table_images=False,
        accelerator_options=AcceleratorOptions(device=device),
    )
    pdf_options.table_structure_options.mode = TableFormerMode.ACCURATE
    logger.info("Docling converter on %s (pid %d)", device, os.getpid())

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


def _is_unusable(text: Optional[str]) -> bool:
    r"""True when a text-layer read carries C0 control characters.

    Some publisher fonts ship no usable ToUnicode mapping for the minus glyph,
    and PyPDFium then returns its raw character code: a peak at -48 reads back
    as "\x0448". Docling's own read of the same cell is correct, so a cell like
    this must keep Docling's text. Stripping the control character instead would
    turn -48 into 48 and move the peak to the other hemisphere -- the exact
    failure this whole re-read exists to prevent.
    """
    if not text:
        return False
    return any(ord(char) < 32 and char not in "\t\r\n" for char in text)


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
    if not exact or _is_unusable(exact):
        return None

    padded = _bounded_text(text_page, bbox.l - _SIGN_PAD, bottom, bbox.r, top)
    if padded and not _is_unusable(padded) and padded != exact and padded.endswith(exact):
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


__all__ = [
    "convert_pdf",
    "cuda_device_count",
    "normalize_text_tokens",
    "usable_cuda_devices",
]
