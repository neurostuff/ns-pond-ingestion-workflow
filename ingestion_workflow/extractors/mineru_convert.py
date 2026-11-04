"""MinerU PDF conversion: parsing a PDF, and reading its middle JSON into text and tables.

MinerU's advanced tier reads each page from its image with a vision-language model, and
`ocr` mode takes the prose from the image too, so a text layer that maps the minus to 2
and `=` to 5 reaches neither its tables nor its text, where Docling reads that layer.

A worker process holds one parser, pinned to one GPU, and parses PDF after PDF with it;
the model loads on the first. MinerU is the `mineru` extra (README).
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from dataclasses import dataclass, field
from io import StringIO
from multiprocessing.util import Finalize
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.extractors.docling_convert import free_cuda_devices

logger = logging.getLogger(__name__)

#: The tier and mode the comparison against Docling measured
#: (wiki PR-189-MinerU-PDF-Extractor).
TIER = "advanced"
OCR_MODE = "ocr"
#: MinerU's vLLM engine held 6.9 GiB of an 8 GiB card.
MIN_FREE_GPU_MIB = 7000

#: Blocks that are page furniture, not the article.
_FURNITURE = {"header", "footer", "page_number", "aside_text"}
#: Blocks whose body is a picture: their captions go to the figure legends, and their
#: body (text read off the picture, or values read off a chart) goes nowhere.
_FIGURES = {"image", "chart"}

#: This worker process's parser (`open_parser`).
_parser: Any = None


@dataclass
class MineruTable:
    caption: str
    footer: str
    #: The table's HTML, its leading header rows marked `<th>` (`mark_header_rows`).
    html: str


@dataclass
class MineruDocument:
    text: str
    #: Each figure's id and legend, in order, as `figure_captions.dedupe` takes them.
    captions: List[Tuple[str, str]] = field(default_factory=list)
    tables: List[MineruTable] = field(default_factory=list)


def mineru_gpus() -> List[int]:
    """The GPUs with room for a MinerU worker; MinerU does not run on a CPU."""
    if importlib.util.find_spec("mineru") is None:
        raise ModelUnavailable("MinerU is not installed (the `mineru` extra)")
    gpus = free_cuda_devices(MIN_FREE_GPU_MIB)
    if not gpus:
        raise ModelUnavailable(f"MinerU: no GPU has {MIN_FREE_GPU_MIB} MiB free")
    return gpus


def open_parser() -> None:
    """This worker's parser, closed when the worker exits, which frees its GPU."""
    global _parser
    from mineru.parser import MinerUParser

    # vLLM compiles kernels with the ninja installed beside this Python.
    os.environ["PATH"] = os.pathsep.join([str(Path(sys.executable).parent),
                                          os.environ.get("PATH", "")])
    _parser = MinerUParser(tier=TIER, parse_mode=OCR_MODE)
    Finalize(_parser, _parser.close, exitpriority=10)


def parse_pdf(pdf: Path) -> Dict[str, Any]:
    """MinerU's middle JSON for one PDF, by this worker's parser."""
    return _parser.parse(str(pdf)).middle_json.to_dict()


def read_middle_json(middle: Mapping[str, Any]) -> MineruDocument:
    """The article's text, figure legends and tables, in reading order.

    The text holds every block but page furniture and figures, a table as its caption,
    its rows (cells joined by " | ") and its footnote. A figure's legend is its caption
    and its footnote together: MinerU takes "Figure 3." alone as the caption."""
    paragraphs: List[str] = []
    captions: List[Tuple[str, str]] = []
    tables: List[MineruTable] = []
    for page in middle.get("pages") or []:
        for block in page.get("blocks") or []:
            kind = block.get("type")
            if kind in _FURNITURE:
                continue
            parts = _parts(block)
            if kind in _FIGURES:
                legend = " ".join(_text(c) for k, c in parts if not k.endswith("_body")).strip()
                if legend:
                    captions.append((f"page-{page.get('page_idx')}-block-{block.get('index')}",
                                     legend))
                continue
            if kind == "table":
                caption = " ".join(_text(c) for k, c in parts if k == "table_caption").strip()
                footer = " ".join(_text(c) for k, c in parts if k == "table_footnote").strip()
                body = next((c for k, c in parts if k == "table_body"), "")
                html = mark_header_rows(body if isinstance(body, str) else "")
                tables.append(MineruTable(caption=caption, footer=footer, html=html))
                paragraphs.extend(p for p in (caption, table_text(html), footer) if p)
                continue
            if kind == "list":
                items = (_text(item.get("content")) for item in _parts_of(block))
                paragraphs.extend(item for item in items if item)
                continue
            text = _text(block.get("content"))
            if text:
                paragraphs.append(text)
    return MineruDocument(text="\n\n".join(paragraphs), captions=captions, tables=tables)


def _parts_of(block: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    content = block.get("content")
    return [c for c in content if isinstance(c, Mapping)] if isinstance(content, list) else []


def _parts(block: Mapping[str, Any]) -> List[Tuple[str, Any]]:
    """A composite block's parts as (type, content)."""
    return [(str(c.get("type", "")), c.get("content")) for c in _parts_of(block)]


def _text(content: Any) -> str:
    """A block's spans as one string: text as read, an inline equation as its LaTeX."""
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    out = []
    for span in content:
        if isinstance(span, Mapping):
            inner = span.get("content")
            out.append(inner if isinstance(inner, str) else _text(inner))
    return " ".join(" ".join(out).split())


def _rows(html: str) -> List[Any]:
    """The table's `<tr>` elements, or none when the HTML holds no table."""
    import lxml.html

    if "<tr" not in html:
        return []
    try:
        root = lxml.html.fragment_fromstring(html, create_parent="div")
    except Exception:  # noqa: BLE001 - lxml rejects some broken markup outright
        return []
    return root.findall(".//tr")


def _cells(row: Any) -> List[str]:
    return [c.text_content() for c in row if c.tag in ("td", "th")]


def _has_digit(row: Any) -> bool:
    return any(ch.isdigit() for cell in _cells(row) for ch in cell)


def _names_columns(row: Any, first: bool) -> bool:
    """Whether `row` reads as column names: no digit, and, below the first row, more than
    one cell; a lone cell there labels the rows under it ("Social > non-social")."""
    if _has_digit(row):
        return False
    return first or sum(1 for cell in _cells(row) if cell.strip()) > 1


def mark_header_rows(html: str) -> str:
    """`html` with its leading rows that name columns marked as header rows.

    MinerU writes every cell `<td>`, and the serialiser takes a table's header from
    `<th>`. A header names columns; the first row with a digit starts the body. The rule
    is the serialiser's own for a CSV, which has no markup either. A table whose every
    row is digitless is left as it is: there is no body to tell it from."""
    import lxml.html

    rows = _rows(html)
    if not any(_has_digit(row) for row in rows):
        return html
    head = 0
    while head < len(rows) and _names_columns(rows[head], first=head == 0):
        head += 1
    if head == 0:
        return html
    for row in rows[:head]:
        for cell in row.findall("./td"):
            cell.tag = "th"
    table = rows[0].getparent()
    while table is not None and table.tag != "table":
        table = table.getparent()
    if table is None:
        return html
    return lxml.html.tostring(table, encoding="unicode")


def table_text(html: str) -> str:
    """A table's rows as lines of cells joined by " | ", for the article text."""
    lines = []
    for row in _rows(html):
        cells = [" ".join(cell.split()) for cell in _cells(row)]
        if any(cells):
            lines.append(" | ".join(cells))
    return "\n".join(lines)


def table_frame(html: str) -> Optional[Any]:
    """The table as a DataFrame, its `<th>` rows as the columns, or None when pandas
    cannot read it."""
    import pandas as pd

    if "<tr" not in html:
        return None
    try:
        frames = pd.read_html(StringIO(html))
    except (ValueError, IndexError) as exc:
        logger.debug("pandas could not read a MinerU table: %s", exc)
        return None
    return frames[0] if frames else None


__all__ = [
    "MIN_FREE_GPU_MIB",
    "OCR_MODE",
    "TIER",
    "MineruDocument",
    "MineruTable",
    "mark_header_rows",
    "mineru_gpus",
    "open_parser",
    "parse_pdf",
    "read_middle_json",
    "table_frame",
    "table_text",
]
