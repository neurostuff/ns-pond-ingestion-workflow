"""MinerU PDF conversion: parsing a PDF, and reading its middle JSON into text and tables.

MinerU's advanced tier reads each page from its image with a vision-language model, and
`ocr` mode takes the prose from the image too, so a text layer that maps the minus to 2
and `=` to 5 reaches neither its tables nor its text, where Docling reads that layer.

A worker process holds one parser, pinned to one GPU, and parses PDF after PDF with it;
the model loads on the first. MinerU is the `mineru` extra (README).
"""

from __future__ import annotations

import copy
import importlib.util
import logging
import os
import re
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


#: How a worker whose parser's engine died exits: `bounded` then starts one with a new engine.
ENGINE_DEAD_EXIT = 75


def parse_pdf(pdf: Path) -> Dict[str, Any]:
    """MinerU's middle JSON for one PDF, by this worker's parser.

    When the parser's vLLM engine has died (the kernel kills it first when the machine runs
    out of memory), every later PDF fails the same way in seconds and goes to the next
    engine. The worker exits instead: this PDF fails alone, and its replacement starts a new
    engine."""
    try:
        return _parser.parse(str(pdf)).middle_json.to_dict()
    except Exception as exc:
        if engine_dead(exc):
            logger.error("MinerU's engine died reading %s; this worker exits", pdf.name)
            sys.stderr.flush()
            os._exit(ENGINE_DEAD_EXIT)
        raise


def engine_dead(exc: Optional[BaseException]) -> bool:
    """Whether `exc`, or what it was raised from, is vLLM's `EngineDeadError`: matched by
    name, since the parent process has no vLLM to import it from."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        if type(exc).__name__ == "EngineDeadError":
            return True
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return False


def read_middle_json(middle: Mapping[str, Any]) -> MineruDocument:
    """The article's text, figure legends and tables, in reading order.

    The text holds every block but page furniture and figures, a table as its caption,
    its rows (cells joined by " | ") and its footnote. A figure's legend is its caption
    and its footnote together: MinerU takes "Figure 3." alone as the caption."""
    paragraphs: List[str] = []
    captions: List[Tuple[str, str]] = []
    tables: List[MineruTable] = []
    last_table_page: Optional[int] = None
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
                paragraphs.extend(p for p in (caption, table_text(html), footer) if p)
                page_idx = page.get("page_idx")
                if (not caption and tables and isinstance(page_idx, int)
                        and isinstance(last_table_page, int) and page_idx == last_table_page + 1):
                    caption, html = continued(tables[-1], html, caption)
                tables.append(MineruTable(caption=caption, footer=footer, html=html))
                last_table_page = page_idx
                continue
            if kind == "list":
                items = (_text(item.get("content")) for item in _parts_of(block))
                paragraphs.extend(item for item in items if item)
                continue
            text = _text(block.get("content"))
            if text:
                paragraphs.append(text)
    return MineruDocument(text="\n\n".join(paragraphs), captions=captions, tables=tables)


def continued(previous: MineruTable, html: str, caption: str) -> Tuple[str, str]:
    """(caption, html) for a table that may be `previous` carried over a page break.

    MinerU reads each page on its own, so a table running onto the next page comes out
    as two: the first with its caption and header, the second with neither. Read alone,
    the second piece has no column names, and the extractor took its cluster size and
    statistic for x and y (doi:10.1002/jnr.25176, Table 2's right hemisphere; 71 such
    pieces read shifted in the production catalog, 2026-11-19). A captionless piece with
    no header of its own, on the page after a table of the same width, gets that
    table's header rows and its caption marked continued, as MinerU's own continued
    pieces read ("Table 1. (continued)"). Each piece keeps its own rows.

    Equal width is not enough: a table with an empty leading column has the width of
    one shifted by a column, and its header over the other would name the wrong numbers.
    Where the previous table has rows of its own, each column must hold the same kind of
    cell, number or text, in both. A previous table that is only a header -- MinerU often
    leaves the header at the foot of one page and every row on the next -- has nothing
    to compare, and width decides."""
    if _header_rows(html, after_titles=True) or not _header_rows(previous.html):
        return caption, html
    width = _width(html)
    if width != _width(previous.html):
        return caption, html
    head_kinds = _column_kinds(_rows(previous.html)[len(_header_rows(previous.html)):], width)
    if any(head_kinds) and not _same_columns(head_kinds, _column_kinds(_rows(html), width)):
        return caption, html
    import lxml.html

    rows = _rows(html)
    if not rows:
        return caption, html
    body = rows[0].getparent()
    for row in reversed(_header_rows(previous.html)):
        body.insert(0, copy.deepcopy(row))
    table = body
    while table is not None and table.tag != "table":
        table = table.getparent()
    if table is None:
        return caption, html
    label = previous.caption.removesuffix(" (continued)")
    return (f"{label} (continued)" if label else ""), lxml.html.tostring(table, encoding="unicode")


#: A cell that is nothing but a number: data, never a column name. A digit alone is not
#: enough -- a header names "Cluster Size (mm^2)".
_BARE_NUMBER = re.compile(r"^[-+]?\d+(?:[.,]\d+)?$")


def _header_rows(html: str, after_titles: bool = False) -> List[Any]:
    """The table's leading rows that name columns: no cell a bare number, and more than
    one cell filled -- a lone cell labels a group of rows ("Right Hemisphere").

    With `after_titles`, lone-cell rows at the top are passed over first: a piece that
    opens "Table 1. (Continued)" and then names its columns has a header of its own."""
    rows = _rows(html)
    if after_titles:
        while rows and not _numbered(rows[0]) and _filled(rows[0]) <= 1:
            rows = rows[1:]
    head = []
    for row in rows:
        if _numbered(row) or _filled(row) <= 1:
            break
        head.append(row)
    return head


def _numbered(row: Any) -> bool:
    return any(_BARE_NUMBER.match(" ".join(cell.split())) for cell in _cells(row))


def _filled(row: Any) -> int:
    return sum(1 for cell in _cells(row) if cell.strip())


#: A cell that reads as a value: a number, signed, bounded or approximate (`< 0.001`).
_NUMERIC = re.compile(r"^[-+<>=~±]?\s*\.?\d")


def _column_kinds(rows: List[Any], width: int) -> List[Optional[str]]:
    """Per column, "number" or "text" by what most of its filled cells hold, or None when
    none is filled. Only rows with one cell per column are read: a span moves the rest."""
    kinds: List[List[bool]] = [[] for _ in range(width)]
    for row in rows:
        cells = [" ".join(cell.split()) for cell in _cells(row)]
        if len(cells) != width:
            continue
        for i, cell in enumerate(cells):
            if cell:
                kinds[i].append(bool(_NUMERIC.match(cell)))
    return [None if not k else ("number" if 2 * sum(k) >= len(k) else "text") for k in kinds]


def _same_columns(head: List[Optional[str]], piece: List[Optional[str]]) -> bool:
    """Whether every column both bodies fill holds the same kind in each, and at least two
    can be compared."""
    both = [(a, b) for a, b in zip(head, piece) if a and b]
    return len(both) >= 2 and all(a == b for a, b in both)


def _width(html: str) -> int:
    """The table's width in columns: its widest row, spans counted."""
    def span(cell: Any) -> int:
        try:
            return max(1, int(cell.get("colspan") or 1))
        except ValueError:
            return 1
    return max((sum(span(c) for c in row if c.tag in ("td", "th")) for row in _rows(html)),
               default=0)


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
