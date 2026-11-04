"""MinerU PDF conversion: running its CLI, and reading its middle JSON into text and tables.

MinerU's advanced tier reads each page from its image with a vision-language model, and
`--ocr-mode ocr` takes the prose from the image too, so a text layer that maps the minus
to 2 and `=` to 5 reaches neither its tables nor its text, where Docling reads that layer.
MinerU runs as its own program, from its own environment: its torch and vLLM builds are
not Docling's.

One run loads its models once, for every PDF it is given, so a GPU gets one run over its
share. A PDF MinerU cannot read stops the run at that PDF and leaves the rest unread
(`mineru-kit` 4.0.12), so each PDF is first opened with PDFium, and a run that stops
anyway is started again on the PDFs it had not reached, without the one it stopped on.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import threading
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.extractors.docling_convert import free_cuda_devices

logger = logging.getLogger(__name__)

#: The tier and modes the comparison against Docling measured
#: (wiki PR-189-MinerU-PDF-Extractor).
MINERU_ARGS = ("--tier", "advanced", "--ocr-mode", "ocr", "--format", "middle_json")
#: MinerU's vLLM engine held 6.9 GiB of an 8 GiB card.
MIN_FREE_GPU_MIB = 7000
#: Loading the models took about a minute, and the slowest PDF in the comparison a minute;
#: a run past this budget is taken as stuck on the PDF it had reached.
STARTUP_SECONDS = 600
SECONDS_PER_PDF = 300
#: The tail of a failed run's output kept in its error.
_LOG_TAIL = 2000

#: Blocks that are page furniture, not the article.
_FURNITURE = {"header", "footer", "page_number", "aside_text"}
#: Blocks whose body is a picture: their captions go to the figure legends, and their
#: body (text read off the picture, or values read off a chart) goes nowhere.
_FIGURES = {"image", "chart"}

#: What `run_mineru` gives for each PDF: its middle JSON, or why MinerU read none.
Read = Union[Dict[str, Any], str]


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
    """The GPUs with room for a MinerU run; MinerU does not run on a CPU."""
    gpus = free_cuda_devices(MIN_FREE_GPU_MIB)
    if not gpus:
        raise ModelUnavailable(f"MinerU: no GPU has {MIN_FREE_GPU_MIB} MiB free")
    return gpus


def unreadable(pdf: Path) -> Optional[str]:
    """Why PDFium cannot open `pdf`, or None when it can."""
    import pypdfium2

    try:
        pypdfium2.PdfDocument(str(pdf)).close()
    except Exception as exc:  # noqa: BLE001 - PDFium's errors name the file's fault
        return f"PDFium cannot open the file: {exc}"
    return None


def run_mineru(
    pdfs: Sequence[Path],
    work_dir: Path,
    command: str,
    gpu: int,
) -> Dict[int, Read]:
    """Each PDF's middle JSON, or why MinerU read none, by position in `pdfs`.

    Raises `ModelUnavailable` when MinerU cannot run at all: its command is missing, or
    two runs in a row read nothing."""
    inputs, outputs = work_dir / "in", work_dir / "out"
    inputs.mkdir(parents=True, exist_ok=True)
    outputs.mkdir(parents=True, exist_ok=True)
    out: Dict[int, Read] = {}
    pending: List[int] = []
    for index, pdf in enumerate(pdfs):
        why = unreadable(pdf)
        if why:
            out[index] = why
            continue
        link = inputs / f"{index}.pdf"
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(Path(pdf).resolve())
        pending.append(index)

    stalled = False
    while pending:
        returncode, log = _run(command, [inputs / f"{i}.pdf" for i in pending], outputs, gpu)
        read = [i for i in pending if (outputs / f"{i}.json").is_file()]
        for index in read:
            out[index] = json.loads((outputs / f"{index}.json").read_text(encoding="utf-8"))
        pending = [i for i in pending if i not in out]
        if not pending:
            break
        if returncode == 0:
            for index in pending:
                out[index] = "MinerU finished without writing this PDF's JSON"
            break
        if not read and stalled:
            raise ModelUnavailable(f"MinerU read nothing in two runs in a row: {log}")
        stalled = not read
        # Inputs are read in the order given, so the run stopped on the first unread one.
        out[pending.pop(0)] = f"MinerU stopped on this PDF: {log}"
    return out


def _run(command: str, pdfs: Sequence[Path], outputs: Path, gpu: int) -> Tuple[int, str]:
    """One `mineru-kit parse` over `pdfs` on `gpu`: its return code and its output's tail."""
    env = dict(os.environ, CUDA_VISIBLE_DEVICES=str(gpu))
    # vLLM compiles kernels with the ninja installed beside MinerU.
    env["PATH"] = os.pathsep.join([str(Path(command).parent), env.get("PATH", "")])
    argv = [command, "parse", *map(str, pdfs), "--output", str(outputs), *MINERU_ARGS]
    timeout = STARTUP_SECONDS + SECONDS_PER_PDF * len(pdfs)
    try:
        # Its own session, so a stuck run is killed with the vLLM engine it started,
        # which would otherwise keep the GPU.
        process = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, start_new_session=True)
    except FileNotFoundError as exc:
        raise ModelUnavailable(f"MinerU: {command} not found ({exc})") from exc
    try:
        log, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        log, _ = process.communicate()
        return -1, f"timed out after {timeout} s: {(log or '')[-_LOG_TAIL:].strip()}"
    return process.returncode, (log or "")[-_LOG_TAIL:].strip()


def run_on_gpus(
    pdfs: Sequence[Path],
    work_dir: Path,
    command: str,
) -> Dict[int, Read]:
    """`run_mineru` over `pdfs`, split across the free GPUs, one run on each at a time."""
    gpus = mineru_gpus()
    shares = {gpu: list(range(n, len(pdfs), len(gpus))) for n, gpu in enumerate(gpus)}
    out: Dict[int, Read] = {}
    errors: List[BaseException] = []
    lock = threading.Lock()

    def share(gpu: int, indices: List[int]) -> None:
        try:
            read = run_mineru([pdfs[i] for i in indices], work_dir / f"gpu{gpu}", command, gpu)
        except BaseException as exc:  # noqa: BLE001 - raised again below, in this thread
            errors.append(exc)
            return
        with lock:
            out.update({indices[local]: answer for local, answer in read.items()})

    threads = [threading.Thread(target=share, args=(gpu, indices), daemon=True)
               for gpu, indices in shares.items() if indices]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]
    return out


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
    "MINERU_ARGS",
    "MineruDocument",
    "MineruTable",
    "mark_header_rows",
    "mineru_gpus",
    "read_middle_json",
    "run_mineru",
    "run_on_gpus",
    "table_frame",
    "table_text",
    "unreadable",
]
