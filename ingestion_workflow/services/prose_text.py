"""The Methods and Results of a downloaded article, read straight from the download.

Prose coordinates are often in papers whose tables -- if any -- never parsed,
so this does not depend on an extraction. JATS and Elsevier XML, publisher
HTML and PDF are turned into text with Markdown headings, table markup left
out, and `coordinate_space.sectionize` picks the Methods and Results. Figure
legends are kept wherever they sit: they report results. Without recognisable
sections the whole text is used.

`may_hold_coordinates` is the cheap test run on the raw file first, so that
the corpus is parsed only where a coordinate could be.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Optional

from ingestion_workflow.extractors.utils import normalize_minus
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.prose_passages import PATTERNS, find

#: Never read: tables are the table path's, the rest is not the paper's prose.
_SKIP_XML = {"table-wrap", "table", "table-wrap-foot", "tables", "ref-list", "bibliography",
             "bib-reference", "references", "fn-group", "ack", "acknowledgment", "meta", "ref-info",
             "item-toc", "graphic", "inline-graphic", "math", "mml", "formula", "disp-formula"}
_SECTION_XML = {"sec", "section"}
_TITLE_XML = {"title", "section-title"}
_BLOCK_XML = {"p", "para", "simple-para", "caption", "abstract", "list-item", "def"}
_CAPTION_XML = {"caption"}
_BODY_XML = {"body", "floats-group", "floats"}

_TABLE_BLOCK = re.compile(r"(?is)<((?:ce:)?table(?:-wrap)?|script|style)\b.*?</\1\s*>")
_TAG = re.compile(r"<[^>]+>")
_ENTITY_SPACE = re.compile(r"&(?:nbsp|#160|#xa0|thinsp|#8201|#x2009);", re.I)

KEPT_SECTIONS = ("methods", "results")


def may_hold_coordinates(raw: str) -> bool:
    """Whether a raw XML or HTML download could hold a coordinate in its prose.

    Table blocks and tags are stripped by regex, not parsed, and wherever any
    of the detector's patterns matches, the detector is put to the text around
    it. A no here is final; a yes still goes through the real reader.
    """
    text = _TAG.sub(" ", _TABLE_BLOCK.sub(" ", raw))
    text = " ".join(normalize_minus(_ENTITY_SPACE.sub(" ", text)).split())
    for _, pattern in PATTERNS:
        for m in pattern.finditer(text):
            if find(text[max(m.start() - 300, 0):m.end() + 120]):
                return True
    return False


def _local(el) -> str:
    tag = el.tag if isinstance(el.tag, str) else ""
    return tag.rsplit("}", 1)[-1].split(":")[-1].lower()


def _text_of(el) -> str:
    parts = [el.text or ""]
    for child in el:
        if _local(child) not in _SKIP_XML:
            parts.append(_text_of(child))
        parts.append(child.tail or "")
    return " ".join("".join(parts).split())


def _xml_text(path: Path) -> tuple[str, str]:
    from lxml import etree

    root = etree.parse(str(path), etree.XMLParser(recover=True, huge_tree=True)).getroot()
    if root is None:
        return "", ""
    body, legends = [], []

    def walk(el, depth: int, out: List[str]) -> None:
        local = _local(el)
        if local in _SKIP_XML:
            return
        if local in _SECTION_XML:
            title = next((c for c in el if _local(c) in _TITLE_XML), None)
            if title is not None:
                out.append("\n\n" + "#" * min(depth + 2, 6) + " " + _text_of(title) + "\n\n")
            for child in el:
                if child is not title:
                    walk(child, depth + 1, out)
            return
        if local in _CAPTION_XML:
            legends.append(_text_of(el))
            return
        if local in _BLOCK_XML:
            out.append(_text_of(el) + "\n\n")
            return
        for child in el:
            walk(child, depth, out)

    bodies = [el for el in root.iter() if _local(el) in _BODY_XML]
    for el in bodies or [root]:
        walk(el, 0, body)
    return "".join(body), "\n\n".join(t for t in legends if t)


def _html_text(path: Path) -> tuple[str, str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(path.read_text(errors="ignore"), "lxml")
    for el in soup(["table", "script", "style", "noscript", "nav", "header", "footer", "form", "svg"]):
        el.decompose()
    body, legends = [], []
    blocks = ("p", "li", "dd")
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "dd", "figcaption"]):
        text = " ".join(el.get_text(" ").split())
        if not text:
            continue
        if el.name == "figcaption":
            legends.append(text)
        elif el.name.startswith("h"):
            body.append("\n\n" + "#" * int(el.name[1]) + " " + text + "\n\n")
        elif not el.find_parent(blocks + ("figcaption",)):
            body.append(text + "\n\n")
    return "".join(body), "\n\n".join(legends)


def _pdf_text(path: Path) -> tuple[str, str]:
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(str(path))
    try:
        return "\n".join(doc[i].get_textpage().get_text_range() for i in range(len(doc))), ""
    finally:
        doc.close()


def read_download(path: Path, file_type: str) -> tuple[str, str]:
    """The article's text with Markdown headings, and its figure legends."""
    kind = (file_type or path.suffix.lstrip(".")).lower()
    if kind == "xml":
        return _xml_text(path)
    if kind in ("html", "htm"):
        return _html_text(path)
    if kind == "pdf":
        return _pdf_text(path)
    return "", ""


def kept_spans(text: str) -> tuple[list, str]:
    """`(start, end)` of the Methods and Results in `text`, and which were found;
    the whole text when neither section is recognised."""
    spans = [(a, b) for a, b, label in sectionize(text) if label in KEPT_SECTIONS]
    return (spans, "methods+results") if spans else ([(0, len(text))], "full text")


def main_file(files: Iterable[dict]) -> Optional[dict]:
    """The article's own file in a download: not a table, not metadata."""
    for f in files:
        path = Path(f.get("file_path", ""))
        kind = (f.get("file_type") or path.suffix.lstrip(".")).lower()
        if kind in ("xml", "html", "htm", "pdf") and "tables" not in path.parts[-2:] and path.is_file():
            return {**f, "file_type": kind}
    return None


__all__ = ["KEPT_SECTIONS", "kept_spans", "main_file", "may_hold_coordinates", "read_download"]
