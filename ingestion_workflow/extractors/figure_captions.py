"""Figure captions, written into an extraction's text at one defined place.

Every extractor takes the figures out of the article before its text is built and
writes their captions after the text, under a "Figure legends" heading, one paragraph
per figure, in document order, each as the source words it (label, then caption). A
reader finds a caption by its span in the extraction payload (`figure_captions`), never
by searching the text: a caption also quoted in the body, or two figures sharing one
caption, would otherwise be found in the wrong place or twice.

Table captions are the table path's and are left where the extractor puts them.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

#: The heading the legends sit under. Markdown in a text that has Markdown headings
#: (pubget, Elsevier), a bare line otherwise (ACE, PDF), so `sectionize` reads the text as before.
HEADING = "Figure legends"

_ATX = re.compile(r"(?m)^#{1,6} \S")

#: (figure ids, caption text), one per distinct caption.
Caption = Tuple[List[str], str]


def squash(text: str) -> str:
    return " ".join((text or "").split())


def dedupe(captions: Iterable[Tuple[Optional[str], str]]) -> List[Caption]:
    """Captions in order, empty ones dropped, and one entry for figures whose captions
    read the same (a figure printed twice, or two panels captioned alike), with all their ids."""
    out: List[Caption] = []
    seen = {}
    for fig_id, text in captions:
        text = squash(text)
        if not text:
            continue
        if text in seen:
            ids = out[seen[text]][0]
            if fig_id and fig_id not in ids:
                ids.append(fig_id)
            continue
        seen[text] = len(out)
        out.append(([fig_id] if fig_id else [], text))
    return out


def append_legends(text: str, captions: Sequence[Caption]) -> Tuple[str, List[dict]]:
    """`text` with the captions after it, and each caption's `{"ids", "span"}` in the result.

    The text before the legends is unchanged, so an offset into it holds."""
    if not captions:
        return text, []
    heading = ("## " if _ATX.search(text) else "") + HEADING
    out = text.rstrip() + "\n\n" + heading
    spans = []
    for ids, caption in captions:
        out += "\n\n"
        spans.append({"ids": list(ids), "span": [len(out), len(out) + len(caption)]})
        out += caption
    return out, spans


# -- the sources ---------------------------------------------------------------


def _local(el) -> str:
    tag = el.tag if isinstance(el.tag, str) else ""
    return tag.rsplit("}", 1)[-1].split(":")[-1].lower()


def _remove_keeping_tail(el) -> None:
    parent = el.getparent()
    if parent is None:
        return
    if el.tail:
        prev = el.getprevious()
        if prev is not None:
            prev.tail = (prev.tail or "") + el.tail
        else:
            parent.text = (parent.text or "") + el.tail
    parent.remove(el)


#: Elements whose text is a block of its own: a space goes around them and nowhere else,
#: so "V<sub>1</sub>" reads "V1", as the extractors write inline markup in the body.
_BLOCKS = frozenset({"p", "para", "simple-para", "title", "label", "caption", "list", "list-item",
                     "def-item", "break", "br", "div", "li", "ul", "ol", "tr", "td", "th",
                     "h1", "h2", "h3", "h4", "h5", "h6"})


def _joined_text(el) -> str:
    parts = [el.text or ""] if isinstance(el.tag, str) else []
    for child in el:
        if isinstance(child.tag, str):
            block = _local(child) in _BLOCKS
            parts += [" ", _joined_text(child), " "] if block else [_joined_text(child)]
        parts.append(child.tail or "")
    return "".join(parts)


def _xml_caption(fig) -> str:
    return squash(" ".join(_joined_text(c) for c in fig if _local(c) in ("label", "caption")))


def take_xml_figures(root, figure_tag: str) -> List[Caption]:
    """Remove every `figure_tag` element (JATS `fig`, Elsevier `figure`) from the tree, and
    return their captions. A figure inside another is the outer one's panel and goes with it."""
    figs = [el for el in root.iter() if _local(el) == figure_tag]
    outer = [f for f in figs if not any(_local(a) == figure_tag for a in f.iterancestors())]
    captions = [(f.get("id"), _xml_caption(f)) for f in outer]
    for f in outer:
        _remove_keeping_tail(f)
    return dedupe(captions)


_FIGCAPTION = re.compile(r"(?is)<figcaption\b([^>]*)>(.*?)</figcaption\s*>")
_FIGURE_TAG = re.compile(r"(?is)<(/?)figure\b([^>]*)>")
_ID_ATTR = re.compile(r"""(?i)\bid\s*=\s*["']([^"']+)["']""")
_CLASS_ATTR = re.compile(r"""(?i)\bclass\s*=\s*["']([^"']*)["']""")
#: Table ids as publishers number them: Springer/Nature "Tab1", Frontiers "T1", Elsevier "tbl1".
_TABLE_ID = re.compile(r"(?i)^(tab|tbl|table|t)[-_]?\d")


def _figures(html: str) -> List[Tuple[int, int, str]]:
    """Each `<figure>` element's (start, end, attributes), nested ones too."""
    out, open_ = [], []
    for m in _FIGURE_TAG.finditer(html):
        if not m.group(1):
            open_.append(m)
        elif open_:
            start = open_.pop()
            out.append((start.start(), m.end(), start.group(2)))
    return out


def _is_table(html: str, figure: Tuple[int, int, str]) -> bool:
    start, end, attrs = figure
    fig_id, cls = _ID_ATTR.search(attrs), _CLASS_ATTR.search(attrs)
    return bool((fig_id and _TABLE_ID.match(fig_id.group(1)))
                or (cls and re.search(r"(?i)\btable\b", cls.group(1)))
                or re.search(r"(?i)<table\b", html[start:end]))


def _html_text(fragment: str) -> str:
    import lxml.html

    if not fragment.strip():
        return ""
    return _joined_text(lxml.html.fragment_fromstring(fragment, create_parent="div"))


def take_html_figures(html: str) -> Tuple[str, List[Caption]]:
    """The page with every figure's `<figcaption>` cut out, the rest byte for byte as it was
    (the publisher parsers match on it), and the captions' texts.

    A table set in a `<figure>` (Springer's `<figure id="Tab1">`) keeps its caption: ACE reads
    the table's label from it."""
    figures = _figures(html)
    captions, cuts = [], []
    for m in _FIGCAPTION.finditer(html):
        inside = [f for f in figures if f[0] < m.start() and m.end() <= f[1]]
        figure = max(inside, key=lambda f: f[0]) if inside else None
        if figure and _is_table(html, figure):
            continue
        fig_id = _ID_ATTR.search(m.group(1)) or (figure and _ID_ATTR.search(figure[2]))
        captions.append((fig_id.group(1) if fig_id else None, _html_text(m.group(2))))
        cuts.append(m.span())
    for start, end in reversed(cuts):
        html = html[:start] + " " + html[end:]
    return html, dedupe(captions)


__all__ = ["HEADING", "Caption", "append_legends", "dedupe", "take_html_figures", "take_xml_figures"]
