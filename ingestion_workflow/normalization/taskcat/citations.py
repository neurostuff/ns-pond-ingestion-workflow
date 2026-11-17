"""In-text citation parsing and resolution against a paper's bibliography (taskcat/citations.py).

Numeric markers ("[15-18]", "(3, 5)") resolve by order; author-year markers ("Hariri et al., 2002a",
"Smith & Jones 1999") by first-author surname + year; a marker the source's markup linked resolves by its link
(`xrefs`). The Europe PMC fetch and its resolver are not ported: the bibliography is ns-pond's `references` artifact.
"""
import re
import unicodedata

DASH = "\u2013\u2014\u2012\u2212\\-"


def _ascii(s):
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()


def parse_numeric(s):
    """'[ 15 – 18 ]' / '(3, 5-7)' / '12' -> [15,16,17,18] etc. Returns [] if not numeric."""
    inner = re.sub(r"[\[\]()]", " ", s)
    if re.search(r"[A-Za-z]{3,}", inner):
        return []
    out = []
    for part in re.split(r"[,;]", inner):
        part = part.strip()
        m = re.fullmatch(rf"(\d+)\s*[{DASH}]\s*(\d+)", part)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if 0 < b - a < 60:
                out += list(range(a, b + 1))
        elif re.fullmatch(r"\d+", part):
            out.append(int(part))
    return out


AY = re.compile(r"([A-Z][A-Za-z'À-ſ\-]+(?:\s+(?:van|de|der|von|den|du|la|le)\s+[A-Z][\w\-]+)?)"
                r"(?:\s+(?:et\s+al\.?|and\s+colleagues)|\s*(?:&|and)\s+[A-Z][\w'\-]+)?[,\s]*\(?\s*((?:19|20)\d\d)([a-z]?)")


def parse_author_year(s):
    """'Hariri et al., 2002a; Delgado et al. 2000' -> [('hariri','2002','a'), ('delgado','2000','')]"""
    out = []
    for m in AY.finditer(s.replace("( ", "(").replace(" )", ")")):
        name = m.group(1).split()[-1]
        if name.lower() in {"see", "e.g", "eg", "for", "in", "and", "the", "fig", "table", "experiment"}:
            continue
        out.append((_ascii(name), m.group(2), m.group(3)))
    return out


def find_citations(text):
    """Find in-text citation markers in a span of text. Returns list of strings."""
    found = []
    for m in re.finditer(rf"\[\s*\d+(?:\s*[{DASH},;]\s*\d+)*\s*\]", text):
        found.append(m.group(0))
    for m in re.finditer(r"\(([^()]{3,200}?(?:19|20)\d\d[a-z]?[^()]{0,120})\)", text):
        if parse_author_year(m.group(1)):
            found.append(m.group(1))
    for m in re.finditer(r"([A-Z][A-Za-z\-']+(?:\s+et\s+al\.?|\s+(?:and|&)\s+[A-Z][\w\-']+)?)\s*\(\s*((?:19|20)\d\d[a-z]?)\s*\)", text):
        found.append(f"{m.group(1)}, {m.group(2)}")
    return found


# --------------------------------------------------------------------------- XML-bibliography resolution
def _sq(s):
    return re.sub(r"[\s()\[\],;.]+", "", (s or "")).lower()


def resolve_with_bib(citation, bib):
    """Resolve a verbatim marker against a parsed XML bibliography (taskcat.bibliography).
    Returns list of ref dicts. Order of attempts: exact xref text -> numeric order -> author-year."""
    if not bib or not bib.get("refs"):
        return []
    by_rid = {r["rid"]: r for r in bib["refs"]}
    out, seen = [], set()

    def add(r):
        if r and r["rid"] not in seen:
            seen.add(r["rid"]); out.append(r)
    # 1. exact xref text (Elsevier author-year cross-refs, JATS numbers)
    xmap = {}
    for x in bib.get("xrefs", []):
        xmap.setdefault(_sq(x["text"]), set()).update(x["rids"])
    parts = [p for p in re.split(r";", citation) if p.strip()]
    for p in parts + [citation]:
        for rid in sorted(xmap.get(_sq(p), ())):
            add(by_rid.get(rid))
    if out:
        return out
    # 2. numeric -> order (JATS numbering follows ref order in almost all journals)
    nums = parse_numeric(citation)
    if nums:
        by_order = {r["order"]: r for r in bib["refs"]}
        # prefer xref text '12' -> rid when present
        for n in nums:
            rids = xmap.get(str(n))
            if rids and len(rids) == 1:
                add(by_rid.get(next(iter(rids))))
            else:
                add(by_order.get(n))
        return out
    # 3. author-year
    for sur, yr, suffix in parse_author_year(citation):
        cands = [r for r in bib["refs"] if r.get("year") == yr and _ascii(r.get("first_author") or "").replace(" ", "")
                 .endswith(sur.replace(" ", ""))]
        if not cands:
            cands = [r for r in bib["refs"] if r.get("year") in (str(int(yr) - 1), str(int(yr) + 1)) and
                     _ascii(r.get("first_author") or "").endswith(sur)]
        if len(cands) > 1 and suffix:
            i = ord(suffix) - 97
            cands = cands[i:i + 1] if i < len(cands) else cands
        for r in cands[:3]:
            add(r)
    return out
