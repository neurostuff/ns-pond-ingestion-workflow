"""What a consistent catalog holds: each article is one paper, by every id it holds.

* `journal-dois`: no article holds two DOIs that are not a preprint server's;
* `pmids`, `pmcids`: no article holds two PMIDs, or two PMCIDs (a preprint's PMID
  beside its paper's is not a second);
* `pmid-doi`: where PubMed gives a PMID's record a DOI, no other article holds it, and
  the article holding the PMID holds it if it holds any DOI and the DOI is registered:
  a few PubMed records carry a DOI with a typo, which no paper has. A preprint's
  PMID is held to the same rule;
* `download`: no ok download names, by its files, a paper another article holds and
  this one does not. A file names a paper by an ACE `<pmid>.html` name, a
  `pmcid_<n>/` directory, or the DOI in Elsevier or JATS XML.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Collection, Dict, Iterator, List, Optional, Set, Tuple

from ingestion_workflow.models.ids import PREPRINT_PMID, Identifier
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.preprints import is_preprint_doi

from .store import Catalog

#: Enough of an XML file to hold its front matter.
_XML_HEAD = 80_000

_ACE_FILE = re.compile(r"/(\d+)\.html$")
_PMC_DIR = re.compile(r"pmcid_(\d+)/")
_ELSEVIER_DOI = re.compile(r"<prism:doi>\s*([^<\s]+)")
_JATS_DOI = re.compile(r"<article-id[^>]*pub-id-type=\"doi\"[^>]*>\s*([^<\s]+)")


@dataclass(frozen=True)
class Violation:
    article_id: str
    rule: str
    detail: str


def suspects(catalog: Catalog) -> Set[str]:
    """Every live article holding two PMIDs, two PMCIDs or two DOIs.

    Only these can break the first three rules' counts, so checking them with the
    articles a repair touched covers those rules for the whole catalog.
    """
    rows = catalog._conn.execute(
        "SELECT a.article_id FROM aliases a JOIN articles r ON r.id = a.article_id "
        "WHERE r.merged_into IS NULL AND a.kind IN ('pmid', 'pmcid', 'doi') "
        "GROUP BY a.article_id, a.kind HAVING COUNT(*) > 1")
    return {row[0] for row in rows}


def violations(
    catalog: Catalog, article_ids: Collection[str], registered: Callable[[str], Optional[bool]]
) -> List[Violation]:
    """Every way the live articles among `article_ids` break the rules above.

    The PMIDs are asked of PubMed, through the lookup the catalog was opened with;
    `registered` says whether a DOI PubMed gives exists (None: it cannot say, which
    counts as yes), asked only of one the article does not hold.
    """
    if catalog.pubmed_dois is None:
        raise ValueError("checking PMIDs needs the catalog opened with pubmed_dois")
    live = sorted({a for a in article_ids if catalog.live(a)})
    held = {a: _held(catalog, a) for a in live}
    pmids = sorted({p for h in held.values() for p in h["pmid"] + h[PREPRINT_PMID]})
    answers = catalog.pubmed_dois(pmids) if pmids else {}
    pubmed = {p: normalize_doi(d) for p, d in answers.items() if d}
    found: List[Violation] = []
    for article_id in live:
        h = held[article_id]
        journal = sorted({d for d in h["doi"] if not is_preprint_doi(d)})
        singular = (("journal-dois", journal), ("pmids", h["pmid"]), ("pmcids", h["pmcid"]))
        for rule, values in singular:
            if len(values) > 1:
                found.append(Violation(article_id, rule, f"holds {', '.join(values)}"))
        for pmid in h["pmid"] + h[PREPRINT_PMID]:
            doi = pubmed.get(pmid)
            if doi is None:
                continue
            holder = _holder(catalog, "doi", doi)
            if holder not in (None, article_id):
                found.append(Violation(article_id, "pmid-doi",
                                       f"pmid {pmid} is {doi} in PubMed, which {holder} holds"))
            elif h["doi"] and doi not in h["doi"] and registered(doi) is not False:
                found.append(Violation(article_id, "pmid-doi", f"pmid {pmid} is {doi} in PubMed, "
                                       f"not {', '.join(h['doi'])}"))
        for source, named in _downloads_name(catalog, article_id):
            holders = {_holder(catalog, k, v) for k, v in named}
            others = sorted(a for a in holders if a not in (None, article_id))
            if article_id not in holders and others:
                ids = ", ".join(f"{k} {v}" for k, v in named)
                found.append(Violation(article_id, "download", f"download/{source} names "
                                       f"{ids}, which {', '.join(others)} holds"))
    return found


def _held(catalog: Catalog, article_id: str) -> Dict[str, List[str]]:
    held: Dict[str, List[str]] = {"pmid": [], "pmcid": [], "doi": [], PREPRINT_PMID: []}
    for kind in held:
        values = catalog.aliases(article_id, kind)
        held[kind] = sorted({normalize_doi(v) or v for v in values} if kind == "doi" else values)
    return held


def _holder(catalog: Catalog, kind: str, value: str) -> Optional[str]:
    ref = catalog.resolve(Identifier(**{kind: value}))
    return ref.id if ref else None


def _downloads_name(
    catalog: Catalog, article_id: str
) -> Iterator[Tuple[str, List[Tuple[str, str]]]]:
    """(source, the ids its files name) for each ok download whose files name any."""
    for artifact in catalog.artifacts_for_article(article_id):
        if artifact.stage != "download" or not artifact.ok or not artifact.blob:
            continue
        payload = catalog.payload(artifact) or {}
        named: List[Tuple[str, str]] = []
        for entry in payload.get("files") or []:
            path = str(entry.get("file_path") or "")
            if artifact.source == "ace" and (m := _ACE_FILE.search(path)):
                named.append(("pmid", m.group(1)))
            if m := _PMC_DIR.search(path):
                named.append(("pmcid", f"PMC{m.group(1)}"))
            elsevier = artifact.source == "elsevier" and path.endswith("content.xml")
            pattern = (_ELSEVIER_DOI if elsevier
                       else _JATS_DOI if path.endswith("article.xml") else None)
            if pattern is not None:
                try:
                    with open(Path(path), "rb") as f:
                        text = f.read(_XML_HEAD).decode("utf-8", "replace")
                except OSError:
                    continue
                if (m := pattern.search(text)) and (doi := normalize_doi(m.group(1))):
                    named.append(("doi", doi))
        if named:
            yield artifact.source, list(dict.fromkeys(named))
