"""What a consistent catalog holds: each article is one paper, by every id it holds.

* `journal-dois`: no article holds two DOIs that are not a preprint server's (an
  alternate DOI is not counted);
* `pmids`, `pmcids`, `preprint-pmids`, `preprint-pmcids`, `base-studies`: no article
  holds two of a kind (a preprint's PMID beside its paper's is not a second PMID);
* `pmid-doi`: where PubMed gives a PMID's record a DOI that the registries give the
  same title, no other article holds that DOI, and the article holding the PMID
  holds it if it holds any DOI. A preprint's PMID is held to the same rule. A PMID
  the article's own PMC record gives, with a DOI the article holds, is consistent
  (PubMed gives an English edition's record its Spanish edition's DOI);
* `pmcid`: the PMID NCBI's ID converter gives a PMC record is held by the article
  holding the PMCID; so is the DOI it gives, under the same condition as `pmid-doi`
  (with the PMC record's PubMed title), or, for a record without a PMID, when the
  DOI is registered and the article holds another;
* `two-versions`: no two articles hold a preprint and the version of record Crossref
  relates it to;
* `download`: no ok download names, by its files, a paper another article holds and
  this one does not. A file names a paper by an ACE `<pmid>.html` name, a
  `pmcid_<n>/` directory, or the DOI in Elsevier or JATS XML;
* `download-unreadable`: an ok download's record and the XML it names can be read.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Collection, Dict, Iterator, List, Set, Tuple

from ingestion_workflow.models.ids import PREPRINT_PMCID, PREPRINT_PMID
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.preprints import is_preprint_doi

from .models import ALTERNATE_DOI, SECOND_KINDS
from .store import Catalog

#: Enough of an XML file to hold its front matter.
_XML_HEAD = 80_000

_ACE_FILE = re.compile(r"/(\d+)\.html$")
_PMC_DIR = re.compile(r"pmcid_(\d+)/")
_ELSEVIER_DOI = re.compile(r"<prism:doi>\s*([^<\s]+)")
_JATS_DOI = re.compile(r"<article-id[^>]*pub-id-type=\"doi\"[^>]*>\s*([^<\s]+)")

_ONE_EACH = (("pmids", "pmid"), ("pmcids", "pmcid"), ("preprint-pmids", PREPRINT_PMID),
             ("preprint-pmcids", PREPRINT_PMCID), ("base-studies", "neurostore"))


@dataclass(frozen=True)
class Violation:
    article_id: str
    rule: str
    detail: str


class Unreadable(Exception):
    """An ok download whose record or XML cannot be read."""


def suspects(catalog: Catalog) -> Set[str]:
    """Every live article holding two of an id kind other than an alternate DOI.

    Only these can break the one-of-a-kind rules, so they are checked with the
    articles a repair touched, which covers those rules for the whole catalog.
    """
    rows = catalog._conn.execute(
        "SELECT a.article_id FROM aliases a JOIN articles r ON r.id = a.article_id "
        "WHERE r.merged_into IS NULL AND a.kind != ? "
        "GROUP BY a.article_id, a.kind HAVING COUNT(*) > 1", (ALTERNATE_DOI,))
    return {row[0] for row in rows}


def neighbourhood(catalog: Catalog, article_ids: Collection[str]) -> Set[str]:
    """The live articles among `article_ids` and every live article a rule links to one:

    * the holder of any id they hold, in any spelling or second kind;
    * the holder of the DOI PubMed gives one of their PMIDs, of that record's PMC copy,
      and of a PMID PubMed gives for one of their DOIs;
    * the holder of the PMID and DOI of one of their PMC records;
    * the holder of a version Crossref relates one of their DOIs to;
    * the holder of a PMID whose PubMed record has the title of one of theirs: a
      preprint's record and its paper's.

    A change to one article can break another only through such a link, so a repair is
    checked over the neighbourhood of what it touched; each article's violations are the
    same whatever else is checked with it.
    """
    evidence = _evidence(catalog)
    live = {a for a in article_ids if catalog.live(a)}
    held = {a: _held(catalog, a) for a in live}
    dois = sorted({d for h in held.values() for d in h["doi"] + h[ALTERNATE_DOI]})
    pmids = sorted({p for h in held.values() for p in h["pmid"] + h[PREPRINT_PMID]})
    pmcids = sorted({c for h in held.values() for c in h["pmcid"] + h[PREPRINT_PMCID]})
    found = set(live)
    for article_id in live:
        for kind, value in catalog._conn.execute(
                "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)):
            found |= _sharing(catalog, kind, value)
    for pmid in evidence.pmids_for_dois(dois):
        found.add(catalog.holder("pmid", pmid))
    for summary in evidence.summaries(pmids).values():
        if summary.doi:
            found.add(catalog.holder("doi", normalize_doi(summary.doi) or summary.doi))
        if summary.pmcid:
            found.add(catalog.holder("pmcid", summary.pmcid))
    for record in evidence.pmc(pmcids).values():
        if record.pmid:
            found.add(catalog.holder("pmid", record.pmid))
        if record.doi:
            found.add(catalog.holder("doi", normalize_doi(record.doi) or record.doi))
    for doi in dois:
        found.update(catalog.holder("doi", v) for v in evidence.versions(doi))
    for pmid in pmids:
        found.update(catalog.holder("pmid", p) for p in evidence.same_title_pmids(pmid))
    return {a for a in found if a}


def violations(catalog: Catalog, article_ids: Collection[str]) -> List[Violation]:
    """Every way the live articles among `article_ids` break the rules above."""
    evidence = _evidence(catalog)
    live = sorted({a for a in article_ids if catalog.live(a)})
    held = {a: _held(catalog, a) for a in live}
    summaries = evidence.summaries(sorted({p for h in held.values()
                                           for p in h["pmid"] + h[PREPRINT_PMID]}))
    pmc = evidence.pmc(sorted({c for h in held.values() for c in h["pmcid"] + h[PREPRINT_PMCID]}))
    pmc_titles = evidence.summaries(sorted({r.pmid for r in pmc.values() if r.pmid}))
    found: List[Violation] = []
    for article_id in live:
        h = held[article_id]
        mine = set(h["doi"]) | set(h[ALTERNATE_DOI])

        def elsewhere(kind: str, value: str) -> bool:
            return catalog.holder(kind, value) not in (None, article_id)

        journal = sorted(d for d in h["doi"] if not is_preprint_doi(d))
        if len(journal) > 1:
            found.append(Violation(article_id, "journal-dois", f"holds {', '.join(journal)}"))
        for rule, kind in _ONE_EACH:
            if len(h[kind]) > 1:
                found.append(Violation(article_id, rule, f"holds {', '.join(h[kind])}"))
        own_pmc = {(r.pmid, normalize_doi(r.doi)) for c in h["pmcid"] + h[PREPRINT_PMCID]
                   if (r := pmc.get(c)) and r.doi}
        for pmid in h["pmid"] + h[PREPRINT_PMID]:
            summary = summaries.get(pmid)
            doi = normalize_doi(summary.doi) if summary else None
            if not doi or doi in mine or not evidence.same_paper(doi, summary.title):
                continue
            if any(p == pmid and d in mine for p, d in own_pmc):
                continue
            if elsewhere("doi", doi):
                found.append(Violation(article_id, "pmid-doi", f"pmid {pmid} is {doi} in "
                                       f"PubMed, which {catalog.holder('doi', doi)} holds"))
            elif h["doi"]:
                found.append(Violation(article_id, "pmid-doi", f"pmid {pmid} is {doi} in "
                                       f"PubMed, not {', '.join(h['doi'])}"))
        for pmcid in h["pmcid"] + h[PREPRINT_PMCID]:
            record = pmc.get(pmcid)
            if record is None:
                continue
            if record.pmid and catalog.holder("pmid", record.pmid) != article_id:
                holder = catalog.holder("pmid", record.pmid) or "no article"
                found.append(Violation(article_id, "pmcid", f"{pmcid} is PubMed {record.pmid} "
                                       f"in PMC, which {holder} holds"))
            doi = normalize_doi(record.doi) if record.doi else None
            if not doi or doi in mine:
                continue
            title = pmc_titles[record.pmid].title if record.pmid in pmc_titles else None
            evident = (evidence.same_paper(doi, title) if record.pmid
                       else evidence.title(doi) is not None)
            if evident and (elsewhere("doi", doi) or h["doi"]):
                holder = catalog.holder("doi", doi)
                found.append(Violation(article_id, "pmcid", f"{pmcid} is {doi} in PMC, "
                                       + (f"which {holder} holds" if holder else
                                          f"not {', '.join(h['doi'])}")))
        preprints = [d for d in h["doi"] if is_preprint_doi(d)]
        preprints += [normalize_doi(s.doi) for p in h[PREPRINT_PMID] + h["pmid"]
                      if (s := summaries.get(p)) and s.preprint and s.doi]
        for doi in dict.fromkeys(d for d in preprints if d):
            for version in evidence.versions(doi):
                if elsewhere("doi", version):
                    found.append(Violation(article_id, "two-versions", f"preprint {doi} is "
                                           f"{version}'s, which {catalog.holder('doi', version)} holds"))
        try:
            for source, named in _downloads_name(catalog, article_id):
                holders = {catalog.holder(k, v) for k, v in named}
                others = sorted(a for a in holders if a not in (None, article_id))
                if article_id not in holders and others:
                    ids = ", ".join(f"{k} {v}" for k, v in named)
                    found.append(Violation(article_id, "download", f"download/{source} names "
                                           f"{ids}, which {', '.join(others)} holds"))
        except Unreadable as exc:
            found.append(Violation(article_id, "download-unreadable", str(exc)))
    return found


def _sharing(catalog: Catalog, kind: str, value: str) -> Set[str]:
    """Every live article holding `value` as `kind`, as a kind of the same base, or as
    another spelling of the DOI."""
    base = SECOND_KINDS.get(kind, kind)
    kinds = [base] + [k for k, b in SECOND_KINDS.items() if b == base]
    values = {value} | ({normalize_doi(value)} if base == "doi" and normalize_doi(value) else set())
    rows = catalog._conn.execute(
        f"SELECT article_id FROM aliases WHERE kind IN ({','.join('?' * len(kinds))}) "
        f"AND value IN ({','.join('?' * len(values))})", (*kinds, *values))
    return {catalog.current_id(a) for (a,) in rows}


def _evidence(catalog: Catalog):
    if catalog.evidence is None:
        raise ValueError("checking identity needs the catalog opened with evidence")
    return catalog.evidence


def _held(catalog: Catalog, article_id: str) -> Dict[str, List[str]]:
    held = {}
    for kind in ("pmid", "pmcid", "doi", "neurostore", PREPRINT_PMID, PREPRINT_PMCID, ALTERNATE_DOI):
        values = catalog.aliases(article_id, kind)
        held[kind] = sorted({normalize_doi(v) or v for v in values}
                            if kind in ("doi", ALTERNATE_DOI) else values)
    return held


def _downloads_name(
    catalog: Catalog, article_id: str
) -> Iterator[Tuple[str, List[Tuple[str, str]]]]:
    """(source, the ids its files name) for each ok download whose files name any.

    Raises Unreadable for an ok download whose record or named XML is gone.
    """
    for artifact in catalog.artifacts_for_article(article_id):
        if artifact.stage != "download" or not artifact.ok:
            continue
        try:
            payload = catalog.payload(artifact) if artifact.blob else None
        except OSError as exc:
            raise Unreadable(f"download/{artifact.source}: its record cannot be read: {exc}") from exc
        if not isinstance(payload, dict):
            raise Unreadable(f"download/{artifact.source}: it has no record of its files")
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
                except OSError as exc:
                    raise Unreadable(f"download/{artifact.source}: {path} cannot be read") from exc
                if (m := pattern.search(text)) and (doi := normalize_doi(m.group(1))):
                    named.append(("doi", doi))
        if named:
            yield artifact.source, list(dict.fromkeys(named))
