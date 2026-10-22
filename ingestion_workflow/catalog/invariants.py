"""What a consistent catalog holds: each article is one paper, by every id it holds.

* `journal-dois`: no article holds two DOIs that are not a preprint server's (an
  alternate DOI is not counted);
* `pmids`, `pmcids`, `preprint-pmids`, `preprint-pmcids`, `base-studies`: no article
  holds two of a kind (a preprint's PMID beside its paper's is not a second PMID);
* `pmid-doi`: where PubMed gives a PMID's record a DOI that the registries give the
  same paper, no other article holds that DOI, and the article holding the PMID
  holds it if it holds any DOI. A preprint's PMID is held to the same rule. A PMID
  the article's own PMC record gives, with a DOI the article holds, is consistent
  (PubMed gives an English edition's record its Spanish edition's DOI);
* `vor-doi`: the DOI a journal's PubMed record gives is held as the article's DOI,
  not only as an alternate: it is the DOI the article presents;
* `pmcid`: the PMID NCBI's ID converter gives a PMC record is held by the article
  holding the PMCID; so is the DOI it gives, under the same condition as `pmid-doi`
  (with the PMC record's PubMed record), or, for a record without a PMID (a meeting
  abstract's), when the DOI is registered, and then as the article's DOI, not an
  alternate;
* `alternate-doi`: the registries give an alternate DOI that is not a preprint
  server's the work of the article's journal DOI;
* `two-versions`: no two articles hold a preprint and its version of record, as
  Crossref relates them or as PubMed's records are one work by title, first author
  and year (`evidence.same_work`);
* `download`: no ok download names, by its files, a paper another article holds and
  this one does not. A file names a paper by an ACE `<pmid>.html` name, a
  `pmcid_<n>/` directory, or the DOI in Elsevier or JATS XML;
* `download-unreadable`: an ok download's record and the XML it names can be read;
* `evidence-unreadable`: the registry can be read for every DOI a rule asks about.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Collection, Dict, Iterator, List, Set, Tuple

from ingestion_workflow.models.ids import PREPRINT_PMCID, PREPRINT_PMID
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.preprints import is_preprint_doi

from .evidence import Unreadable as RegistryUnreadable
from .evidence import same_work
from .models import ALTERNATE_DOI, DUPLICATE_PMCID, DUPLICATE_PMID, SECOND_KINDS, Artifact
from .store import Catalog, _artifact

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
    """Every live article holding two of an id kind other than an alternate DOI or a
    duplicate record's.

    Only these can break the one-of-a-kind rules, so they are checked with the
    articles a repair touched, which covers those rules for the whole catalog.
    """
    rows = catalog._conn.execute(
        "SELECT a.article_id FROM aliases a JOIN articles r ON r.id = a.article_id "
        "WHERE r.merged_into IS NULL AND a.kind NOT IN (?, ?, ?) "
        "GROUP BY a.article_id, a.kind HAVING COUNT(*) > 1",
        (ALTERNATE_DOI, DUPLICATE_PMID, DUPLICATE_PMCID))
    return {row[0] for row in rows}


def neighbourhood(catalog: Catalog, article_ids: Collection[str]) -> Set[str]:
    """The live articles among `article_ids` and every live article a rule links to one:

    * the holder of any id they hold, in any spelling or second kind;
    * the holder of the DOI PubMed gives one of their PMIDs, of that record's PMC copy,
      and of a PMID PubMed gives for one of their DOIs;
    * the holder of the PMID and DOI of one of their PMC records;
    * the holder of a version Crossref relates one of their DOIs to;
    * the holder of the other version, by title, of one of their PubMed records;
    * an article whose download names an id they hold (`index_downloads` first).

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
    every_id = set()
    for article_id in live:
        for kind, value in catalog._conn.execute(
                "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)):
            found |= _sharing(catalog, kind, value)
            base = SECOND_KINDS.get(kind, kind)
            every_id.add((base, normalize_doi(value) or value if base == "doi" else value))
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
        found.update(catalog.holder("pmid", p) for p in evidence.title_versions(pmid))
    found |= _downloads_naming(catalog, every_id)
    return {a for a in found if a and catalog.live(a)}


def index_downloads(catalog: Catalog) -> int:
    """Read the ids the ok downloads not yet indexed name into `download_names`, for
    `neighbourhood`; return how many downloads were read. A download whose record or
    XML cannot be read names nothing it can be found by; `violations` reports it."""
    rows = catalog._conn.execute(
        "SELECT * FROM artifacts WHERE stage='download' AND status='ok' "
        "AND blob IS NOT NULL AND blob NOT IN (SELECT blob FROM download_names_read)").fetchall()
    read: Dict[str, List[Tuple[str, str]]] = {}
    for row in rows:
        artifact = _artifact(row)
        if artifact.blob in read:
            continue
        try:
            read[artifact.blob] = _named(catalog, artifact)
        except Unreadable:
            read[artifact.blob] = []
    record_download_names(catalog, read)
    return len(read)


def record_download_names(catalog: Catalog, read: Dict[str, List[Tuple[str, str]]]) -> None:
    """Keep the ids each download record's blob names, as `index_downloads` reads them."""
    with catalog._write() as conn:
        conn.executemany("INSERT OR IGNORE INTO download_names(blob, kind, value) VALUES(?, ?, ?)",
                         [(blob, kind, value) for blob, named in read.items()
                          for kind, value in named])
        conn.executemany("INSERT OR IGNORE INTO download_names_read(blob) VALUES(?)",
                         [(blob,) for blob in read])


def _downloads_naming(catalog: Catalog, ids: Collection[Tuple[str, str]]) -> Set[str]:
    """The articles holding an ok download whose files name one of `ids`."""
    (unread,) = catalog._conn.execute(
        "SELECT COUNT(*) FROM artifacts WHERE stage='download' AND status='ok' "
        "AND blob IS NOT NULL AND blob NOT IN (SELECT blob FROM download_names_read)").fetchone()
    if unread:
        raise ValueError(f"{unread:,} downloads are not indexed: run index_downloads first")
    blobs: Set[str] = set()
    for kind, value in ids:
        blobs.update(b for (b,) in catalog._conn.execute(
            "SELECT blob FROM download_names WHERE value=? AND kind=?", (value, kind)))
    if not blobs:
        return set()
    marks = ",".join("?" * len(blobs))
    return {catalog.current_id(a) for (a,) in catalog._conn.execute(
        f"SELECT article_id FROM artifacts WHERE stage='download' AND status='ok' "
        f"AND blob IN ({marks})", sorted(blobs))}


def violations(catalog: Catalog, article_ids: Collection[str]) -> List[Violation]:
    """Every way the live articles among `article_ids` break the rules above."""
    evidence = _evidence(catalog)
    live = sorted({a for a in article_ids if catalog.live(a)})
    held = {a: _held(catalog, a) for a in live}
    summaries = evidence.summaries(sorted({p for h in held.values()
                                           for p in h["pmid"] + h[PREPRINT_PMID]}))
    pmc = evidence.pmc(sorted({c for h in held.values() for c in h["pmcid"] + h[PREPRINT_PMCID]}))
    pmc_records = evidence.summaries(sorted({r.pmid for r in pmc.values() if r.pmid}))
    found: List[Violation] = []
    for article_id in live:
        h = held[article_id]
        dois, alternates = set(h["doi"]), set(h[ALTERNATE_DOI])
        mine = dois | alternates

        def elsewhere(kind: str, value: str) -> bool:
            return catalog.holder(kind, value) not in (None, article_id)

        def add(rule: str, detail: str) -> None:
            found.append(Violation(article_id, rule, detail))

        def asking(check: Callable[[], bool]) -> bool:
            # A registry that cannot be read for a DOI is reported, and the rule asking
            # about it is not judged.
            try:
                return check()
            except RegistryUnreadable as exc:
                add("evidence-unreadable", str(exc))
                return False

        journal = sorted(d for d in h["doi"] if not is_preprint_doi(d))
        if len(journal) > 1:
            add("journal-dois", f"holds {', '.join(journal)}")
        for rule, kind in _ONE_EACH:
            if len(h[kind]) > 1:
                add(rule, f"holds {', '.join(h[kind])}")
        own_pmc = {(r.pmid, normalize_doi(r.doi)) for c in h["pmcid"] + h[PREPRINT_PMCID]
                   if (r := pmc.get(c)) and r.doi}
        for pmid in h["pmid"] + h[PREPRINT_PMID]:
            summary = summaries.get(pmid)
            doi = normalize_doi(summary.doi) if summary else None
            if not doi or doi in dois:
                continue
            if not asking(lambda: evidence.same_paper(doi, summary)):
                continue
            if doi in alternates:
                if not summary.preprint and pmid in h["pmid"]:
                    add("vor-doi", f"pmid {pmid} is {doi} in PubMed, which it holds only as an "
                        "alternate DOI")
                continue
            if any(p == pmid and d in mine for p, d in own_pmc):
                continue
            if elsewhere("doi", doi):
                add("pmid-doi", f"pmid {pmid} is {doi} in PubMed, which "
                    f"{catalog.holder('doi', doi)} holds")
            elif h["doi"]:
                add("pmid-doi", f"pmid {pmid} is {doi} in PubMed, not {', '.join(h['doi'])}")
        for pmcid in h["pmcid"] + h[PREPRINT_PMCID]:
            record = pmc.get(pmcid)
            if record is None:
                continue
            if record.pmid and catalog.holder("pmid", record.pmid) != article_id:
                holder = catalog.holder("pmid", record.pmid) or "no article"
                add("pmcid", f"{pmcid} is PubMed {record.pmid} in PMC, which {holder} holds")
            doi = normalize_doi(record.doi) if record.doi else None
            if not doi or doi in dois:
                continue
            source = pmc_records.get(record.pmid) if record.pmid else None
            evident = asking(lambda: evidence.same_paper(doi, source) if record.pmid
                             else evidence.work(doi) is not None)
            if not evident:
                continue
            if doi in alternates:
                # A PMC record of the article's PubMed record may give the paper's other DOI;
                # one without a PMID is its own work, a meeting abstract's.
                if not record.pmid:
                    add("pmcid",
                        f"{pmcid} is {doi} in PMC, which it holds only as an alternate DOI")
            elif elsewhere("doi", doi) or h["doi"]:
                holder = catalog.holder("doi", doi)
                add("pmcid", f"{pmcid} is {doi} in PMC, "
                    + (f"which {holder} holds" if holder else f"not {', '.join(h['doi'])}"))
        for alternate in sorted(d for d in alternates if not is_preprint_doi(d)):
            for doi in journal:
                if asking(lambda: _other_work(evidence, alternate, doi)):
                    add("alternate-doi", f"alternate DOI {alternate} is another work than {doi}")
        preprints = [d for d in h["doi"] if is_preprint_doi(d)]
        preprints += [normalize_doi(s.doi) for p in h[PREPRINT_PMID] + h["pmid"]
                      if (s := summaries.get(p)) and s.preprint and s.doi]
        for doi in dict.fromkeys(d for d in preprints if d):
            for version in asking(lambda: evidence.versions(doi)) or ():
                if elsewhere("doi", version):
                    add("two-versions", f"preprint {doi} is {version}'s, which "
                        f"{catalog.holder('doi', version)} holds")
        for pmid in h["pmid"]:
            if (summary := summaries.get(pmid)) and summary.preprint:
                for other in evidence.title_versions(pmid):
                    if elsewhere("pmid", other):
                        add("two-versions", f"preprint PubMed {pmid} is PubMed {other}'s work, "
                            f"which {catalog.holder('pmid', other)} holds")
        try:
            for source, named in _downloads_name(catalog, article_id):
                holders = {catalog.holder(k, v) for k, v in named}
                others = sorted(a for a in holders if a not in (None, article_id))
                if article_id not in holders and others:
                    ids = ", ".join(f"{k} {v}" for k, v in named)
                    add("download",
                        f"download/{source} names {ids}, which {', '.join(others)} holds")
        except Unreadable as exc:
            add("download-unreadable", str(exc))
    return found


def _other_work(evidence, alternate: str, doi: str) -> bool:
    """Whether the registries give the two DOIs two works; an unregistered spelling is
    no other work."""
    first, second = evidence.work(alternate), evidence.work(doi)
    return first is not None and second is not None and not same_work(first, second)


def _sharing(catalog: Catalog, kind: str, value: str) -> Set[str]:
    """Every live article holding `value` as `kind`, as a kind of the same base, or as
    another spelling of the DOI."""
    base = SECOND_KINDS.get(kind, kind)
    kinds = [base] + [k for k, b in SECOND_KINDS.items() if b == base]
    values = {value}
    if base == "doi" and normalize_doi(value):
        values.add(normalize_doi(value))
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
    for kind in ("pmid", "pmcid", "doi", "neurostore",
                 PREPRINT_PMID, PREPRINT_PMCID, ALTERNATE_DOI):
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
        if artifact.stage == "download" and artifact.ok:
            named = _named(catalog, artifact)
            if named:
                yield artifact.source, named


def _named(catalog: Catalog, artifact: Artifact) -> List[Tuple[str, str]]:
    """The ids an ok download's files name. Raises Unreadable when its record or a
    named XML file cannot be read."""
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
    return list(dict.fromkeys(named))
