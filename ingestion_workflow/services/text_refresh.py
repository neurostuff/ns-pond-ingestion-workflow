"""Rebuild extractions' text from their downloads, leaving everything else about them alone.

A change to a text builder that touches only the text (pubget keeping superscripts,
every extractor writing the figure captions under "Figure legends") would, as a version bump, re-run every stage the extraction feeds -- triage, the
analyses model, upload -- though none of them reads the text. This rewrites the text
file in place instead: the extraction keeps its tables, its payload and its fingerprint.
The ns-pond corpus's copy (`processed/<source>/text.txt`) is replaced only where it is
byte-identical to the text it was copied from.

What is stored against the text moves with it: each rewrite carries the offset map from
the old text to the new, and `carried` turns it into one catalog write -- the extraction's
text hash, the passages' spans remapped. Passages a map cannot carry (a span inside
rewritten text, or a passage or hit whose characters an edit changed) are left on the
old hash, so the passages stage reads the text again; so are passages whose text gained
a caption holding a coordinate, which only a new read finds. The extraction's caption
spans are the rebuilt text's. The references and the sync are marked stale: the
references stage reads the new text again (a kept superscript, or a caption, can hold a
citation marker), its citations' offsets moved meanwhile; and the parse files' spans are
written again from the text.

The texts are all rewritten before the catalog is: a run stopped in between leaves the
new texts against the old hashes, which every reader of a span checks and refuses, and
the next run, seeing the texts unchanged, records their hashes and the passages are
read again rather than remapped.
"""

from __future__ import annotations

import hashlib
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.services.citations import JATS_SOURCES

logger = logging.getLogger(__name__)

#: The sources whose text a rebuild reproduces from the download: JATS through pubget's
#: stylesheet, Elsevier XML, and ACE's HTML (its fetched tables read from the extraction's
#: cache). Not a PDF: its text is docling's conversion, which is not kept.
REFRESHABLE_SOURCES = JATS_SOURCES + ("elsevier", "ace")


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class Job:
    article_id: str
    source: str
    text_path: Optional[str]
    #: The download's article file: article.xml (JATS), content.xml (Elsevier), the page (ACE).
    article_file: Optional[str]
    #: The extraction's PMID, which ACE needs for a page that does not print one.
    pmid: Optional[str] = None


@dataclass
class Result:
    article_id: str
    source: str
    status: str  # unchanged | rewritten | would_rewrite | no_text | no_download | failed: ...
    text_path: Optional[str] = None
    old_sha256: Optional[str] = None
    new_sha256: Optional[str] = None
    #: The offset map from the old text to the new, as its edits.
    edits: Optional[List[Tuple[int, int, int, int]]] = None
    #: The rebuilt text's caption spans (`ExtractedContent.figure_captions`).
    figure_captions: Optional[List[dict]] = None
    #: Whether a caption of the rebuilt text holds a coordinate, which a remap cannot add.
    captions_hold_coordinates: bool = False


def _read(path: Path) -> str:
    """The text as stored: `read_text` would turn a `\r` the builder kept into `\n`."""
    return path.read_bytes().decode("utf-8")


def _write_atomically(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".refresh-tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    try:
        os.chmod(tmp, path.stat().st_mode & 0o7777)
    except OSError:
        pass
    os.replace(tmp, path)


def rebuild(job: Job, write: bool) -> Result:
    """One extraction: its text rebuilt from its download, written when it changed and `write`."""
    result = Result(job.article_id, job.source, "unchanged", job.text_path)
    if not job.text_path or not Path(job.text_path).is_file():
        result.status = "no_text"
        return result
    if not job.article_file or not Path(job.article_file).is_file():
        result.status = "no_download"
        return result
    path = Path(job.text_path)
    old = _read(path)
    result.old_sha256 = sha256(old)
    try:
        new, result.figure_captions = build(job.source, Path(job.article_file), path, job.pmid)
    except Exception as exc:  # noqa: BLE001 - one unreadable article keeps its old text
        result.status = f"failed: {type(exc).__name__}: {exc}"[:300]
        return result
    result.new_sha256 = sha256(new)
    if new == old:
        result.figure_captions = None  # the payload's spans are the old text's, which stays
        return result
    if not new.strip():
        result.status = "failed: rebuilt text is empty"
        return result
    from ingestion_workflow.services.offsets import diff
    from ingestion_workflow.services.prose_passages import find

    result.captions_hold_coordinates = any(find(new[c["span"][0]:c["span"][1]]) for c in result.figure_captions or ())
    result.edits = [list(e) for e in diff(old, new).edits]
    if write:
        _write_atomically(path, new)
        result.status = "rewritten"
    else:
        result.status = "would_rewrite"
    return result


def build(source: str, article_file: Path, text_path: Path,
          pmid: Optional[str] = None) -> Tuple[str, List[dict]]:
    """The text the source's extractor writes for this download, and its caption spans."""
    if source in JATS_SOURCES:
        from lxml import etree

        from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions

        return article_text_and_captions(etree.parse(str(article_file)), article_file.parent)
    if source == "elsevier":
        from ingestion_workflow.extractors.elsevier_extractor import article_text_and_captions

        return article_text_and_captions(article_file.read_bytes())
    if source == "ace":
        from ingestion_workflow.extractors import ace_extractor
        from ingestion_workflow.patches.ace_patch import set_skip_remote_tables

        set_skip_remote_tables(True)  # the tables fetched at extraction are in its cache
        _, text, captions = ace_extractor.article_text_and_captions(
            article_file.read_text(encoding="utf-8"), pmid, text_path.parent / "downloaded_tables")
        return text, captions
    raise ValueError(f"cannot rebuild the text of {source}")


def _rebuild_write(job: Job) -> Result:
    return rebuild(job, True)


def _rebuild_dry(job: Job) -> Result:
    return rebuild(job, False)


def jobs(catalog, sources: Sequence[str]) -> Iterator[Job]:
    """Every OK extraction of `sources`, with its text path and its download's article.xml."""
    from ingestion_workflow.catalog import Status

    ids = list(catalog.all_article_ids())
    for start in range(0, len(ids), 900):
        chunk = ids[start : start + 900]
        extractions = catalog.artifacts(chunk, "extract")
        downloads = catalog.artifacts(chunk, "download")
        for article_id in chunk:
            for source, extraction in extractions.get(article_id, {}).items():
                if source not in sources or extraction.status is not Status.OK:
                    continue
                payload = catalog.payload(extraction) or {}
                download = catalog.payload(downloads.get(article_id, {}).get(source)) or {}
                identifier = payload.get("identifier") or download.get("identifier") or {}
                yield Job(article_id, source, payload.get("full_text_path"),
                          _article_file(source, download.get("files", [])), identifier.get("pmid"))


def _article_file(source: str, files: Sequence[dict]) -> Optional[str]:
    """The download file the source's extractor builds the text from, as it picks it."""
    for f in files:
        path, kind = str(f.get("file_path", "")), str(f.get("file_type", "")).lower()
        if source == "ace" and kind == "html":
            return path
        if source == "elsevier" and kind == "xml" and Path(path).name.startswith("content."):
            return path
        if source in JATS_SOURCES and path.endswith("/article.xml"):
            return path
    return None


def run(all_jobs: Iterable[Job], *, write: bool, workers: int = 1) -> Iterator[Result]:
    worker = _rebuild_write if write else _rebuild_dry
    batch: List[Job] = []

    def flush(batch):
        if workers <= 1:
            yield from (worker(job) for job in batch)
            return
        # fork: nothing here touches CUDA; spawn would re-import the package per worker
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("fork")) as pool:
            yield from pool.map(worker, batch, chunksize=32)

    for job in all_jobs:
        batch.append(job)
        if len(batch) >= 20000:
            yield from flush(batch)
            batch = []
    if batch:
        yield from flush(batch)


#: Fingerprint a stale sync or references artifact is recorded under: never one a stage computes.
STALE = "stale: text refreshed"

#: Rewritten texts whose catalog rows are read and recorded together.
BATCH = 1000


def _remap_citations(payload: dict, offset_map) -> dict:
    """The references payload with its citations' spans on the new text. A citation whose
    marker an edit changed is dropped, and a sentence an edit changed is cleared (the stage,
    marked stale, reads them all again)."""
    def move(span):
        if not span or offset_map.touches(span["start_char"], span["end_char"]):
            return None
        got = offset_map.span(span["start_char"], span["end_char"])
        return {**span, "start_char": got[0], "end_char": got[1]} if got else None

    citations = []
    for c in payload.get("citations") or []:
        text_span = move(c.get("text_span"))
        if text_span is None:
            continue
        citations.append({**c, "text_span": text_span, "sentence": move(c.get("sentence"))})
    return {**payload, "citations": citations}


def _remap_passages(payload: dict, offset_map) -> Optional[dict]:
    """The passages payload with every span moved onto the new text, or None if one is lost
    or a passage or hit holds characters an edit changed (its x, y, z may no longer be printed)."""
    def move(span, read=False):
        if not span:
            return span
        got = None if read and offset_map.touches(*span) else offset_map.span(*span)
        if got is None:
            raise LookupError
        return list(got)

    try:
        passages = [{**p, "span": move(p["span"], True), "before": move(p.get("before")),
                     "after": move(p.get("after")), "heading": move(p.get("heading")),
                     "hits": [{**h, "span": move(h["span"], True)} for h in p.get("hits", [])]}
                    for p in payload.get("passages", [])]
    except LookupError:
        return None
    return {**payload, "passages": passages}


def _found(catalog, results: Sequence[Result]) -> Dict[Tuple[str, str], Dict[str, object]]:
    """{(article, source): {stage: artifact}}: the rows `carried` reads, in one query per stage."""
    ids = sorted({r.article_id for r in results})
    by_stage = {stage: catalog.artifacts(ids, stage) for stage in ("extract", "references", "passages", "sync")}
    out = {}
    for r in results:
        got = {stage: by_stage[stage].get(r.article_id, {}).get(r.source) for stage in ("extract", "references")}
        got.update({stage: by_stage[stage].get(r.article_id, {}).get("") for stage in ("passages", "sync")})
        out[(r.article_id, r.source)] = {k: v for k, v in got.items() if v is not None}
    return out


def carried(catalog, result: Result, found: Optional[Dict[str, object]] = None) -> Tuple[list, str]:
    """The catalog rows a rewritten (or unchanged) text changes, and what became of its passages:
    `remapped`, `stale` (a span the map could not carry), or `none` (no passages of it).

    `found` is the result's entry of `_found`, when the caller read a batch at once."""
    from ingestion_workflow.catalog import Outcome, Status
    from ingestion_workflow.pipeline.stages.passages import PassagesStage
    from ingestion_workflow.services.offsets import OffsetMap

    if found is None:
        found = _found(catalog, [result])[(result.article_id, result.source)]
    extraction = found.get("extract")
    if extraction is None:
        return [], "none"
    if result.status == "unchanged":
        # The text stays; its hash is recorded where passages looks for it, once.
        if extraction.summary.get("text_sha256") == result.old_sha256 or not result.old_sha256:
            return [], "none"
        return [Outcome(article_id=result.article_id, stage="extract", source=result.source,
                        status=extraction.status, fingerprint=extraction.fingerprint,
                        payload=catalog.payload(extraction),
                        summary={**extraction.summary, "text_sha256": result.old_sha256})], "none"
    offset_map = OffsetMap(tuple(e) for e in result.edits or ())
    extract_payload = dict(catalog.payload(extraction) or {})
    if result.figure_captions is not None:
        extract_payload["figure_captions"] = result.figure_captions
    rows = [Outcome(article_id=result.article_id, stage="extract", source=result.source, status=extraction.status,
                    fingerprint=extraction.fingerprint, payload=extract_payload,
                    summary={**extraction.summary, "text_sha256": result.new_sha256})]
    for stage in ("references", "sync"):
        stale = found.get(stage)
        if stale is not None and stale.status is Status.OK:
            payload = catalog.payload(stale)
            if stage == "references" and payload:
                payload = _remap_citations(payload, offset_map)
            rows.append(Outcome(article_id=result.article_id, stage=stage, source=stale.source, status=stale.status,
                                fingerprint=STALE, payload=payload, summary=stale.summary))
    passages = found.get("passages")
    payload = catalog.payload(passages) if passages is not None and passages.status is Status.OK else None
    if (not payload or not payload.get("passages") or payload.get("text_sha256") != result.old_sha256
            or payload.get("full_text_path") != result.text_path):
        return rows, "none"
    moved = _remap_passages(payload, offset_map)
    if moved is None:
        return rows, "stale"
    moved["text_sha256"] = result.new_sha256
    # spans moved either way; a caption's coordinates are found only by reading the text again
    read_again = result.captions_hold_coordinates
    rows.append(Outcome(article_id=result.article_id, stage="passages", source="", status=Status.OK,
                        fingerprint=STALE if read_again else PassagesStage.fingerprint_for(
                            extraction, result.new_sha256),
                        payload=moved, summary=passages.summary))
    return rows, "captions" if read_again else "remapped"


def carry_all(catalog, results: Iterable[Result], *, write: bool, batch: int = BATCH) -> Counter:
    """`carried` for every result, read and (when `write`) recorded `batch` results at a time:
    how many passages were remapped, left stale, or had none."""
    counts: Counter = Counter()
    results = list(results)
    for start in range(0, len(results), batch):
        chunk = results[start : start + batch]
        found = _found(catalog, chunk)
        rows = []
        for result in chunk:
            got, what = carried(catalog, result, found[(result.article_id, result.source)])
            rows += got
            counts[what] += 1
        if write:
            catalog.record(rows)
    return counts


def refresh_corpus(ns_pond_root: Path, replaced: Dict[str, str], sources: Sequence[str], *,
                   write: bool) -> Iterator[Tuple[Path, str]]:
    """Replace each corpus `text.txt` that is a copy of a rewritten extraction text.

    `replaced` maps the old text's sha256 to the path of the rebuilt one. A corpus file
    whose bytes are not one of those old texts is not touched, whatever its source.
    """
    for record in sorted(p for p in Path(ns_pond_root).iterdir() if p.is_dir()):
        for source in sources:
            target = record / "processed" / source / "text.txt"
            if not target.is_file():
                continue
            old_sha = sha256(_read(target))
            if old_sha not in replaced:
                continue
            if write:
                _write_atomically(target, _read(Path(replaced[old_sha])))
            yield target, old_sha
