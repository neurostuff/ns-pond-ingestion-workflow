"""Rebuild extractions' text from their downloads, leaving everything else about them alone.

A change to a text builder that touches only the text (pubget keeping superscripts)
would, as a version bump, re-run every stage the extraction feeds -- triage, the
analyses model, upload -- though none of them reads the text. This rewrites the text
file in place instead: the extraction keeps its tables, its payload and its fingerprint.
The ns-pond corpus's copy (`processed/<source>/text.txt`) is replaced only where it is
byte-identical to the text it was copied from.
"""

from __future__ import annotations

import hashlib
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

#: The sources whose text a rebuild reproduces: JATS, through pubget's stylesheet.
REFRESHABLE_SOURCES = ("pubget", "pmc", "europepmc")


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class Job:
    article_id: str
    source: str
    text_path: Optional[str]
    article_xml: Optional[str]


@dataclass
class Result:
    article_id: str
    source: str
    status: str  # unchanged | rewritten | would_rewrite | no_text | no_download | failed: ...
    text_path: Optional[str] = None
    old_sha256: Optional[str] = None
    new_sha256: Optional[str] = None


def _write_atomically(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".refresh-tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.chmod(tmp, path.stat().st_mode & 0o7777)
    except OSError:
        pass
    os.replace(tmp, path)


def rebuild(job: Job, write: bool) -> Result:
    """One extraction: its text rebuilt from its download, written when it changed and `write`."""
    from lxml import etree

    from ingestion_workflow.extractors.pubget_extractor import article_text

    result = Result(job.article_id, job.source, "unchanged", job.text_path)
    if not job.text_path or not Path(job.text_path).is_file():
        result.status = "no_text"
        return result
    if not job.article_xml or not Path(job.article_xml).is_file():
        result.status = "no_download"
        return result
    path = Path(job.text_path)
    old = path.read_text(encoding="utf-8")
    result.old_sha256 = sha256(old)
    try:
        xml = Path(job.article_xml)
        new = article_text(etree.parse(str(xml)), xml.parent)
    except Exception as exc:  # noqa: BLE001 - one unreadable article keeps its old text
        result.status = f"failed: {type(exc).__name__}: {exc}"[:300]
        return result
    result.new_sha256 = sha256(new)
    if new == old:
        return result
    if not new.strip():
        result.status = "failed: rebuilt text is empty"
        return result
    if write:
        _write_atomically(path, new)
        result.status = "rewritten"
    else:
        result.status = "would_rewrite"
    return result


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
                xml = next((f["file_path"] for f in download.get("files", [])
                            if str(f.get("file_path", "")).endswith("/article.xml")), None)
                yield Job(article_id, source, payload.get("full_text_path"), xml)


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
            old_sha = sha256(target.read_text(encoding="utf-8"))
            if old_sha not in replaced:
                continue
            if write:
                _write_atomically(target, Path(replaced[old_sha]).read_text(encoding="utf-8"))
            yield target, old_sha
