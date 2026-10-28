"""Every http(s) link an article gives (`services.paper_links`), in its text.

The text and the source copy are those `references` reads: the extraction triage judges,
which sync writes. The artifact is keyed by that extraction's source, because an
occurrence's offsets belong to one text. No network; where `references` has read the
paper's list, its entries' links and dataset DOIs are read too, and an occurrence from the
list names its entry by the id sync writes in `ParsedPaper.references`. `linkprobe` asks
OSF and OpenNeuro about the links that name a project or dataset there, and sync writes
`ParsedPaper.links` from both.
"""

from __future__ import annotations

import hashlib
from functools import partial
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import DownloadResult

from ..plan import StagePlan, Work
from ..stage import Context, alone

#: Bump when what is read, or how a link is normalized, changes.
#: 2: the reference list is the references stage's, not Crossref's, and an occurrence names
#: its entry as `reference_id`; each link's occurrences in text order.
LINKS_VERSION = 2


def read_article(source: str, download: dict, text_path: Optional[str],
                 listed: Optional[List[dict]] = None) -> Tuple[str, Optional[dict]]:
    """("ok", payload) or (why it failed, None). `listed` is the references stage's list,
    when it has read one."""
    from ingestion_workflow.services.paper_links import from_markup, probe_target, read_links

    if not text_path or not Path(text_path).exists():
        return "extraction kept no text", None
    text = Path(text_path).read_bytes().decode("utf-8")  # as stored: offsets index it
    notes: Dict[str, int] = {}
    markup = from_markup(source, DownloadResult.from_dict(download), text, notes)
    links = read_links(text, markup, listed)
    return "ok", {
        "source": source,
        "markup_read": markup is not None,
        "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "links": links,
        "probe": sorted(link["url"] for link in links if probe_target(link["url"])),
        "notes": notes,
    }


def judged_links(ctx: Context, refs: Sequence[ArticleRef],
                 links: Dict[str, Dict[str, Artifact]]) -> Dict[str, Artifact]:
    """{article_id: the OK links artifact of the extraction triage judges}."""
    from .triage import judged_extraction

    ids = [ref.id for ref in refs]
    extracted = ctx.catalog.artifacts(ids, "extract")
    downloads = ctx.catalog.artifacts(ids, "download")
    out = {}
    for ref in refs:
        judged = judged_extraction(ctx, extracted.get(ref.id, {}), downloads.get(ref.id, {}))
        found = links.get(ref.id, {}).get(judged.source) if judged is not None else None
        if found is not None and found.status is Status.OK:
            out[ref.id] = found
    return out


class LinksStage:
    name = "links"
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, extraction: Artifact, references: Optional[Artifact] = None) -> str:
        listed = references.fingerprint if references is not None and references.ok else ""
        return fingerprint("links", LINKS_VERSION, extraction.source, listed,
                           upstream=extraction.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        from .triage import judged_extraction

        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        downloads = ctx.catalog.artifacts(ids, "download")
        listed = ctx.catalog.artifacts(ids, "references")
        attempts: Dict[str, Dict[str, tuple]] = {}
        for ref in refs:
            judged = judged_extraction(ctx, upstream.get(ref.id, {}), downloads.get(ref.id, {}))
            if judged is None:
                plan.blocked += 1
                continue
            source = judged.source
            fp = self.fingerprint_for(judged, listed.get(ref.id, {}).get(source))
            existing = artifacts.get(ref.id, {}).get(source)
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            if source not in attempts:
                attempts[source] = ctx.catalog.attempt_counts(ids, self.name, source)
            count, last = attempts[source].get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name, source):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source=source, fingerprint=fp, upstream=judged))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from .references import read_articles

        jobs, ready = [], []
        lists = ctx.catalog.artifacts([w.article_id for w in works], "references")
        for work in works:
            download = ctx.catalog.artifact(work.article_id, "download", work.source)
            download_payload = ctx.payload(download) if download else None
            extraction = ctx.payload(work.upstream)
            if not download_payload or not extraction:
                yield Outcome.failure(work.article_id, self.name, work.source,
                                      "download or extraction payload missing from blob store",
                                      fingerprint=work.fingerprint)
                continue
            references = lists.get(work.article_id, {}).get(work.source)
            listed = ((ctx.payload(references) or {}).get("references")
                      if references is not None and references.ok else None)
            text_path = ctx.recorded_path(extraction.get("full_text_path"))
            jobs.append((work.source, download_payload, str(text_path) if text_path else None,
                         listed or None))
            ready.append(work)
        results = read_articles(read_article, jobs, getattr(self.settings, "max_workers", 1) or 1)
        for work, (how, payload) in zip(ready, results):
            yield alone(self.name, work, partial(self._outcome, work, how, payload))

    def _outcome(self, work: Work, how: str, payload: Optional[dict]) -> Outcome:
        if payload is None:
            return Outcome.failure(work.article_id, self.name, work.source, how,
                                   fingerprint=work.fingerprint)
        links = payload["links"]
        found_by: Dict[str, int] = {}
        for link in links:
            for occurrence in link["occurrences"]:
                found_by[occurrence["found_by"]] = found_by.get(occurrence["found_by"], 0) + 1
        return Outcome(
            article_id=work.article_id, stage=self.name, source=work.source, status=Status.OK,
            fingerprint=work.fingerprint, payload=payload,
            # `probe` is what linkprobe asks about, read from here without the payload
            summary={"links": len(links), "found_by": found_by,
                     "nifti_context": sum(link["nifti_context"] for link in links),
                     "probe": payload["probe"], "markup_read": payload["markup_read"]},
        )
