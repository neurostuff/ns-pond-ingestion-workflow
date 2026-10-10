"""Materialise uploaded articles into the ns-pond tree pondie reads."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    DownloadResult,
    ExtractedContent,
)
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.services import nspond, paper_parse

from .. import exclusions as excl
from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: 2: stage1 points carry `sign` and `is_subpeak`.
#: 3: parse/parsed_paper.json and parse/coordinate_parse.json beside stage1.
SYNC_VERSION = 3


class SyncStage:
    name = "sync"
    requires = "upload"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._synced: List[Tuple[str, ArticleExtractionBundle]] = []
        self._retracted: List[str] = []

    def fingerprint_for(self, upstream: Artifact) -> str:
        return fingerprint(
            "sync", SYNC_VERSION, paper_parse.PAPER_PARSE_VERSION, upstream=upstream.fingerprint
        )

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        for ref in refs:
            upload = upstream.get(ref.id, {}).get("")
            if upload is None or upload.status is not Status.OK:
                plan.blocked += 1
                continue
            if not upload.summary.get("base_study_id"):
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(upload)
            if ctx.is_fresh(artifacts.get(ref.id, {}).get(""), fp):
                plan.fresh += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=upload))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        ids = [work.article_id for work in works]
        extractions = ctx.catalog.artifacts(ids, "extract")
        metadata = ctx.catalog.artifacts(ids, "metadata")
        # The analyses as uploaded: `space` is `analyses` with unknown spaces read in.
        analyses = ctx.catalog.artifacts(ids, "space")
        downloads = ctx.catalog.artifacts(ids, "download")
        triaged = ctx.catalog.artifacts(ids, "triage")
        # What the prose read, for an article extract could not read.
        passages = ctx.catalog.artifacts(ids, "passages")
        # For the coordinate parse: table readings, prose passages, restatements.
        read = ctx.catalog.artifacts(ids, "analyses")
        prose = ctx.catalog.artifacts(ids, "prose")
        resolved = ctx.catalog.artifacts(ids, "resolve")

        excluded = ctx.catalog.exclusions(ids)
        for work in works:
            base_study_id = work.upstream.summary.get("base_study_id")
            if work.upstream.summary.get("retracted"):
                yield self._retract(work, base_study_id)
                continue
            try:
                bundle, per_table, files = self._assemble(
                    ctx, work, extractions, metadata, analyses, downloads, triaged,
                    excluded.get(work.article_id, {}),
                )
            except LookupError as exc:
                yield Outcome.failure(
                    work.article_id, self.name, "", str(exc), fingerprint=work.fingerprint
                )
                continue
            try:
                target = nspond.write_article(
                    self.settings.ns_pond_root,
                    base_study_id,
                    bundle,
                    per_table,
                    files,
                    overwrite=self.settings.sync_overwrite,
                )
            except Exception as exc:
                logger.warning("sync failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            inputs = _parse_inputs(
                ctx, work, base_study_id, excluded.get(work.article_id, {}),
                {stage: found.get(work.article_id, {}) for stage, found in (
                    ("extract", extractions), ("metadata", metadata), ("space", analyses),
                    ("triage", triaged), ("analyses", read), ("prose", prose),
                    ("resolve", resolved), ("passages", passages))},
                bundle.article_data.source.value,
            )
            self._synced.append((base_study_id, bundle))
            try:
                parse = paper_parse.write(target, bundle, per_table, inputs,
                                          overwrite=self.settings.sync_overwrite)
            except Exception as exc:  # noqa: BLE001 - stage1 is written; the parse is retried
                logger.warning("parse files failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id, self.name, "", f"parse: {type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                summary={"base_study_id": base_study_id, "path": str(target), **parse},
            )

    def _retract(self, work: Work, base_study_id: str) -> Outcome:
        """Take a retracted article out of the corpus.

        Moved, not deleted: the corpus is not reproducible from the catalog
        alone, so the directory goes to a sibling `<root>-retracted/` tree,
        stamped with the time, where it can be put back by hand.
        """
        root = Path(self.settings.ns_pond_root)
        source = root / base_study_id
        moved = None
        if source.exists():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            moved = root.parent / f"{root.name}-retracted" / f"{base_study_id}-{stamp}"
            moved.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(moved))
        self._retracted.append(base_study_id)
        return Outcome(
            article_id=work.article_id,
            stage=self.name,
            source="",
            status=Status.OK,
            fingerprint=work.fingerprint,
            summary={"base_study_id": base_study_id, "retracted": True,
                     "moved_to": str(moved) if moved else None},
        )

    def _assemble(self, ctx, work, extractions, metadata, analyses, downloads, triaged, excluded=None):
        extraction = _synced_extraction(
            ctx,
            extractions.get(work.article_id, {}),
            downloads.get(work.article_id, {}),
            triaged.get(work.article_id, {}).get(""),
        )
        if extraction is None:
            raise LookupError("no successful extraction to sync")
        payload = ctx.payload(extraction)
        if payload is None:
            raise LookupError("extraction payload missing from blob store")
        content = ExtractedContent.from_dict(payload)
        source = extraction.source
        content.identifier = work.ref.identifier
        content.slug = work.ref.identifier.slug

        meta_payload = ctx.payload(metadata.get(work.article_id, {}).get(""))
        article_metadata = (
            ArticleMetadata.from_dict(meta_payload)
            if meta_payload
            else ArticleMetadata(title=content.slug)
        )

        analysis_payload = excl.kept(
            ctx.payload(analyses.get(work.article_id, {}).get("")), excluded or {}
        )
        per_table = {
            table_id: AnalysisCollection.from_dict(blob)
            for table_id, blob in analysis_payload.items()
        }

        files: List[DownloadResult] = []
        download = downloads.get(work.article_id, {}).get(source)
        download_payload = ctx.payload(download)
        if download_payload:
            files.append(DownloadResult.from_dict(download_payload))

        return ArticleExtractionBundle(content, article_metadata), per_table, files

    def finish(self) -> None:
        """Write the corpus-level manifest once the run is over."""
        if not self._synced and not self._retracted:
            return
        nspond.write_corpus_manifest(
            self.settings.ns_pond_root / "pmids.tsv", self._synced, drop=self._retracted
        )
        self._synced.clear()
        self._retracted.clear()


def _parse_inputs(
    ctx: Context, work: Work, base_study_id, excluded, found, source
) -> paper_parse.ParseInputs:
    """What the parse files need from the catalog beyond the bundle."""
    def ok(stage, key=""):
        artifact = found[stage].get(key)
        return artifact if artifact is not None and artifact.status is Status.OK else None

    def payload(stage, key=""):
        artifact = ok(stage, key)
        return ctx.payload(artifact) if artifact is not None else None

    analyses = ok("analyses")
    summary = analyses.summary if analyses is not None else {}
    passages = payload("passages")
    resolved = ok("resolve")
    picked = {"extract": ok("extract", source), "metadata": ok("metadata"), "space": ok("space"),
              "triage": ok("triage")}
    return paper_parse.ParseInputs(
        article_id=work.article_id,
        base_study_id=base_study_id,
        triage=payload("triage"),
        excluded=excluded or {},
        readings=summary.get("readings"),
        unread=summary.get("unread"),
        prose=payload("prose"),
        passages_kept=len(passages.get("passages") or []) if passages else None,
        restated=(resolved.summary or {}).get("restated") if resolved is not None else None,
        fingerprints={k: a.fingerprint for k, a in picked.items() if a and a.fingerprint},
    )


def _synced_extraction(
    ctx: Context,
    extractions: Dict[str, Artifact],
    downloads: Dict[str, Artifact],
    triage: Artifact | None,
) -> Artifact | None:
    """The extraction triage judged, which is the one the analyses came from.

    Table ids are unique only within an extraction, so writing another one
    beside these analyses would pair them with different tables. An article
    triage has not reached takes the one triage would judge, whose text its
    passages index.
    """
    from .triage import judged_extraction

    judged = extractions.get((triage.summary or {}).get("source", "")) if triage else None
    if judged is not None and judged.status is Status.OK:
        return judged
    return judged_extraction(ctx, extractions, downloads)
