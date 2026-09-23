"""Materialise uploaded articles into the ns-pond tree pondie reads."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    DownloadResult,
    ExtractedContent,
)
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.services import nspond

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

SYNC_VERSION = 1


class SyncStage:
    name = "sync"
    requires = "upload"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._synced: List[Tuple[str, ArticleExtractionBundle]] = []

    def fingerprint_for(self, upstream: Artifact) -> str:
        return fingerprint("sync", SYNC_VERSION, upstream=upstream.fingerprint)

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
        analyses = ctx.catalog.artifacts(ids, "analyses")
        downloads = ctx.catalog.artifacts(ids, "download")

        for work in works:
            base_study_id = work.upstream.summary.get("base_study_id")
            try:
                bundle, per_table, files = self._assemble(
                    ctx, work, extractions, metadata, analyses, downloads
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
            self._synced.append((base_study_id, bundle))
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                summary={"base_study_id": base_study_id, "path": str(target)},
            )

    def _assemble(self, ctx, work, extractions, metadata, analyses, downloads):
        extraction = _best(extractions.get(work.article_id, {}))
        if extraction is None:
            raise LookupError("no successful extraction to sync")
        payload = ctx.payload(extraction)
        if payload is None:
            raise LookupError("extraction payload missing from blob store")
        content = ExtractedContent.from_dict(payload)
        content.identifier = work.ref.identifier
        content.slug = work.ref.identifier.slug

        meta_payload = ctx.payload(metadata.get(work.article_id, {}).get(""))
        article_metadata = (
            ArticleMetadata.from_dict(meta_payload)
            if meta_payload
            else ArticleMetadata(title=content.slug)
        )

        analysis_payload = ctx.payload(analyses.get(work.article_id, {}).get("")) or {}
        per_table = {
            table_id: AnalysisCollection.from_dict(blob)
            for table_id, blob in analysis_payload.items()
        }

        files: List[DownloadResult] = []
        download = downloads.get(work.article_id, {}).get(extraction.source)
        download_payload = ctx.payload(download)
        if download_payload:
            files.append(DownloadResult.from_dict(download_payload))

        return ArticleExtractionBundle(content, article_metadata), per_table, files

    def finish(self) -> None:
        """Write the corpus-level manifest once the run is over."""
        if not self._synced:
            return
        nspond.write_corpus_manifest(self.settings.ns_pond_root / "pmids.tsv", self._synced)
        self._synced.clear()


def _best(candidates: Dict[str, Artifact]) -> Artifact | None:
    usable = [a for a in candidates.values() if a.status is Status.OK]
    if not usable:
        return None
    return max(usable, key=lambda a: a.summary.get("tables_with_coordinates", 0))
