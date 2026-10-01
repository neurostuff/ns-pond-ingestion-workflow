"""Write studies, analyses and coordinates to the Neurostore database."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.metadata import ArticleMetadata

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

UPLOAD_VERSION = 1


class UploadStage:
    name = "upload"
    requires = "analyses"

    #: `analyses` records how many tables produced a collection. An article it
    #: found nothing in is a legitimate `ok` -- the model read the tables and
    #: they held no coordinates -- so the status check does not exclude it, and
    #: 30,741 such articles would be planned to upload nothing.
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, upstream: Artifact) -> str:
        return fingerprint(
            "upload",
            UPLOAD_VERSION,
            # neurostore keeps one study version per source, so this is not a
            # label on the same row -- a different source writes a different
            # study. Without it here, pointing the run at a new extractor left
            # 32,947 already-uploaded articles looking fresh, and the new
            # source would never have been written for any of them.
            self.settings.upload_source,
            self.settings.upload_behavior.value,
            self.settings.upload_metadata_mode.value,
            self.settings.upload_metadata_only,
            upstream=upstream.fingerprint,
        )

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            analyses = upstream.get(ref.id, {}).get("")
            if analyses is None or analyses.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(analyses)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=analyses))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService

        analyses, metadata, empty = self._gather(ctx, works)
        # An article whose every collection came back with no analyses has
        # nothing to say. Uploading it would create a study claiming the paper
        # reports no coordinates, when what the extractor said is that these
        # are not coordinate tables. Recorded as skipped rather than failed:
        # the stage did its job, and a failure would be retried forever.
        for work in empty:
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.SKIPPED,
                fingerprint=work.fingerprint,
                summary={"reason": "no analyses to upload"},
            )
        skipped = {work.article_id for work in empty}
        works = [work for work in works if work.article_id not in skipped]
        if not analyses:
            return

        by_slug = {work.ref.identifier.slug: work for work in works}
        try:
            with SSHTunnel(self.settings) as tunnel:
                sessions = SessionFactory(self.settings, tunnel=tunnel)
                service = UploadService(self.settings, sessions)
                items = service.prepare_work_items(
                    analyses, metadata, metadata_mode=self.settings.upload_metadata_mode
                )
                outcomes = service.run(
                    items,
                    behavior=self.settings.upload_behavior,
                    metadata_only=self.settings.upload_metadata_only,
                    metadata_mode=self.settings.upload_metadata_mode,
                )
        except Exception as exc:
            logger.error("upload batch failed: %s", exc)
            for work in works:
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
            return

        seen = set()
        learned: List[Tuple[str, str, str]] = []
        for outcome in outcomes:
            work = by_slug.get(outcome.slug)
            if work is None:
                continue
            seen.add(work.article_id)
            if not outcome.success:
                yield Outcome.failure(
                    work.article_id, self.name, "", outcome.error or "upload failed",
                    fingerprint=work.fingerprint,
                )
                continue
            if outcome.base_study_id:
                # Neurostore assigns this during upload, so it is only knowable
                # now. Recording it as an alias makes the mapping queryable and
                # lets `ingest show <base_study_id>` find the article.
                learned.append((work.article_id, "neurostore", outcome.base_study_id))
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                summary={
                    "base_study_id": outcome.base_study_id,
                    "study_id": outcome.study_id,
                    "analyses": len(outcome.analysis_ids),
                },
            )
        ctx.catalog.add_aliases(learned)

        for work in works:
            if work.article_id not in seen:
                yield Outcome.failure(
                    work.article_id, self.name, "", "upload returned no outcome",
                    fingerprint=work.fingerprint,
                )

    def _gather(self, ctx: Context, works: Sequence[Work]):
        """Load only this batch's analyses and metadata, never the whole cache.

        Also returns the works holding nothing to upload. The gate in `narrow`
        gets as far as `summary.tables`, which counts collections rather than
        analyses, so a table the extractor returned an empty answer for still
        reaches here -- 22 of 49,778 articles in the v19 corpus run.
        """
        analyses: Dict[str, Dict[str, AnalysisCollection]] = {}
        metadata: Dict[str, ArticleMetadata] = {}
        empty: List[Work] = []
        meta_artifacts = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        for work in works:
            payload = ctx.payload(work.upstream)
            if not payload:
                empty.append(work)
                continue
            if not any((blob or {}).get("analyses") for blob in payload.values()):
                empty.append(work)
                continue
            slug = work.ref.identifier.slug
            analyses[slug] = {
                table_id: AnalysisCollection.from_dict(blob)
                for table_id, blob in payload.items()
            }
            meta_payload = ctx.payload(meta_artifacts.get(work.article_id, {}).get(""))
            if meta_payload:
                metadata[slug] = ArticleMetadata.from_dict(meta_payload)
        return analyses, metadata, empty
