"""Write studies, analyses and coordinates to the Neurostore database."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.metadata import ArticleMetadata

from .. import exclusions as excl
from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

UPLOAD_VERSION = 1


class UploadStage:
    name = "upload"
    #: `space` is the analyses with their unknown spaces filled in.
    requires = "space"

    #: `space` carries `analyses`' count of tables that produced a collection.
    #: An article it found nothing in is a legitimate `ok` -- the model read
    #: the tables and they held no coordinates -- so the status check does not
    #: exclude it, and 30,741 such articles would be planned to upload nothing.
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, upstream: Artifact, excluded=None) -> str:
        # A person's exclusions are an input like any other: marking a table
        # makes the article stale, so the next upload takes it back out of
        # neurostore. Appended only when there is one, so the fingerprint of
        # every article nobody has marked is exactly what it was.
        marked = excl.digest(excluded or {})
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
            *([marked] if marked else []),
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
        excluded = ctx.catalog.exclusions([ref.id for ref in refs])
        for ref in refs:
            spaced = upstream.get(ref.id, {}).get("")
            if spaced is None or spaced.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(spaced, excluded.get(ref.id))
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=spaced))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService, resolve_upload_source

        # Before the tunnel and before a single row is written. Checked here
        # rather than in `__init__` so that planning and `--dry-run`, which
        # change nothing, still work on a config that has not named a source.
        resolve_upload_source(self.settings)

        excluded = ctx.catalog.exclusions([work.article_id for work in works])
        analyses, metadata, empty = self._gather(ctx, works, excluded)
        # An article left with nothing because a person marked its tables is
        # not "nothing to upload": what it uploaded before is still there, and
        # still claims coordinates the paper does not report. Those are
        # retracted. One that was never uploaded has nothing to take back.
        previous = ctx.catalog.artifacts([w.article_id for w in empty if w.article_id in excluded], "upload")
        retract, empty = self._retractable(ctx, empty, excluded, previous)
        # PubMed lists a retraction of the article: what it uploaded before is
        # taken back the same way, and one never uploaded is not uploaded now.
        ids_retracted = set()
        doomed = [w for w in works if getattr(metadata.get(w.ref.identifier.slug), "retracted", False)]
        if doomed:
            ids = ids_retracted = {w.article_id for w in doomed}
            more, never = self._retractable(
                ctx, doomed, ids, ctx.catalog.artifacts(list(ids), "upload"))
            retract += more
            empty += never
            for work in doomed:
                metadata.pop(work.ref.identifier.slug, None)
                analyses.pop(work.ref.identifier.slug, None)
        yield from self._retract(retract)
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
                summary=({"reason": "excluded by hand"} if work.article_id in excluded
                         else {"reason": "retracted in PubMed"} if work.article_id in ids_retracted
                         else {"reason": "no analyses to upload"}),
            )
        skipped = {work.article_id for work in empty} | {work.article_id for work, _ in retract}
        works = [work for work in works if work.article_id not in skipped]
        if not analyses:
            return

        by_slug = {work.ref.identifier.slug: work for work in works}
        try:
            with SSHTunnel(self.settings) as tunnel:
                sessions = SessionFactory(self.settings, tunnel=tunnel)
                service = UploadService(self.settings, sessions)
                items = service.prepare_work_items(
                    analyses, metadata, metadata_mode=self.settings.upload_metadata_mode,
                    identifiers={slug: work.ref.identifier for slug, work in by_slug.items()},
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

    def _retractable(self, ctx, empty, excluded, previous):
        """Split the empty works into those to retract and those to skip."""
        retract, skip = [], []
        for work in empty:
            prior = previous.get(work.article_id, {}).get("")
            base_study_id = (prior.summary or {}).get("base_study_id") if prior and prior.status is Status.OK else None
            base_study_id = base_study_id or work.ref.identifier.neurostore
            if work.article_id in excluded and base_study_id:
                retract.append((work, base_study_id))
            else:
                skip.append(work)
        return retract, skip

    def _retract(self, retract) -> Iterator[Outcome]:
        if not retract:
            return
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService

        by_slug = {work.ref.identifier.slug: work for work, _ in retract}
        try:
            with SSHTunnel(self.settings) as tunnel:
                service = UploadService(self.settings, SessionFactory(self.settings, tunnel=tunnel))
                outcomes = service.retract([(work.ref.identifier.slug, bsid) for work, bsid in retract])
        except Exception as exc:
            logger.error("retraction batch failed: %s", exc)
            for work, _ in retract:
                yield Outcome.failure(work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                                      fingerprint=work.fingerprint)
            return
        for outcome in outcomes:
            work = by_slug.get(outcome.slug)
            if work is None:
                continue
            if not outcome.success:
                yield Outcome.failure(work.article_id, self.name, "", outcome.error or "retraction failed",
                                      fingerprint=work.fingerprint)
                continue
            # OK, not SKIPPED: sync reads the summary and takes the article out
            # of the corpus as well.
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                summary={
                    "base_study_id": outcome.base_study_id,
                    "study_id": outcome.study_id,
                    "retracted": outcome.action,
                    "removed": outcome.removed,
                    "kept_annotated": outcome.kept_annotated,
                    "studysets": len(outcome.studysets or []),
                    "analyses": 0,
                },
            )

    def _gather(self, ctx: Context, works: Sequence[Work], excluded=None):
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
        excluded = excluded or {}
        for work in works:
            payload = excl.kept(ctx.payload(work.upstream), excluded.get(work.article_id, {}))
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
