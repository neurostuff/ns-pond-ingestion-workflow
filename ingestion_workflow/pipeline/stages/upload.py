"""Write studies, analyses and coordinates to the Neurostore database."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.metadata import ArticleMetadata

from .. import exclusions as excl
from ..plan import StagePlan, Work
from ..stage import Context
from .roles import refuse_unassigned, uploaded_sets, with_roles

logger = logging.getLogger(__name__)

UPLOAD_VERSION = 1


def _notice_marker(notices: Optional[Artifact]) -> Optional[str]:
    """What upload does differently for this article's PubMed notices, if anything.

    The catalog keeps the last OK notices answer through a failed refresh, so a
    status that is not OK means PubMed has never answered for this paper. That
    is unknown, not "no notices": upload goes ahead as if there were none (a
    paper whose lookup keeps failing must not hold up the corpus), and the
    answer, once it arrives, changes the fingerprint and uploads it again.
    """
    if notices is None or notices.status is not Status.OK:
        return None
    summary = notices.summary or {}
    if summary.get("retraction_notice"):
        return "retraction-notice"
    if summary.get("retracted"):
        return "retracted:" + ((summary.get("retraction") or {}).get("pmid") or "")
    return None


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

    def fingerprint_for(
        self, upstream: Artifact, excluded=None, notices: Optional[Artifact] = None
    ) -> str:
        # A person's exclusions are an input like any other: marking a table
        # makes the article stale, so the next upload takes it back out of
        # neurostore. Appended only when there is one, so the fingerprint of
        # every article nobody has marked is exactly what it was.
        marked = excl.digest(excluded or {})
        # A retraction PubMed lists, or the paper being a retraction notice, is
        # an input too, so a paper retracted after its upload is uploaded again
        # and marked. Appended the same way as exclusions: no other fingerprint
        # moves.
        notice = _notice_marker(notices)
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
            *([notice] if notice else []),
            # The sets and their decided roles, not the role models: a retrained
            # model that decides every set as before uploads nothing again.
            upstream=(upstream.summary or {}).get("upload_basis") or upstream.fingerprint,
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
        notices = ctx.catalog.artifacts([ref.id for ref in refs], "notices")
        roled = with_roles(ctx, [ref.id for ref in refs])
        for ref in refs:
            spaced = upstream.get(ref.id, {}).get("")
            # Without OK roles nothing is uploaded, whatever an older space artifact holds.
            if spaced is None or spaced.status is not Status.OK or ref.id not in roled:
                plan.blocked += 1
                continue
            notice = notices.get(ref.id, {}).get("")
            fp = self.fingerprint_for(spaced, excluded.get(ref.id), notice)
            existing = artifacts.get(ref.id, {}).get("")
            # Uploaded while neurostore had no is_retracted column: tried again
            # each run until the flag is set.
            unmarked = existing is not None and (
                existing.summary.get("retraction_marked") is False
                or existing.summary.get("retraction_cleared") is False
            )
            if ctx.is_fresh(existing, fp) and not unmarked:
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=spaced))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.upload import resolve_upload_source

        resolve_upload_source(self.settings)
        # A retraction that has since gone from PubMed: the study was marked on
        # an earlier upload, and neurostore's own ingester clears the flag when
        # the notice is gone, so this does too.
        withdrawn = self._withdrawn(ctx, works)
        cleared = self._mark_retracted({}, list(withdrawn.values())) if withdrawn else True
        for outcome in self._execute(ctx, works):
            if not cleared and outcome.article_id in withdrawn and outcome.status in (
                Status.OK, Status.SKIPPED
            ):
                # Still marked in neurostore: planned again until it is cleared.
                outcome.summary = {**outcome.summary, "retraction_marked": True,
                                   "retraction_cleared": False}
            yield outcome

    def _withdrawn(self, ctx: Context, works: Sequence[Work]) -> Dict[str, str]:
        """{article_id: base study id} for papers marked retracted that PubMed no longer lists so."""
        ids = [w.article_id for w in works]
        found = ctx.catalog.artifacts(ids, "notices")
        before = ctx.catalog.artifacts(ids, "upload")
        out = {}
        for work in works:
            notices = found.get(work.article_id, {}).get("")
            prior = before.get(work.article_id, {}).get("")
            if notices is None or notices.status is not Status.OK or _notice_marker(notices):
                continue
            if prior is None or prior.summary.get("retraction_marked") is None:
                continue
            bsid = prior.summary.get("base_study_id") or work.ref.identifier.neurostore
            if bsid:
                out[work.article_id] = bsid
        return out

    def _execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService

        refused = {}
        for work in works:
            outcome = refuse_unassigned(self.name, work, ctx.payload(work.upstream))
            if outcome is not None:
                refused[work.article_id] = outcome
        yield from refused.values()
        works = [work for work in works if work.article_id not in refused]

        excluded = ctx.catalog.exclusions([work.article_id for work in works])
        found = ctx.catalog.artifacts([w.article_id for w in works], "notices")
        markers = {w.article_id: _notice_marker(found.get(w.article_id, {}).get(""))
                   for w in works}
        # A paper that is itself a retraction notice is not a study, whatever
        # the extractor found in it (a notice can reprint the table it retracts).
        for work in works:
            if markers[work.article_id] == "retraction-notice":
                yield Outcome(article_id=work.article_id, stage=self.name, source="",
                              status=Status.SKIPPED, fingerprint=work.fingerprint,
                              summary={"reason": "retraction notice"})
        works = [w for w in works if markers[w.article_id] != "retraction-notice"]
        # Retracted papers are uploaded like any other and then marked: the
        # decision is "marked, excluded by default", so the study is kept.
        retractions = {
            w.article_id: found[w.article_id][""].summary.get("retraction")
            for w in works if (markers[w.article_id] or "").startswith("retracted")
        }
        analyses, metadata, empty = self._gather(ctx, works, excluded)
        # An article left with nothing because a person marked its tables is
        # not "nothing to upload": what it uploaded before is still there, and
        # still claims coordinates the paper does not report. Those are
        # retracted. One that was never uploaded has nothing to take back.
        previous = ctx.catalog.artifacts([w.article_id for w in empty], "upload")
        retract, empty = self._retractable(ctx, empty, excluded, previous)
        # A retracted paper with nothing to upload is still retracted: its
        # earlier study is marked, or releases would keep it.
        flagged = {}
        for work in [w for w, _ in retract] + empty:
            if work.article_id in retractions:
                bsid = self._base_study_id(work, previous)
                if bsid:
                    flagged[work.article_id] = bsid
        flag_marked = self._mark_retracted({b: retractions[a] for a, b in flagged.items()})
        yield from self._retract(retract, {a: flag_marked for a in flagged})
        # An article whose every collection came back with no analyses has
        # nothing to say. Uploading it would create a study claiming the paper
        # reports no coordinates, when what the extractor said is that these
        # are not coordinate tables. Recorded as skipped rather than failed:
        # the stage did its job, and a failure would be retried forever.
        for work in empty:
            summary = ({"reason": "excluded by hand"} if work.article_id in excluded
                       else {"reason": "no analyses to upload"})
            if work.article_id in flagged:
                summary |= {"base_study_id": flagged[work.article_id],
                            "retraction_marked": flag_marked}
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.SKIPPED,
                fingerprint=work.fingerprint,
                summary=summary,
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

        marked = self._mark_retracted({
            o.base_study_id: retractions[by_slug[o.slug].article_id]
            for o in outcomes
            if o.success and o.base_study_id and o.slug in by_slug
            and by_slug[o.slug].article_id in retractions
        })

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
                } | ({"retraction_marked": marked} if work.article_id in retractions else {}),
            )
        ctx.catalog.add_aliases(learned)

        for work in works:
            if work.article_id not in seen:
                yield Outcome.failure(
                    work.article_id, self.name, "", "upload returned no outcome",
                    fingerprint=work.fingerprint,
                )

    def _mark_retracted(self, targets: Dict[str, Optional[dict]], clear: Sequence[str] = ()) -> bool:
        """Set neurostore's retraction flag on these base studies and clear it on `clear`.

        False if it could not.
        """
        if not targets and not clear:
            return True
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService

        try:
            with SSHTunnel(self.settings) as tunnel:
                sessions = SessionFactory(self.settings, tunnel=tunnel)
                return UploadService(self.settings, sessions).mark_retracted(targets, clear) is not None
        except Exception as exc:  # noqa: BLE001 - recorded as unmarked, so tried again
            logger.error("marking %d retraction(s) failed: %s", len(targets) + len(clear), exc)
            return False

    @staticmethod
    def _base_study_id(work, previous) -> Optional[str]:
        """The neurostore study an earlier upload made for this work, if any."""
        prior = previous.get(work.article_id, {}).get("")
        found = (prior.summary or {}).get("base_study_id") if prior and prior.status in (
            Status.OK, Status.SKIPPED) else None
        return found or work.ref.identifier.neurostore

    def _retractable(self, ctx, empty, excluded, previous):
        """Split the empty works into those to retract and those to skip."""
        retract, skip = [], []
        for work in empty:
            base_study_id = self._base_study_id(work, previous)
            if work.article_id in excluded and base_study_id:
                retract.append((work, base_study_id))
            else:
                skip.append(work)
        return retract, skip

    def _retract(self, retract, marked=None) -> Iterator[Outcome]:
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
                } | ({"retraction_marked": marked[work.article_id]} if work.article_id in (marked or {}) else {}),
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
            # Held sets stay in the payload, in place, for the exclusions' and
            # pondie's positions; neurostore gets only the uploaded ones.
            payload = uploaded_sets(
                excl.kept(ctx.payload(work.upstream), excluded.get(work.article_id, {})) or {}
            )
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
