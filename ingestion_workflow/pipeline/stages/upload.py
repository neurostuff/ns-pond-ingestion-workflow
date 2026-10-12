"""Send each article's studies, analyses and coordinates to neurostore.

`IngesterUploadStage` is the upload the pipeline runs: it posts the coordinate parse
sync wrote to neurostore's study_schema ingester. `UploadStage` is the deprecated
direct-database upload (`upload_backend: legacy`), which runs before sync.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.metadata import ArticleMetadata

from .. import exclusions as excl
from ..plan import StagePlan, Work
from ..stage import (
    HELD_FOR_REVIEW,
    NO_TEXT,
    Context,
    is_take_back,
    take_back_or_block,
    taken_back,
)
from .roles import refuse_unassigned, uploaded_sets, with_roles

logger = logging.getLogger(__name__)

#: 2: the inverse half of a sign split is named by its reversed contrast, its
#: negative values are negated, and `split` declares it.
UPLOAD_VERSION = 2


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
    """Deprecated: write neurostore's database directly over an ssh tunnel.

    Runs only with `upload_backend: legacy`, before `SyncStage`. It has no dry run.
    """

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
            if spaced is None or spaced.status is not Status.OK:
                take_back_or_block(plan, ref, spaced, artifacts.get(ref.id, {}).get(""))
                continue
            # Without OK roles nothing is uploaded, whatever an older space artifact
            # holds; missing roles only wait, they never take back what was uploaded.
            if ref.id not in roled:
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

        if getattr(self.settings, "dry_run", False):
            # It would write the database: a dry run's upload is the ingester's.
            raise ValueError("the direct-database upload (upload_backend: legacy) has no dry "
                             "run; use upload_backend: ingester")
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

        # An uploaded article whose text was taken back: its pipeline analyses came from a
        # text no extraction holds now, so they are retracted the way an article whose
        # every table was excluded is, except that a study any studyset holds is left
        # as it is and recorded as held for review. Annotated analyses stay.
        back = [work for work in works if is_take_back(work)]
        works = [work for work in works if not is_take_back(work)]
        uploaded = ctx.catalog.artifacts([w.article_id for w in back], "upload")
        retract_back, nothing = [], []
        for work in back:
            prior = uploaded.get(work.article_id, {}).get("")
            # No upload of its own: whatever neurostore holds for the article, this
            # pipeline did not put there, so the take-back is only recorded.
            base_study_id = prior is not None and (
                (prior.summary or {}).get("base_study_id") or work.ref.identifier.neurostore)
            if base_study_id:
                retract_back.append((work, base_study_id))
            else:
                nothing.append(work)
        for work in nothing:
            yield Outcome.failure(work.article_id, self.name, "", NO_TEXT, fingerprint=NO_TEXT)
        yield from self._retract(retract_back, hold_studyset_members=True)
        if not works:
            return

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

    def _retract(self, retract, marked=None, *, hold_studyset_members: bool = False) -> Iterator[Outcome]:
        if not retract:
            return
        from ingestion_workflow.services.db import SessionFactory, SSHTunnel
        from ingestion_workflow.services.upload import UploadService

        by_slug = {work.ref.identifier.slug: work for work, _ in retract}
        try:
            with SSHTunnel(self.settings) as tunnel:
                service = UploadService(self.settings, SessionFactory(self.settings, tunnel=tunnel))
                targets = [(work.ref.identifier.slug, bsid) for work, bsid in retract]
                outcomes = service.retract(targets, hold_studyset_members=hold_studyset_members)
        except Exception as exc:
            logger.error("retraction batch failed: %s", exc)
            # The failure keeps base_study_id, so the next run can still retract.
            for work, bsid in retract:
                yield Outcome.failure(work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                                      fingerprint=work.fingerprint,
                                      summary={"base_study_id": bsid})
            return
        held = []
        for outcome in outcomes:
            work = by_slug.get(outcome.slug)
            if work is None:
                continue
            if not outcome.success:
                yield Outcome.failure(work.article_id, self.name, "", outcome.error or "retraction failed",
                                      fingerprint=work.fingerprint,
                                      summary={"base_study_id": outcome.base_study_id})
                continue
            if outcome.action == "held":
                # Not OK, so sync leaves the article in the corpus; planned again each
                # run, so it is retracted once no studyset holds it.
                held.append(outcome.base_study_id)
                yield Outcome.failure(
                    work.article_id, self.name, "",
                    f"{HELD_FOR_REVIEW}: in {len(outcome.studysets)} studysets",
                    fingerprint=work.fingerprint,
                    summary={"base_study_id": outcome.base_study_id, "study_id": outcome.study_id,
                             "studysets": outcome.studysets})
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
        if held:
            logger.warning("upload: %d studies held for review instead of retracted, because a "
                           "studyset holds them: %s", len(held), ", ".join(held))

    def _gather(self, ctx: Context, works: Sequence[Work], excluded=None):
        """Load only this batch's analyses and metadata, never the whole cache.

        Also returns the works holding nothing to upload, and a failure for each
        whose stored analyses predate declared sign splits. The gate in `narrow`
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


#: 1: the coordinate parse sync wrote, posted to the study_schema ingester.
INGEST_UPLOAD_VERSION = 1

#: What a held-back upload's error starts with. Its parse was built from a table or
#: prose reading that is stale or failed, so it may lack analyses neurostore holds, and
#: the ingester would delete or supersede them.
GATED = "held back: upstream not fresh"

#: What an upload records for an article whose text was taken back after it was
#: uploaded: the ingester cannot take a study back, so neurostore keeps the last upload.
NOT_TAKEN_BACK = (f"{NO_TEXT}; the ingester cannot take a study back, so neurostore keeps "
                  "the last upload")

#: The stages the parse is built from, in pipeline order; the prose ones run only with prose.
GATE_STAGES = ("analyses", "passages", "prose", "resolve", "roles", "space", "sync")

#: The readings a person can leave out of an upload as a whole, with an exclusion on the
#: table id `source:<name>` (`ingest exclude <article> -t source:tables --reason ...`),
#: and the stages each covers. The stages after them are never excused.
SOURCE_STAGES = {"tables": ("analyses",), "prose": ("passages", "prose")}
SOURCE_MARK = "source:"

#: Under the dry-run root: one JSON line per article for each dry run of the stage.
DRY_RUN_RUNS = "upload_runs"


def _short(text: Optional[str], limit: int = 200) -> str:
    text = " ".join((text or "no reason recorded").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def upstream_state(name: str, plan: StagePlan, existing: Optional[Artifact],
                   triage: Optional[Artifact] = None) -> Optional[str]:
    """Why a stage's output for one article is not fresh, from that stage's own plan of it.

    None when it is fresh, or when the stage has nothing for the article (it skipped it,
    or, for `analyses`, triage passed no table and so there is nothing to read).
    """
    if plan.fresh or plan.skipped:
        return None
    if plan.pending:
        if is_take_back(plan.pending[0]):
            return "taken back"
        if existing is None:
            if name == "analyses" and triage is not None and triage.status is Status.OK \
                    and (triage.summary or {}).get("passed") == 0:
                return None
            return "not run"
        if existing.status is Status.OK:
            return "stale"
        return f"failed: {_short(existing.error)}"
    if plan.permanent:
        if existing is not None and existing.status is Status.SKIPPED:
            return None
        # FAILED here is waiting out its retry backoff, or has used up its tries.
        if existing is not None and existing.status is Status.FAILED:
            return f"failed: {_short(existing.error)}"
        return f"failed, not retried: {_short(existing.error if existing else None)}"
    reasons = ", ".join(plan.reasons)
    return "blocked" + (f" ({reasons})" if reasons else "")


def _excused(name: str, marks: Dict[str, Dict[str, str]]) -> Optional[str]:
    """The recorded reason a person left this stage's reading out of the upload, if any."""
    for key in [excl.WHOLE_ARTICLE] + [
            SOURCE_MARK + source for source, stages in SOURCE_STAGES.items() if name in stages]:
        reason = (marks.get(key) or {}).get("reason")
        if reason:
            return reason
    return None


def _last_upload(existing: Optional[Artifact]) -> Optional[Dict[str, Any]]:
    """What the last successful upload of the article sent, carried through later failures."""
    if existing is None:
        return None
    summary = existing.summary or {}
    if existing.status is Status.OK and existing.fingerprint != NO_TEXT:
        return {k: summary.get(k) for k in ("base_study_id", "parse_id", "analyses")} | {
            "at": existing.updated_at, "backend": summary.get("backend", "legacy")}
    return summary.get("last_upload")


class IngesterUploadStage:
    """Post the coordinate parse sync wrote to neurostore's study_schema ingester.

    One call per article. The ingester keys analyses by the parse's keys and deletes or
    supersedes the ones a new parse lacks, so a parse built from a table or prose
    reading that is not fresh is held back (`GATED`) with what is behind and how many
    analyses it holds against the last upload. Each record's summary names the backend,
    whether it was a dry run, and the ingester's summary or error.

    A dry run (`dry_run`) validates each parse and writes its request body under the
    ingester's dry-run root, with one JSON line per article under `upload_runs/`; it
    records nothing in the catalog and never touches an upload artifact.
    """

    name = "upload"
    requires = "sync"

    def __init__(self, settings) -> None:
        self.settings = settings
        self.requires = self.upstream_for(settings)[0]
        self._ingester = None
        self._stopped: Optional[BaseException] = None
        self._chain: Optional[List[Tuple[str, Any]]] = None
        self._dry_path: Optional[Path] = None
        self._dry_counts: Dict[str, int] = {}

    @classmethod
    def upstream_for(cls, settings):
        """`sync`; `space` under the deprecated direct-database upload, which runs first."""
        from .sync import legacy_upload

        if legacy_upload(settings):
            return UploadStage.requires, UploadStage.requires_flag
        return cls.requires, None

    @property
    def dry_run(self) -> bool:
        return bool(getattr(self.settings, "dry_run", False))

    def fingerprint_for(self, upstream: Artifact) -> str:
        from ingestion_workflow.services.neurostore_ingest import api_url

        # A different API is a different neurostore, so the same parse goes to it anew.
        return fingerprint(
            "upload", "ingester", INGEST_UPLOAD_VERSION, api_url(self.settings),
            bool(self.settings.neurostore_upload_parsed_paper),
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
        candidates = []
        for ref in refs:
            synced = upstream.get(ref.id, {}).get("")
            existing = artifacts.get(ref.id, {}).get("")
            if synced is None or synced.status is not Status.OK:
                if taken_back(synced) and (existing is None or existing.fingerprint != NO_TEXT):
                    plan.pending.append(Work(ref=ref, source="", fingerprint=NO_TEXT))
                else:
                    plan.blocked += 1
                continue
            fp = self.fingerprint_for(synced)
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            # Only a failure on these same inputs uses up a retry: a parse sync wrote
            # again is tried again, and a held-back upload is not a failed try.
            if existing is not None and existing.fingerprint == fp \
                    and not (existing.error or "").startswith(GATED):
                if existing.status is Status.SKIPPED:
                    plan.skipped += 1
                    continue
                count, last = attempts.get(ref.id, (0, None))
                if not ctx.should_attempt(existing, count, last, self.name):
                    plan.permanent += 1
                    continue
            candidates.append((ref, synced, existing, fp))
        held = self.held_back(ctx, [c[0] for c in candidates],
                              {ref.id: synced for ref, synced, _, _ in candidates}, artifacts)
        for ref, synced, existing, fp in candidates:
            gate = held.get(ref.id)
            if gate and gate.get("reason") and not self.dry_run and existing is not None \
                    and existing.fingerprint == fp and existing.error == gate["reason"]:
                plan.block(GATED)  # recorded already, with this reason
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=synced))
        return plan

    def gate_stages(self) -> List[Tuple[str, Any]]:
        """(name, stage) for the stages the parse is built from, as this run builds them."""
        if self._chain is None:
            from . import PROSE_STAGES, STAGE_TYPES

            prose = bool(getattr(self.settings, "prose_model", None))
            self._chain = [(name, STAGE_TYPES[name](self.settings)) for name in GATE_STAGES
                           if prose or name not in PROSE_STAGES]
        return self._chain

    def held_back(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        synced: Dict[str, Artifact],
        uploaded: Dict[str, Dict[str, Artifact]],
    ) -> Dict[str, Dict[str, Any]]:
        """{article_id: why its upload is held back} for the articles whose parse may lack
        analyses: a stage it is built from is not fresh, by that stage's own plan, and no
        person left that reading out with a reason. An article that passes only because a
        person did gets `{"excused": ...}` and no reason."""
        if not refs:
            return {}
        ids = [ref.id for ref in refs]
        marks = ctx.catalog.exclusions(ids)
        triaged = ctx.catalog.artifacts(ids, "triage")
        roled = ctx.catalog.artifacts(ids, "roles")
        behind: Dict[str, Dict[str, str]] = {ref.id: {} for ref in refs}
        for name, stage in self.gate_stages():
            found = ctx.catalog.artifacts(ids, name)
            parents = ctx.catalog.artifacts(ids, stage.requires) if stage.requires else {}
            for ref in refs:
                state = upstream_state(
                    name, stage.plan(ctx, [ref], found, parents),
                    found.get(ref.id, {}).get(""), triaged.get(ref.id, {}).get(""))
                if state:
                    behind[ref.id][name] = state
        out = {}
        for ref in refs:
            excused = {name: why for name in behind[ref.id]
                       if (why := _excused(name, marks.get(ref.id, {})))}
            left = {name: state for name, state in behind[ref.id].items() if name not in excused}
            if not left:
                if excused:  # passes; the summary says what a person left out
                    out[ref.id] = {"excused": excused}
                continue
            holds = (synced[ref.id].summary or {}).get("analyses") or 0
            last = _last_upload(uploaded.get(ref.id, {}).get(""))
            before = (last or {}).get("analyses")
            roles = roled.get(ref.id, {}).get("")
            reason = f"{GATED}: " + "; ".join(f"{name} {state}" for name, state in left.items())
            reason += f"; the parse holds {holds} analyses"
            reason += (", and nothing was uploaded before" if before is None else
                       f", the last upload {before} ({max(before - holds, 0)} missing)")
            out[ref.id] = {
                "reason": reason,
                "upstream": left,
                **({"excused": excused} if excused else {}),
                "parse_analyses": holds,
                "last_uploaded_analyses": before,
                "missing": max(before - holds, 0) if before is not None else None,
                "sets": ((roles.summary or {}).get("sets") if roles is not None else None),
            }
        return out

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        if self._stopped is not None:
            self.close()
            raise self._stopped
        previous = ctx.catalog.artifacts([w.article_id for w in works], self.name)
        back = [w for w in works if is_take_back(w)]
        works = [w for w in works if not is_take_back(w)]
        for work in back:
            last = _last_upload(previous.get(work.article_id, {}).get(""))
            yield from self._record(work, Outcome.failure(
                work.article_id, self.name, "", NOT_TAKEN_BACK if last else NO_TEXT,
                fingerprint=NO_TEXT, summary=self._summary(None, last)))
        held = self.held_back(ctx, [w.ref for w in works],
                              {w.article_id: w.upstream for w in works}, previous)
        learned: List[Tuple[str, str, str]] = []
        try:
            yield from self._upload(ctx, works, previous, held, learned)
        finally:
            if learned:
                ctx.catalog.add_aliases(learned)

    def _upload(self, ctx, works, previous, held, learned) -> Iterator[Outcome]:
        from ingestion_workflow.services.neurostore_ingest import StopRun

        for work in works:
            last = _last_upload(previous.get(work.article_id, {}).get(""))
            summary = self._summary(work.upstream, last)
            gate = held.get(work.article_id)
            if gate and gate.get("reason"):
                yield from self._record(work, Outcome.failure(
                    work.article_id, self.name, "", gate["reason"], fingerprint=work.fingerprint,
                    summary={**summary, "gate": gate}))
                continue
            summary["gate"] = gate
            if not summary["analyses"] and last is None:
                # Never uploaded and nothing to say: the ingester would create a study
                # claiming the paper reports no coordinates.
                yield from self._record(work, Outcome(
                    article_id=work.article_id, stage=self.name, source="",
                    status=Status.SKIPPED, fingerprint=work.fingerprint,
                    summary={**summary, "reason": "no analyses to upload"}))
                continue
            try:
                artifact = self.ingester().upload(Path(work.upstream.summary.get("path") or ""))
            except StopRun as exc:
                # Recorded, then the run stops before the next batch: every later
                # article would fail the same way.
                self._stopped = exc
                logger.error("upload: stopping the run: %s", exc)
                yield from self._record(work, Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint, summary=summary))
                return
            except Exception as exc:  # noqa: BLE001 - this article's failure, not the batch's
                logger.warning("upload failed for %s: %s", work.article_id, exc)
                yield from self._record(work, Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {_short(str(exc))}",
                    fingerprint=work.fingerprint, summary=summary))
                continue
            yield from self._record(work, self._outcome(work, artifact, summary, learned))

    def _summary(self, synced: Optional[Artifact],
                 last: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        found = (synced.summary or {}) if synced is not None else {}
        return {
            "backend": "ingester",
            "dry_run": self.dry_run,
            "base_study_id": found.get("base_study_id"),
            "parse_id": found.get("parse_id"),
            "analyses": found.get("analyses"),
            "gate": None,
            **({"last_upload": last} if last else {}),
        }

    def _outcome(self, work: Work, artifact: Dict[str, Any], summary: Dict[str, Any],
                 learned: List[Tuple[str, str, str]]) -> Outcome:
        from ingestion_workflow.services.neurostore_ingest import Result

        result = Result(artifact["status"])
        record = {k: v for k, v in artifact.items() if k not in ("history", "last_uploaded")}
        summary = {**summary, "ingester": record}
        status = result.catalog_status
        if status is Status.OK:
            base_study_id = (record.get("summary") or {}).get("base_study_id")
            if base_study_id:
                summary["base_study_id"] = base_study_id
                learned.append((work.article_id, "neurostore", base_study_id))
            summary.pop("last_upload", None)
            return Outcome(article_id=work.article_id, stage=self.name, source="",
                           status=Status.OK, fingerprint=work.fingerprint, summary=summary)
        error = record.get("error") or {}
        detail = error.get("detail") or error.get("title") or "no detail"
        if status is None:  # a dry run: the outcome goes to the dry run's own record
            return Outcome(article_id=work.article_id, stage=self.name, source="",
                           status=Status.SKIPPED, fingerprint=work.fingerprint,
                           summary={**summary, "reason": result.value})
        return Outcome.failure(
            work.article_id, self.name, "", f"{result.value}: {_short(str(detail))}",
            permanent=status is Status.PERMANENT, fingerprint=work.fingerprint, summary=summary)

    def _record(self, work: Work, outcome: Outcome) -> Iterator[Outcome]:
        """The outcome for the catalog; in a dry run, a line in the dry run's own record."""
        if not self.dry_run:
            yield outcome
            return
        status = (outcome.summary.get("ingester") or {}).get("status") or (
            "held back" if outcome.summary.get("gate") else
            outcome.summary.get("reason") or outcome.status.value)
        self._dry_counts[status] = self._dry_counts.get(status, 0) + 1
        line = {"article_id": work.article_id, "status": status, "error": outcome.error,
                **outcome.summary}
        path = self.dry_run_log()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")

    def dry_run_log(self) -> Path:
        """This run's dry-run record: one JSON line per article."""
        if self._dry_path is None:
            from ingestion_workflow.services.neurostore_ingest import DRY_RUN_DIR

            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            self._dry_path = (Path(self.settings.data_root) / DRY_RUN_DIR / DRY_RUN_RUNS
                              / f"{stamp}.jsonl")
        return self._dry_path

    def ingester(self):
        if self._ingester is None:
            from ingestion_workflow.services.neurostore_ingest import NeurostoreIngester

            self._ingester = NeurostoreIngester.from_settings(self.settings)
        return self._ingester

    def close(self) -> None:
        if self._ingester is not None:
            self._ingester.close()
            self._ingester = None

    def finish(self) -> None:
        self.close()
        if self._dry_counts:
            logger.info("upload dry run: %s; written to %s", ", ".join(
                f"{n:,} {status}" for status, n in sorted(self._dry_counts.items())),
                self._dry_path)
            self._dry_counts = {}
            self._dry_path = None
        if self._stopped is not None:
            stopped, self._stopped = self._stopped, None
            raise stopped
