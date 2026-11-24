"""Post each article's coordinate parse to neurostore's study_schema ingester."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import NO_TEXT, Context, is_take_back, take_back_or_block
from ..upstream import GATED, Assessor, Verdict, short_reason

logger = logging.getLogger(__name__)

#: 1: the coordinate parse sync wrote, posted to the study_schema ingester.
INGEST_UPLOAD_VERSION = 1

#: Under the dry-run root: one JSON line per article for each dry run of the stage.
DRY_RUN_RUNS = "upload_runs"
#: Why every article waits when no neurostore is named.
NO_API_URL = "no neurostore_api_url is configured"


def _sent_by_ingester(existing: Optional[Artifact]) -> bool:
    return existing is not None and (existing.summary or {}).get("backend") == "ingester"


#: What an upload's summary keeps of the parse it sent, carried through later failures.
KEPT = ("base_study_id", "parse_id", "analyses", "text_sha256", "text_length", "revisions")


def _last_upload(existing: Optional[Artifact]) -> Optional[Dict[str, Any]]:
    """What the last successful upload of the article sent, carried through later failures:
    with the blob of the parse it sent (`parse_blob`) and the ingester it went to."""
    if existing is None:
        return None
    summary = existing.summary or {}
    if existing.status is Status.OK and existing.fingerprint != NO_TEXT:
        return {k: summary.get(k) for k in KEPT} | {
            "at": existing.updated_at, "backend": summary.get("backend", "legacy"),
            "parse_blob": existing.blob,
            "endpoint": (summary.get("ingester") or {}).get("endpoint")}
    return summary.get("last_upload")


def not_kept(parse_id: str) -> str:
    """Why a paper whose stored parse is not kept here is not sent."""
    return (f"{GATED}: Neurostore holds parse {parse_id} for this paper, which is not kept "
            "here, so this parse would replace it rather than revise it; set "
            "upload_revised_root to a corpus tree holding it")


class UploadStage:
    """Post the coordinate parse sync wrote to neurostore's study_schema ingester.

    One call per article. A paper uploaded before goes as a revision of the parse sent
    then (`parse_revision`), so what Neurostore holds against an analysis the new parse
    keeps, corrects, splits or merges is carried to it; a paper whose earlier parse is not
    kept (the artifact's blob, else `upload_revised_root`) is held back, since a plain
    parse would supersede it. Each upload keeps the parse it sent as its blob, and its
    summary the text it was read from and the `revisions` since the paper's first parse.

    The ingester keys analyses by the parse's keys and supersedes
    the ones a new parse lacks, so a parse built from a table or prose reading that is
    not fresh is held back (`GATED`) with what is behind and how many analyses it holds
    against the last upload. Each record's summary names the backend, whether it was a
    dry run, and the ingester's summary or error.

    An article whose analyses were taken back is recorded once with the take-back's
    reason, and is not sent again; nothing is deleted from neurostore. When it had been
    uploaded, a warning names it, its base study and the reason, so a person can look at
    what neurostore keeps.

    An upload neurostore filed under the base study a merge kept, not the one the header
    names, is an upload: the base study its summary names is learned as an alias.

    When the client raises a `StopRun` (the API is down, or the token or the URL is
    wrong for every paper), the stage stops for the rest of the run: that article and
    the rest stay pending, and nothing is recorded for them.

    A dry run (`dry_run`) validates each parse and writes its request body under the
    ingester's dry-run root, with one JSON line per article under `upload_runs/`; it
    records nothing in the catalog and never touches an upload artifact.
    """

    name = "upload"
    requires = "sync"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._ingester = None
        #: Why the stage stopped for the rest of this run.
        self._stopped: Optional[str] = None
        self.assessor = Assessor(settings)
        self._dry_path: Optional[Path] = None
        self._dry_counts: Dict[str, int] = {}
        #: {article: the parse it revises, or None} and {article: what was sent}, per batch.
        self._revised: Dict[str, Any] = {}
        self._sent: Dict[str, Dict[str, Any]] = {}


    @property
    def dry_run(self) -> bool:
        return bool(getattr(self.settings, "dry_run", False))

    def fingerprint_for(self, upstream: Artifact) -> str:
        # A different API is a different neurostore, so the same parse goes to it anew.
        return fingerprint(
            "upload", "ingester", INGEST_UPLOAD_VERSION,
            (self.settings.neurostore_api_url or "").rstrip("/"), upstream=upstream.fingerprint,
        )

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        if not self.settings.neurostore_api_url:
            for ref in refs:
                plan.wait(ref, NO_API_URL)
            return plan
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        candidates = []
        for ref in refs:
            synced = upstream.get(ref.id, {}).get("")
            existing = artifacts.get(ref.id, {}).get("")
            if synced is None or synced.status is not Status.OK:
                take_back_or_block(plan, ref, synced, existing)
                continue
            fp = self.fingerprint_for(synced)
            # Only this stage's own record can be fresh: a direct-database upload's, a
            # migrated one with no fingerprint among them, never sent this parse.
            if _sent_by_ingester(existing) and ctx.is_fresh(existing, fp):
                plan.add_fresh(ref)
                continue
            # Only a failure on these same inputs uses up a retry: a parse sync wrote
            # again is tried again, and a held-back upload is not a failed try.
            if existing is not None and existing.fingerprint == fp \
                    and not (existing.error or "").startswith(GATED):
                if existing.status is Status.SKIPPED:
                    plan.add_skipped(ref)
                    continue
                count, last = attempts.get(ref.id, (0, None))
                if not ctx.should_attempt(existing, count, last, self.name):
                    plan.add_permanent(ref)
                    continue
            candidates.append((ref, synced, existing, fp))
        held = self.held_back(ctx, [c[0] for c in candidates],
                              {ref.id: synced for ref, synced, _, _ in candidates}, artifacts)
        for ref, synced, existing, fp in candidates:
            gate = held.get(ref.id)
            if gate and gate.get("reason") and not self.dry_run and existing is not None \
                    and existing.fingerprint == fp and existing.error == gate["reason"]:
                plan.block(ref, GATED)  # recorded already, with this reason
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=synced))
        return plan

    def held_back(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        synced: Dict[str, Artifact],
        uploaded: Dict[str, Dict[str, Artifact]],
    ) -> Dict[str, Dict[str, Any]]:
        """{article_id: why its upload is held back} for the articles `upstream.decide` does
        not let through, with how many analyses the parse holds against the last upload. An
        article that passes only because a person excused a reading gets `{"excused": ...}`
        and no reason."""
        if not refs:
            return {}
        assessed = self.assessor.assess(ctx, refs)
        roled = ctx.catalog.artifacts([ref.id for ref in refs], "roles")
        out = {}
        for ref in refs:
            found = assessed[ref.id]
            if found.verdict is Verdict.PROCEED:
                if found.excused:  # passes; the summary says what a person left out
                    out[ref.id] = {"excused": found.excused}
                continue
            holds = (synced[ref.id].summary or {}).get("analyses") or 0
            last = _last_upload(uploaded.get(ref.id, {}).get(""))
            before = (last or {}).get("analyses")
            roles = roled.get(ref.id, {}).get("")
            reason = f"{GATED}: {found.reason}; the parse holds {holds} analyses"
            reason += (", and nothing was uploaded before" if before is None else
                       f", the last upload {before} ({max(before - holds, 0)} missing)")
            out[ref.id] = {
                "reason": reason,
                "verdict": found.verdict.value,
                "upstream": found.behind,
                **({"excused": found.excused} if found.excused else {}),
                "parse_analyses": holds,
                "last_uploaded_analyses": before,
                "missing": max(before - holds, 0) if before is not None else None,
                "sets": ((roles.summary or {}).get("sets") if roles is not None else None),
            }
        return out

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        if self._stopped:
            logger.error("upload stopped: %s; %d article(s) left pending",
                         self._stopped, len(works))
            return
        previous = ctx.catalog.artifacts([w.article_id for w in works], self.name)
        back = [w for w in works if is_take_back(w)]
        works = [w for w in works if not is_take_back(w)]
        for work in back:
            last = _last_upload(previous.get(work.article_id, {}).get(""))
            if last is not None:
                logger.warning(
                    "upload: %s was taken back (%s) after it was uploaded to base study %s; "
                    "neurostore keeps that upload, so look at it there",
                    work.article_id, work.reason, last.get("base_study_id"))
            # Recorded with its reason and never sent again; neurostore keeps what it holds.
            yield from self._record(work, Outcome.failure(
                work.article_id, self.name, "", work.reason, fingerprint=NO_TEXT,
                summary={**self._summary(None, last), "reason": work.reason,
                         "base_study_id": (last or {}).get("base_study_id")}))
        held = self.held_back(ctx, [w.ref for w in works],
                              {w.article_id: w.upstream for w in works}, previous)
        learned: List[Tuple[str, str, str]] = []
        try:
            yield from self._upload(ctx, works, previous, held, learned)
        finally:
            if learned:
                ctx.catalog.add_aliases(learned)

    def _upload(self, ctx, works, previous, held, learned) -> Iterator[Outcome]:
        sending: List[Tuple[Work, Dict[str, Any]]] = []
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
            revised, why = self.revised_for(ctx, work, last)
            if why:
                yield from self._record(work, Outcome.failure(
                    work.article_id, self.name, "", why, fingerprint=work.fingerprint,
                    summary=summary))
                continue
            self._revised[work.article_id] = revised
            if not summary["analyses"] and last is None:
                # Never uploaded and nothing to say: the ingester would create a study
                # claiming the paper reports no coordinates.
                yield from self._record(work, Outcome(
                    article_id=work.article_id, stage=self.name, source="",
                    status=Status.SKIPPED, fingerprint=work.fingerprint,
                    summary={**summary, "reason": "no analyses to upload"}))
                continue
            sending.append((work, summary))
        try:
            yield from self._send_all(sending, learned)
        finally:
            self._revised.clear()
            self._sent.clear()

    def revised_for(self, ctx: Context, work: Work, last: Optional[Dict[str, Any]]):
        """(the parse Neurostore holds for the paper, which this one revises, or None for a
        plain parse; why the paper is held back instead, or None). A paper never sent through
        the ingester, or sent to another neurostore, gets a plain parse."""
        from ingestion_workflow.services.neurostore_ingest import UPLOAD_PATH, Revised

        if not last or last.get("backend") != "ingester" or not last.get("parse_id"):
            return None, None
        mine = (self.settings.neurostore_api_url or "").rstrip("/") + UPLOAD_PATH
        if last.get("endpoint") not in (None, mine):
            return None, None
        held = ctx.catalog.blobs.get(last.get("parse_blob")) if last.get("parse_blob") else None
        if held is not None and held.get("parse_id") == last["parse_id"]:
            return Revised(held, last.get("text_sha256"), last.get("text_length")), None
        root = getattr(self.settings, "upload_revised_root", None)
        if root:
            parse_dir = Path(root) / Path(work.upstream.summary.get("path") or "").name / "parse"
            try:
                held = json.loads((parse_dir / "coordinate_parse.json").read_text("utf-8"))
                paper = json.loads((parse_dir / "parsed_paper.json").read_text("utf-8"))
            except (OSError, ValueError):
                held = None
            if held is not None and held.get("parse_id") == last["parse_id"]:
                return Revised(held, paper.get("text_sha256"), paper.get("text_length")), None
        return None, not_kept(last["parse_id"])

    def _send_all(self, sending, learned) -> Iterator[Outcome]:
        """Each paper sent, `neurostore_upload_workers` at once, its outcome as it is answered.

        A StopRun (the token refused, neurostore down) starts no other paper: those under
        way are answered and recorded, and the rest stay pending."""
        from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

        from ingestion_workflow.services.neurostore_ingest import StopRun

        if not sending:
            return
        try:
            ingester = self.ingester()  # no client, or no tenant: nothing is sent
        except StopRun as exc:
            self._stopped = f"{type(exc).__name__}: {exc}"
            logger.error("upload stopped: %s; %d article(s) left pending",
                         self._stopped, len(sending))
            return
        workers = max(1, int(getattr(self.settings, "neurostore_upload_workers", 1) or 1))
        queue = list(reversed(sending))
        window: Dict[Any, Tuple[Work, Dict[str, Any]]] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="upload") as pool:
            while window or (queue and not self._stopped):
                while queue and not self._stopped and len(window) < workers:
                    work, summary = queue.pop()
                    window[pool.submit(self._send_one, ingester, work)] = (work, summary)
                done, _ = wait(window, return_when=FIRST_COMPLETED)
                for future in done:
                    work, summary = window.pop(future)
                    try:
                        artifact = future.result()
                    except StopRun as exc:
                        # Not this article's failure: recording one would spend every
                        # remaining article's retries. This article and the rest stay pending.
                        if not self._stopped:
                            self._stopped = f"{type(exc).__name__}: {exc}"
                            logger.error("upload stopped: %s; %d article(s) left pending",
                                         self._stopped, len(queue) + len(window) + 1)
                        continue
                    except Exception as exc:  # noqa: BLE001 - this article's failure
                        logger.warning("upload failed for %s: %s", work.article_id, exc)
                        yield from self._record(work, Outcome.failure(
                            work.article_id, self.name, "",
                            f"{type(exc).__name__}: {short_reason(str(exc))}",
                            fingerprint=work.fingerprint, summary=summary))
                        continue
                    yield from self._record(work, self._outcome(work, artifact, summary, learned))

    def _send_one(self, ingester, work: Work) -> Dict[str, Any]:
        """Send one paper; its upload artifact."""
        sent: Dict[str, Any] = {}
        artifact = ingester.upload(Path(work.upstream.summary.get("path") or ""),
                                   revised=self._revised.get(work.article_id), sent=sent)
        self._sent[work.article_id] = sent
        return artifact

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
        record = {k: v for k, v in artifact.items()
                  if k not in ("history", "last_uploaded", "revision")}
        step = artifact.get("revision")
        if step is not None:
            record["revision"] = {k: step[k] for k in ("revision_of", "verdicts", "text")}
        summary = {**summary, "ingester": record}
        status = result.catalog_status
        if status is Status.OK:
            base_study_id = (record.get("summary") or {}).get("base_study_id")
            if base_study_id:
                summary["base_study_id"] = base_study_id
                learned.append((work.article_id, "neurostore", base_study_id))
            last = summary.pop("last_upload", None) or {}
            sent = self._sent.get(work.article_id) or {}
            paper = sent.get("parsed_paper") or {}
            revised = self._revised.get(work.article_id)
            # The revisions since the paper's first parse: those of the parse this one
            # revises or repeats, and this one's.
            prior = list(last.get("revisions") or []) if revised is not None or (
                last.get("parse_id") == summary.get("parse_id")) else []
            summary.update(text_sha256=paper.get("text_sha256"),
                           text_length=paper.get("text_length"),
                           revisions=prior + ([step] if step is not None else []))
            return Outcome(article_id=work.article_id, stage=self.name, source="",
                           status=Status.OK, fingerprint=work.fingerprint, summary=summary,
                           payload=sent.get("coordinate_parse"))
        error = record.get("error") or {}
        detail = error.get("detail") or error.get("title") or "no detail"
        if error.get("codes"):  # KEY_MISMATCH, DUPLICATE_KEY, NOT_CURRENT, ...
            detail = f"{', '.join(error['codes'])}: {detail}"
        if status is None:  # a dry run: the outcome goes to the dry run's own record
            return Outcome(article_id=work.article_id, stage=self.name, source="",
                           status=Status.SKIPPED, fingerprint=work.fingerprint,
                           summary={**summary, "reason": result.value})
        return Outcome.failure(
            work.article_id, self.name, "", f"{result.value}: {short_reason(str(detail))}",
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
