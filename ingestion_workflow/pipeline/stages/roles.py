"""Decide what each coordinate set is for: this study's result, an anchor, a quoted peak.

Required: it runs between `resolve` (or `analyses`, without prose) and
`space`, and space, upload and sync read nothing it has not passed. Every set's
role comes from the decision model (`services.set_roles.decision`, `role_model`),
one encoder for table and prose sets alike, loaded in this process, each set
read through its origin's context builder. There is no proposal and no default
role: while the model is not configured, or cannot be used, no article with a
set is attempted -- the plan counts it as blocked, with the reason -- so it uses
up no retries, and it runs as soon as the model is configured. Everything
downstream of it stays blocked meanwhile.
The payload it writes is described in docs/set-roles-artifact.md.
"""

from __future__ import annotations

import collections
import logging
import re
import contextlib
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.services.set_roles import (
    decide,
    decision,
    geometry,
    prose_context,
    table_context,
)
from ingestion_workflow.services.set_roles.decision import CONTEXT_VERSIONS, DecisionModel
from ingestion_workflow.services.set_roles.labels import RoleDecision, SetRole, is_decided

from ..plan import StagePlan, Work
from ..stage import Context, alone, take_back_or_block, taking_back
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .passages import passage_from, read_text
from .space import _article_text

logger = logging.getLogger(__name__)

#: Bump when what the stage writes changes, apart from the models and the contexts.
#: 6: every set has `space` and `coordinate_source`, and a set in another space is held.
ROLES_VERSION = 6

#: Articles classified at once, their inputs held in memory meanwhile: their sets
#: share the model's batches (`decision.DecisionModel`), so a batch fills even when
#: every article has a single set.
ARTICLES = 64

#: Why roles waits on an article whose prose indexes a passages list other than the current one.
RESOLVE_READ_OTHER_PROSE = ("its resolve was made from other prose: "
                            "roles waits for resolve to read this prose")
PROSE_READ_OTHER_PASSAGES = "its prose read other passages: roles waits for prose to read these"



def _span(sentence: str, text: Optional[str]) -> dict:
    """A TextSpan for a sentence at its first occurrence in the article, else the text alone."""
    if text:
        found = re.search(r"\s+".join(map(re.escape, sentence.split())), text)
        if found:
            return {"start_char": found.start(), "end_char": found.end(), "text": found.group(0)}
    return {"text": sentence}


def _table_text(
    analysis: Mapping[str, Any], raw: Optional[Path] = None
) -> Tuple[str, Optional[str]]:
    """(The table's serialisation, as nu-v21 read it; None) or ("", why it cannot be read).

    `raw` is the table's raw content in the extraction the analyses were read
    from: the file nu-v21 was given. Most extractors (pubget, ACE, PDF, PMC)
    record no path in the table's metadata, so it is there that the file is found.
    """
    from ingestion_workflow.services.create_analyses import CreateAnalysesService  # noqa: PLC0415

    meta = (analysis.get("metadata") or {}).get("table_metadata") or {}
    named = [p for p in (meta.get("raw_content_path"), meta.get("raw_xml_path"), raw) if p]
    path = next((Path(p) for p in named if Path(p).is_file()), None)
    if path is None:
        return "", (f"its raw table file is missing ({named[-1]})" if named
                    else "no raw table file is recorded for it")
    try:
        # As nu-v21's `_read_table_content` read it.
        try:
            markup = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            markup = path.read_text(encoding="utf-8", errors="ignore")
        return CreateAnalysesService._serialise(markup, meta.get("table_id") or ""), None
    except Exception as exc:  # noqa: BLE001 - the set is read without its rows, and says so
        return "", f"its raw table file cannot be read ({path}): {type(exc).__name__}: {exc}"


def set_contexts(
    payload: Mapping[str, Any],
    passages: Sequence[Mapping[str, Any]],
    text: Optional[str],
    raw_tables: Optional[Mapping[str, Path]] = None,
    unread: Optional[Dict[str, str]] = None,
):
    """(table id, analysis index, origin, builder module, context) for every analysis.

    `raw_tables` is each table's raw content file by table id (`raw_tables`).
    A table whose sets are read without its header and rows is put in
    `unread`, by table id, with the reason.
    """
    for table_id, collection in (payload or {}).items():
        analyses = (collection or {}).get("analyses", [])
        table_text = None
        for index, analysis in enumerate(analyses):
            if (analysis.get("metadata") or {}).get("source") == "prose":
                context = prose_context.build(analysis, passages)
                yield table_id, index, "text", prose_context, context
                continue
            if table_text is None:
                table_text, why = _table_text(analysis, (raw_tables or {}).get(table_id))
                if why is not None and unread is not None:
                    if raw_tables and table_id not in raw_tables:
                        why += f"; the extraction has no table {table_id!r}"
                    unread[table_id] = why
            yield (
                table_id,
                index,
                "table",
                table_context,
                table_context.build(
                    analysis,
                    index=index,
                    siblings=analyses,
                    table_text=table_text,
                    article_text=text,
                ),
            )


def assign_roles(
    payload: Mapping[str, Any],
    passages: Sequence[Mapping[str, Any]],
    text: Optional[str],
    classifier: Any,
    raw_tables: Optional[Mapping[str, Path]] = None,
    unread: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The payload with every analysis's role decided by the classifier, and a summary.

    `classifier` (a `decision.DecisionModel`, or the stage's lazy handle on it)
    reads every set, table and prose alike, each through its origin's context
    builder; it raises `ModelUnavailable` when it cannot be used: no set is
    given a role any other way. Each analysis gains `metadata.set_role`
    (`SET_ROLE_FIELDS`) and `metadata.held`. Tables read without their header
    and rows go in `unread` (`set_contexts`). A set that is not this study's
    result or anchor -- a reference, a bare slice position (other), a
    localization, or any set whose coordinates are not human template
    millimetres (space `other`) -- is held: it stays in the collection's
    `analyses`, in its place, so a set's position (pondie's `table_id#ordinal`)
    never moves. The parse writes it with its role, upload sends it, and
    Neurostore's releases export results only.

    A set whose points cannot be template brain coordinates (`geometry.not_brain`,
    given every point of the set's table, or of the prose, for scale) is decided
    before any model is asked: no role, held, with the rule's reason. It
    stays in its place here and is left out of the parse.
    """
    found = list(set_contexts(payload, passages, text, raw_tables, unread))
    source_points: Dict[str, List[Any]] = collections.defaultdict(list)
    for table_id, _, _, _, context in found:
        source_points[table_id] += context.points
    decisions = {}
    asked = []
    for item in found:
        table_id, index, origin, _, context = item
        reason = geometry.not_brain(context.points, source=source_points[table_id])
        if reason is None:
            asked.append(item)
            continue
        decisions[(table_id, index)] = RoleDecision(
            decided=SetRole(None), confidence=1.0, source=geometry.SOURCE, origin=origin,
            reason=reason,
        )
    model_source = None
    if asked:
        # The stage's handle raises `ModelUnavailable` with the reason the model cannot be used.
        predictions = classifier.predict([module.serialize(ctx) for *_, module, ctx in asked])
        if len(predictions) != len(asked):
            raise ValueError(f"the role model answered {len(predictions)} of {len(asked)} sets")
        model_source = classifier.source
        for (table_id, index, origin, module, context), prediction in zip(asked, predictions):
            decisions[(table_id, index)] = decide(
                prediction,
                source=model_source,
                origin=origin,
                evidence=module.prior_evidence(context),
            )
    out: Dict[str, Any] = {}
    roles, held = collections.Counter(), 0
    values, records = [], []
    for table_id, collection in (payload or {}).items():
        kept = []
        for index, analysis in enumerate((collection or {}).get("analyses", [])):
            decision = decisions[(table_id, index)]
            metadata = decision.to_metadata()
            metadata["prior_study_evidence"] = [
                _span(e["text"], text) for e in metadata["prior_study_evidence"]
            ]
            analysis = {
                **analysis,
                "metadata": {
                    **(analysis.get("metadata") or {}),
                    "set_role": metadata,
                    "held": decision.held,
                },
            }
            # An anchor by its kind.
            if decision.role is not None:
                roles[decision.anchor_kind or decision.role] += 1
            held += decision.held
            kept.append(analysis)
            values.append([table_id, index, decision.role, decision.anchor_kind,
                           decision.from_prior_study, decision.space,
                           decision.coordinate_source, decision.held])
            records.append([table_id, index, metadata, decision.held])
        out[table_id] = {**collection, "analyses": kept}
    summary = {
        "tables": sum(1 for c in out.values() if (c or {}).get("analyses")),
        "sets": len(found),
        "sets_by_origin": dict(sorted(collections.Counter(o for _, _, o, *_ in found).items())),
        # Prose sets whose points the classifier could not find in their passage.
        "text_sets_without_local": sum(not ctx.local for _, _, o, _, ctx in found if o == "text"),
        "roles": dict(roles),
        "held": held,
        # Sets refused by `geometry` before any model was asked.
        "not_brain": sum(d.source == geometry.SOURCE for d in decisions.values()),
        # Sets the model put in another frame than human template millimetres: held.
        "other_space": sum(d.space == "other" for d in decisions.values()),
        # The model that decided every set the geometry check passed; None when it refused all.
        "model": model_source,
        # The decided roles alone, without confidences or the model that
        # decided them: what upload's freshness follows, so a retrained model
        # re-uploads only the articles where a set's role changed.
        "role_values": fingerprint("role-values", values),
        # Every set's whole role record, with its confidence, evidence and model:
        # what sync's freshness follows, since the parse writes all of them.
        "role_records": fingerprint("role-records", records),
    }
    return out, summary


def unassigned(payload: Mapping[str, Any]) -> List[str]:
    """`table_id#index` of every set, held or not, without a role the roles stage decided.

    What upload and sync check before writing anything: a set missing its
    `metadata.set_role` or `metadata.held`, or carrying a role that is
    incomplete or not study_schema's, is never written with a guessed role.
    """
    out = []
    for table_id, collection in (payload or {}).items():
        for index, analysis in enumerate((collection or {}).get("analyses") or []):
            if not is_decided((analysis or {}).get("metadata") or {}):
                out.append(f"{table_id}#{index}")
    return out


def refuse_unassigned(
    stage: str, work: Work, payload: Optional[Mapping[str, Any]]
) -> Optional[Outcome]:
    """A failure for `stage` when the payload it would write holds a set without a decided role."""
    missing = unassigned(payload or {})
    if not missing:
        return None
    shown = ", ".join(missing[:5]) + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
    return Outcome.failure(
        work.article_id,
        stage,
        "",
        f"sets without a role from the roles stage: {shown}; run roles first",
        fingerprint=work.fingerprint,
    )


def _read_passages(ctx: Context, payload: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The passages a payload indexes, as the text they are spans of prints them.

    A text that changed under its passages raises, so the article fails rather
    than have its prose sets classified from nothing.
    """
    stored = (payload or {}).get("passages") or []
    if not stored:
        return []
    text = read_text(ctx, payload)
    return [
        {"text": p.text, "heading": p.heading, "before": p.before, "after": p.after}
        for p in (passage_from(s, text) for s in stored)
    ]


def raw_tables(triage: Optional[Artifact], extractions: Mapping[str, Artifact],
               ctx: Context) -> Dict[str, Path]:
    """Each table's raw content file by table id, in the extraction triage judged.

    The extraction the analyses were read from, so the file is the one nu-v21 serialised.
    """
    source = (triage.summary or {}).get("source") if triage else None
    extraction = extractions.get(source) if source is not None else None
    if extraction is None or extraction.status is not Status.OK:
        return {}
    out = {}
    for table in (ctx.payload(extraction) or {}).get("tables") or []:
        path = ctx.recorded_path(table.get("raw_content_path"))
        if table.get("table_id") and path is not None:
            out[table["table_id"]] = path
    return out


def with_roles(ctx: Context, ids: Sequence[str]) -> Set[str]:
    """The articles whose roles artifact is OK: the only ones downstream stages may write."""
    found = ctx.catalog.artifacts(list(ids), "roles")
    return {
        article_id
        for article_id, by_source in found.items()
        if (by_source.get("") is not None and by_source[""].status is Status.OK)
    }


class RolesStage:
    name = "roles"
    requires = "analyses"
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings
        self.requires, self.requires_flag = self.upstream_for(settings)
        self._model: Optional[DecisionModel] = None
        self._lock = threading.Lock()
        self._said: Set[str] = set()
        #: The model state remembered inside `remembering()`; None outside it.
        self._state: Optional[Tuple[Optional[str], Optional[str]]] = None
        self._remembering = False
        #: Why the stage stopped in this run: no model is loaded after it, and none is asked.
        self._stopped: Optional[str] = None

    @classmethod
    def upstream_for(cls, settings):
        """Read `resolve` when prose is on: it holds the tables' sets and the prose's."""
        if getattr(settings, "prose_model", None):
            return "resolve", "tables"
        return cls.requires, cls.requires_flag

    def origins(self) -> Tuple[str, ...]:
        """The origins this run's sets can have: prose sets only when prose is on."""
        return ("table", "text") if getattr(self.settings, "prose_model", None) else ("table",)

    def model_state(self) -> Tuple[Optional[str], Optional[str]]:
        """`_model_state`, or, inside `remembering()`, the state it gave first."""
        if not self._remembering:
            return self._model_state()
        if self._state is None:
            self._state = self._model_state()
        return self._state

    @contextlib.contextmanager
    def remembering(self):
        """Read the model directory once for the plans made inside: upload's gate plans
        thousands of articles one at a time, and digests the model for each."""
        self._remembering, self._state = True, None
        try:
            yield self
        finally:
            self._remembering, self._state = False, None

    def _model_state(self) -> Tuple[Optional[str], Optional[str]]:
        """(The source its sets are recorded with, None) of the configured model; or (None, why).

        The directory must hold a model whose answers are study_schema's and whose
        contexts are the ones this code builds (`decision.state`).
        """
        configured = getattr(self.settings, "role_model", None)
        return decision.state(configured.path if configured else None)

    def model_fingerprint(self, origin: str, state: Tuple[Optional[str], Optional[str]]) -> str:
        """One origin's part of the fingerprint: its context version and the model, or why none."""
        source, reason = state
        return fingerprint("roles-model", origin, CONTEXT_VERSIONS[origin], source or reason)

    def classifier(self) -> DecisionModel:
        """The decision model, loaded once; `ModelUnavailable` with the reason it cannot be."""
        if self._model is None:
            with self._lock:
                if self._stopped:
                    raise ModelUnavailable(self._stopped)
                if self._model is None:
                    source, reason = self.model_state()
                    if source is None:
                        raise ModelUnavailable(reason)
                    configured = self.settings.role_model
                    try:
                        self._model = DecisionModel(configured.path, source=source,
                                                    device=configured.device,
                                                    batch_size=configured.batch_size)
                    except Exception as exc:  # noqa: BLE001 - no article can be read without it
                        raise ModelUnavailable(
                            f"the role model at {configured.path} cannot be loaded: "
                            f"{type(exc).__name__}: {exc}"
                        ) from exc
                    # Stored before the check, so a stop that did not see it is seen here.
                    if self._stopped:
                        self._model.stop(self._stopped)
        return self._model

    def models(self) -> Dict[str, str]:
        """Each origin's part of the fingerprint, by origin."""
        state = self.model_state()
        return {o: self.model_fingerprint(o, state) for o in self.origins()}

    def fingerprint_for(self, upstream: Artifact, models: Mapping[str, str],
                        read: Optional[Tuple] = None) -> str:
        # Where resolve added nothing from the prose, it names the analyses
        # artifact it passed through, and the article has table sets only: it
        # stays fresh when prose is switched on, and the prose model is not its input.
        basis = (upstream.summary or {}).get("basis") if upstream.stage == "resolve" else None
        if basis:
            models = {o: fp for o, fp in models.items() if o == "table"}
        parts = ("roles", ROLES_VERSION, sorted(models.items())) + ((read,) if read else ())
        return fingerprint(*parts, upstream=basis or upstream.fingerprint)

    @staticmethod
    def prose_read(prose: Optional[Artifact], passages: Optional[Artifact], resolved: Artifact):
        """The prose and the passages its sets' indices point into, or why they cannot be read.

        The sets are resolve's, copied from the prose it read (`prose_read`); a resolve made
        from other prose, or one that skipped the newer prose and so kept its old row, holds
        indices into a list that prose no longer indexes.

        Roles reads the passages artifact as it is now; the prose sets index the list prose
        was given. Passages run again before prose does (prose waits for its go-ahead), so
        until prose reads the new list the two disagree, and roles waits."""
        if prose is None or prose.status is not Status.OK:
            return None, "its prose is not OK: roles waits for prose"
        if (resolved.summary or {}).get("prose_read") != prose.fingerprint:
            return None, RESOLVE_READ_OTHER_PROSE
        if (passages is None or passages.status is not Status.OK
                or (prose.summary or {}).get("passages_read") != passages.fingerprint):
            return None, PROSE_READ_OTHER_PASSAGES
        spans = (passages.summary or {}).get("spans")
        return (prose.fingerprint, passages.fingerprint, spans), None

    @staticmethod
    def needs(parent: Artifact) -> Tuple[str, ...]:
        """The origins of the sets in `parent`, read from its summary; both when it cannot tell."""
        summary = parent.summary or {}
        origins = []
        if summary.get("tables", 1):
            origins.append("table")
        if parent.stage == "resolve" and summary.get("prose_analyses", 1):
            origins.append("text")
        return tuple(origins)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        state = self.model_state()
        models = {o: self.model_fingerprint(o, state) for o in self.origins()}
        missing = state[1]
        if missing and missing not in self._said:
            self._said.add(missing)
            logger.warning("roles: articles with sets are blocked: %s", missing)
        ids = [ref.id for ref in refs]
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        prose = ctx.catalog.artifacts(ids, "prose")
        passages = ctx.catalog.artifacts(ids, "passages")
        downloads = ctx.catalog.artifacts(ids, "download")
        extracted = ctx.catalog.artifacts(ids, "extract")
        for ref in refs:
            parent = upstream.get(ref.id, {}).get("")
            if parent is None or parent.status is not Status.OK:
                take_back_or_block(plan, ref, parent, artifacts.get(ref.id, {}).get(""))
                continue
            if "text" in self.needs(parent) and read_from_bad_page(
                    prose.get(ref.id, {}).get(""), downloads.get(ref.id, {}),
                    extracted.get(ref.id, {})):
                plan.block(ref, f"prose {READ_FROM_BAD_PAGE}")
                continue
            # Not a failure: nothing is attempted, so no retry is used up, and
            # the article is planned on the first run with the model configured.
            if missing and self.needs(parent):
                plan.wait(ref, missing)
                continue
            read = None
            if "text" in self.needs(parent):
                read, stale = self.prose_read(prose.get(ref.id, {}).get(""),
                                              passages.get(ref.id, {}).get(""), parent)
                if stale:
                    plan.block(ref, stale)
                    continue
            fp = self.fingerprint_for(parent, models, read)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.add_fresh(ref)
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.add_permanent(ref)
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=parent))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Each work's outcome as it is decided, `ARTICLES` of them classified at once.

        After a `ModelUnavailable` no article is started: the ones under way are
        finished and yielded, then it is raised, and the article that met it and
        those not started stay pending. On any exception that leaves the stage, an
        interrupt included, every classifier is stopped, so no set not yet sent is.
        """
        back, works = taking_back(self.name, works)
        yield from back
        ids = [work.article_id for work in works]
        triaged = ctx.catalog.artifacts(ids, "triage")
        extractions = ctx.catalog.artifacts(ids, "extract")
        passages = ctx.catalog.artifacts(ids, "passages")
        queue = collections.deque(works)
        window: Dict[Future, Work] = {}
        outage: Optional[ModelUnavailable] = None
        pool = ThreadPoolExecutor(max_workers=ARTICLES, thread_name_prefix="roles")
        try:
            while window or (queue and outage is None):
                while queue and outage is None and len(window) < ARTICLES:
                    work = queue.popleft()
                    window[self._start(ctx, pool, work, triaged, extractions, passages)] = work
                done, _ = wait(window, return_when=FIRST_COMPLETED)
                for future in done:
                    work = window.pop(future)
                    try:
                        outcome = alone(self.name, work, partial(self._outcome, work, future))
                    except ModelUnavailable as exc:
                        outage = outage or exc
                        continue
                    yield outcome
            if outage is not None:
                raise outage
        except BaseException as exc:
            # Not under the lock, which a classifier being loaded holds for its server's answer.
            reason = f"the roles stage stopped: {type(exc).__name__}: {exc}"
            self._stopped = self._stopped or reason
            if self._model is not None:
                self._model.stop(self._stopped)
            raise
        finally:
            # Not waited for: the articles under way are left to the stopped model,
            # which runs nothing more. None is queued, as no more are started than it has threads.
            pool.shutdown(wait=False)

    def _start(self, ctx: Context, pool: ThreadPoolExecutor, work: Work, triaged, extractions,
               passages) -> Future:
        """The work's roles, being decided on `pool`.

        Its inputs are read here, on the stage's thread, which the catalog's
        connection belongs to; the pool builds the contexts and waits on the model.
        """
        future: Future = Future()
        try:
            payload = ctx.payload(work.upstream)
            if payload is None:
                future.set_result(Outcome.failure(work.article_id, self.name, "",
                                                  f"the {self.requires} payload is gone",
                                                  fingerprint=work.fingerprint))
                return future
            read = passages.get(work.article_id, {}).get("")
            read_payload = (
                ctx.payload(read) if read is not None and read.status is Status.OK else None
            )
            triage = triaged.get(work.article_id, {}).get("")
            inputs = (
                payload,
                _read_passages(ctx, read_payload),
                _article_text(triage, extractions.get(work.article_id, {}), ctx),
                _Model(self),
                raw_tables(triage, extractions.get(work.article_id, {}), ctx),
            )
        except Exception as exc:  # noqa: BLE001 - the article's failure, yielded in its place
            future.set_exception(exc)
            return future
        return pool.submit(_assign, inputs)

    def _outcome(self, work: Work, future: Future) -> Outcome:
        """The work's outcome once its roles are decided; `ModelUnavailable` is raised.

        Not the article's failure: the scheduler stops the stage and this article
        and the rest stay pending.
        """
        try:
            done = future.result()
        except ModelUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - the article's failure, not the stage's
            logger.warning("roles failed for %s: %s", work.article_id, exc)
            return Outcome.failure(
                work.article_id,
                self.name,
                "",
                f"{type(exc).__name__}: {exc}",
                fingerprint=work.fingerprint,
            )
        if isinstance(done, Outcome):
            return done
        assigned, summary = done
        # What the roles were decided on, for upload's freshness (`space.upload_basis`).
        parent = work.upstream
        basis = (parent.summary or {}).get("basis") if parent.stage == "resolve" else None
        summary["input"] = fingerprint("roles-input", ROLES_VERSION,
                                       upstream=basis or parent.fingerprint)
        return Outcome(
            article_id=work.article_id,
            stage=self.name,
            source="",
            status=Status.OK,
            fingerprint=work.fingerprint,
            payload=assigned,
            summary=summary,
        )


def _assign(inputs) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """`assign_roles` on a work's inputs; tables read without their text go in the summary."""
    unread: Dict[str, str] = {}
    assigned, summary = assign_roles(*inputs, unread)
    if unread:
        # Its sets were read without the table's header and rows: say which, and why.
        summary["tables_without_text"] = unread
    return assigned, summary


class _Model:
    """The stage's model, loaded when a set first needs it: an article whose sets the
    geometry check refused, every one, needs none."""

    def __init__(self, stage: RolesStage) -> None:
        self._stage = stage

    def predict(self, texts):
        return self._stage.classifier().predict(texts)

    @property
    def source(self) -> str:
        return self._stage.classifier().source
