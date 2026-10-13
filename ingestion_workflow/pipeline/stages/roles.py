"""Decide what each coordinate set is for: this study's result, an anchor, a quoted peak.

Required: it runs between `resolve` (or `analyses`, without prose) and
`space`, and space, upload and sync read nothing it has not passed. Every set's
role comes from its origin's role model (`services.set_roles`), a NuExtract3
fine-tune served by vLLM (`role_model_table`, `role_model_prose`): table sets
from the table model, prose sets from the prose model, each reading its own
context builder's input. There is no proposal and no default role: an article
with a set whose origin's model is not configured, or cannot be used, is not
attempted -- the plan counts it as blocked, with the reason -- so it uses up no
retries, and it runs as soon as the model is configured. Everything downstream
of it stays blocked meanwhile.
The payload it writes is described in docs/set-roles-artifact.md.
"""

from __future__ import annotations

import collections
import logging
import re
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.services.set_roles import Unanswered, decide, prose_context, table_context
from ingestion_workflow.services.set_roles.labels import is_decided
from ingestion_workflow.services.set_roles.served import (
    RoleModelUnavailable,
    ServedClassifier,
    model_identity,
    served_models,
)
from ingestion_workflow.services.set_roles.served_prompt import CONTEXT_VERSIONS, PROMPT

from ..plan import StagePlan, Work
from ..stage import Context, take_back_or_block, taking_back
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .passages import passage_from, read_text
from .space import _article_text

logger = logging.getLogger(__name__)

#: Bump when what the stage writes changes, apart from the models and the contexts.
ROLES_VERSION = 4

#: Why roles waits on an article whose prose indexes a passages list other than the current one.
RESOLVE_READ_OTHER_PROSE = ("its resolve was made from other prose: "
                            "roles waits for resolve to read this prose")
PROSE_READ_OTHER_PASSAGES = "its prose read other passages: roles waits for prose to read these"

#: The setting naming each origin's model, and the origin's name in a reason.
MODEL_SETTINGS = {"table": "role_model_table", "text": "role_model_prose"}
_ORIGIN_NAMES = {"table": "table", "text": "prose"}


class MissingRoleModel(RuntimeError):
    """A set's origin has no usable classifier: the article gets no roles at all."""


class UnansweredSets(RuntimeError):
    """Sets the classifier gave no usable answer for: the article gets no roles at all."""

    def __init__(self, reasons: Mapping[str, str]) -> None:
        self.reasons = dict(reasons)
        shown = "; ".join(f"{k}: {v}" for k, v in list(self.reasons.items())[:5])
        more = len(self.reasons) - 5
        super().__init__(
            f"{len(self.reasons)} set(s) without a usable answer: {shown}"
            + (f" and {more} more" if more > 0 else "")
        )


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
    classifiers: Mapping[str, Any],
    raw_tables: Optional[Mapping[str, Path]] = None,
    unread: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The payload with every analysis's role decided by its origin's classifier, and a summary.

    `classifiers` maps an origin (`table`, `text`) to its classifier (a
    `ServedClassifier`). A set whose origin has none raises `MissingRoleModel`,
    and sets the classifier could not answer raise `UnansweredSets` with each
    one's reason: no set is given a role any other way. Each analysis gains
    `metadata.set_role` (`SET_ROLE_FIELDS`) and `metadata.held`. Tables read
    without their header and rows go in `unread` (`set_contexts`). Sets of a
    role that is not uploaded -- a reference, a bare slice position (other),
    a localization -- are held: they stay in
    the collection's `analyses`, in their place, so a set's position (pondie's
    `table_id#ordinal`) never moves, and upload leaves them out (`uploaded_sets`).
    """
    found = list(set_contexts(payload, passages, text, raw_tables, unread))
    by_origin: Dict[str, list] = collections.defaultdict(list)
    for item in found:
        by_origin[item[2]].append(item)
    chosen = {}
    for origin in sorted(by_origin):
        # The stage's mapping raises with the reason its model cannot be used.
        chosen[origin] = classifiers.get(origin)
        if chosen[origin] is None:
            raise MissingRoleModel(f"no {_ORIGIN_NAMES[origin]} role model")
    decisions, unanswered = {}, {}
    for origin, items in by_origin.items():
        classifier = chosen[origin]
        predictions = classifier.predict([module.serialize(ctx) for *_, module, ctx in items])
        if len(predictions) != len(items):
            raise ValueError(
                f"the {_ORIGIN_NAMES[origin]} role model answered {len(predictions)} of "
                f"{len(items)} sets"
            )
        for (table_id, index, _, module, context), prediction in zip(items, predictions):
            if isinstance(prediction, Unanswered):
                unanswered[f"{table_id}#{index}"] = prediction.reason
                continue
            decisions[(table_id, index)] = decide(
                prediction,
                source=classifier.source,
                origin=origin,
                evidence=module.prior_evidence(context),
            )
    if unanswered:
        raise UnansweredSets(unanswered)
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
                    "held": not decision.uploaded,
                },
            }
            # An anchor by its kind.
            roles[decision.anchor_kind or decision.role] += 1
            held += not decision.uploaded
            kept.append(analysis)
            values.append([table_id, index, decision.role, decision.anchor_kind,
                           decision.from_prior_study, not decision.uploaded])
            records.append([table_id, index, metadata, not decision.uploaded])
        out[table_id] = {**collection, "analyses": kept}
    summary = {
        "tables": sum(1 for c in out.values() if (c or {}).get("analyses")),
        "sets": len(found),
        "sets_by_origin": {o: len(items) for o, items in sorted(by_origin.items())},
        # Prose sets whose points the classifier could not find in their passage.
        "text_sets_without_local": sum(not ctx.local for *_, ctx in by_origin.get("text", [])),
        "roles": dict(roles),
        "held": held,
        "sources": {o: c.source for o, c in chosen.items()},
        # The decided roles alone, without confidences or the model that
        # decided them: what upload's freshness follows, so a retrained model
        # re-uploads only the articles where a set's role changed.
        "role_values": fingerprint("role-values", values),
        # Every set's whole role record, with its confidence, evidence and model:
        # what sync's freshness follows, since the parse writes all of them.
        "role_records": fingerprint("role-records", records),
    }
    return out, summary


def uploaded_sets(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """The payload without its held sets: what upload writes to neurostore."""
    return {
        table_id: {
            **(collection or {}),
            "analyses": [
                a for a in (collection or {}).get("analyses") or []
                if not ((a or {}).get("metadata") or {}).get("held")
            ],
        }
        for table_id, collection in (payload or {}).items()
    }


def unassigned(payload: Mapping[str, Any]) -> List[str]:
    """`table_id#index` of every set, uploaded or held, without a role the roles stage decided.

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
        self._classifiers: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._said: Set[str] = set()

    @classmethod
    def upstream_for(cls, settings):
        """Read `resolve` when prose is on: it holds the tables' sets and the prose's."""
        if getattr(settings, "prose_model", None):
            return "resolve", "tables"
        return cls.requires, cls.requires_flag

    def origins(self) -> Tuple[str, ...]:
        """The origins this run's sets can have: prose sets only when prose is on."""
        return ("table", "text") if getattr(self.settings, "prose_model", None) else ("table",)

    def model_state(self, origin: str) -> Tuple[Optional[str], Optional[str]]:
        """(The source its sets are recorded with, None) of the origin's model; or (None, why).

        The model must be listed by its server now, and the prompt it was trained
        with must be for the contexts this code builds.
        """
        setting = MODEL_SETTINGS[origin]
        served = getattr(self.settings, setting, None)
        name = _ORIGIN_NAMES[origin]
        if served is None:
            return None, f"no {name} role model is configured ({setting})"
        loaded = self._classifiers.get(origin)
        if loaded is not None and loaded.stopped:
            return None, f"the {name} role model stopped this run: {loaded.stopped}"
        where = f"the {name} role model {served.model} at {served.endpoint}"
        try:
            PROMPT.check(origin)
        except ValueError as exc:
            return None, f"{where} cannot be used: {exc}"
        try:
            listed = served_models(served.endpoint)
        except Exception as exc:  # noqa: BLE001 - recorded as the articles' block
            return None, f"{where} cannot be reached: {type(exc).__name__}: {exc}"
        card = next((c for c in listed if c.get("id") == served.model), None)
        if card is None:
            names = [c.get("id") for c in listed]
            return None, f"{where} is not served there (it serves {names})"
        # The checkpoint, not only its served name: a retrained model is a new source.
        return f"{served.model}@{PROMPT.version}+{model_identity(card)[:12]}", None

    def model_fingerprint(self, origin: str, state: Tuple[Optional[str], Optional[str]]) -> str:
        """One origin's part of the fingerprint: its context version and model, or its absence."""
        source, reason = state
        return fingerprint("roles-model", origin, CONTEXT_VERSIONS[origin], source or reason)

    def classifier(self, origin: str) -> ServedClassifier:
        """The origin's classifier; `MissingRoleModel` with the reason when it has none."""
        if origin not in self._classifiers:
            with self._lock:
                if origin not in self._classifiers:
                    source, reason = self.model_state(origin)
                    if source is None:
                        raise MissingRoleModel(reason)
                    served = getattr(self.settings, MODEL_SETTINGS[origin])
                    self._classifiers[origin] = ServedClassifier(
                        served.endpoint, served.model, source=source
                    )
        return self._classifiers[origin]

    def models(self) -> Dict[str, str]:
        """Each origin's part of the fingerprint, by origin."""
        return {o: self.model_fingerprint(o, self.model_state(o)) for o in self.origins()}

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
        states = {o: self.model_state(o) for o in self.origins()}
        models = {o: self.model_fingerprint(o, state) for o, state in states.items()}
        missing = {o: reason for o, (source, reason) in states.items() if source is None}
        for reason in missing.values():
            if reason not in self._said:
                self._said.add(reason)
                logger.warning("roles: articles with such sets are blocked: %s", reason)
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
                plan.block(f"prose {READ_FROM_BAD_PAGE}")
                continue
            # Not a failure: nothing is attempted, so no retry is used up, and
            # the article is planned on the first run with the model configured.
            lacking = [missing[o] for o in self.needs(parent) if o in missing]
            if lacking:
                plan.block(lacking[0])
                continue
            read = None
            if "text" in self.needs(parent):
                read, stale = self.prose_read(prose.get(ref.id, {}).get(""),
                                              passages.get(ref.id, {}).get(""), parent)
                if stale:
                    plan.block(stale)
                    continue
            fp = self.fingerprint_for(parent, models, read)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=parent))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        back, works = taking_back(self.name, works)
        yield from back
        ids = [work.article_id for work in works]
        triaged = ctx.catalog.artifacts(ids, "triage")
        extractions = ctx.catalog.artifacts(ids, "extract")
        passages = ctx.catalog.artifacts(ids, "passages")
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"the {self.requires} payload is gone",
                    fingerprint=work.fingerprint,
                )
                continue
            try:
                read = passages.get(work.article_id, {}).get("")
                read_payload = (
                    ctx.payload(read) if read is not None and read.status is Status.OK else None
                )
                triage = triaged.get(work.article_id, {}).get("")
                text = _article_text(triage, extractions.get(work.article_id, {}), ctx)
                unread: Dict[str, str] = {}
                assigned, summary = assign_roles(
                    payload,
                    _read_passages(ctx, read_payload),
                    text,
                    _Classifiers(self),
                    raw_tables(triage, extractions.get(work.article_id, {}), ctx),
                    unread,
                )
                if unread:
                    # Its sets were read without the table's header and rows: say which, and why.
                    summary["tables_without_text"] = unread
            except RoleModelUnavailable as exc:
                # An outage or a misconfigured server is not the article's failure:
                # recording one would spend every remaining article's retries. This
                # article and the rest stay pending, and the plan blocks them this run.
                logger.error("roles stopped: %s; %d article(s) left pending", exc,
                             len(works) - works.index(work))
                return
            except MissingRoleModel as exc:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"no role for its sets: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            except UnansweredSets as exc:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"no role for its sets: {exc}",
                    fingerprint=work.fingerprint,
                    summary={"unanswered": exc.reasons},
                )
                continue
            except Exception as exc:
                logger.warning("roles failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            # What the roles were decided on, for upload's freshness (`space.upload_basis`).
            parent = work.upstream
            basis = (parent.summary or {}).get("basis") if parent.stage == "resolve" else None
            summary["input"] = fingerprint("roles-input", ROLES_VERSION,
                                           upstream=basis or parent.fingerprint)
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload=assigned,
                summary=summary,
            )


class _Classifiers(Mapping):
    """The stage's classifiers by origin, each loaded when a set of its origin first needs it."""

    def __init__(self, stage: RolesStage) -> None:
        self._stage = stage

    def __getitem__(self, origin: str):
        return self._stage.classifier(origin)

    def __iter__(self):
        return iter(MODEL_SETTINGS)

    def __len__(self) -> int:
        return len(MODEL_SETTINGS)
