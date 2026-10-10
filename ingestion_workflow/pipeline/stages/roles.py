"""Decide what each coordinate set is for: this study's result, an anchor, a quoted peak.

Required: it runs between `resolve` (or `analyses`, without prose) and
`space`, and space, upload and sync read nothing it has not passed. Every set's
role comes from a fine-tuned classifier of its origin (`services.set_roles`):
table sets from the table model (`role_model_table`), prose sets from the
prose model (`role_model_prose`), each reading its own context builder's input.
There is no proposal and no default role: an article with a set whose origin's
model is not configured, or cannot be used, fails here with the reason, and
everything downstream of it stays blocked. The payload it writes is described
in docs/set-roles-artifact.md.
"""

from __future__ import annotations

import collections
import logging
import re
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.services.set_roles import decide, prose_context, table_context
from ingestion_workflow.services.set_roles.labels import role_error
from ingestion_workflow.services.set_roles.model import CONTEXT_VERSIONS, check_meta, read_meta

from ..plan import StagePlan, Work
from ..stage import Context
from .space import _article_text

logger = logging.getLogger(__name__)

#: Bump when what the stage writes changes, apart from the models and the contexts.
ROLES_VERSION = 2

#: The setting naming each origin's model, and the origin's name in a reason.
MODEL_SETTINGS = {"table": "role_model_table", "text": "role_model_prose"}
_ORIGIN_NAMES = {"table": "table", "text": "prose"}

#: The fields every set carries under `metadata.set_role`, once the stage has decided it.
SET_ROLE_FIELDS = (
    "role",
    "anchor_kind",
    "from_prior_study",
    "prior_study_evidence",
    "role_confidence",
    "role_source",
    "role_origin",
)


class MissingRoleModel(RuntimeError):
    """A set's origin has no usable classifier: the article gets no roles at all."""


def _span(sentence: str, text: Optional[str]) -> dict:
    """A TextSpan for a sentence at its first occurrence in the article, else the text alone."""
    if text:
        found = re.search(r"\s+".join(map(re.escape, sentence.split())), text)
        if found:
            return {"start_char": found.start(), "end_char": found.end(), "text": found.group(0)}
    return {"text": sentence}


def _table_text(analysis: Mapping[str, Any]) -> Optional[str]:
    """The table's serialisation, as nu-v21 read it; None when the raw table is gone."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService  # noqa: PLC0415

    meta = (analysis.get("metadata") or {}).get("table_metadata") or {}
    path = meta.get("raw_content_path") or meta.get("raw_xml_path")
    if not path or not Path(path).is_file():
        return None
    try:
        return CreateAnalysesService._serialise(
            Path(path).read_text(encoding="utf-8", errors="replace"), meta.get("table_id") or ""
        )
    except Exception as exc:  # noqa: BLE001 - the set is read without its rows
        logger.debug("cannot serialise %s: %s", path, exc)
        return None


def set_contexts(
    payload: Mapping[str, Any], passages: Sequence[Mapping[str, Any]], text: Optional[str]
):
    """(table id, analysis index, origin, builder module, context) for every analysis."""
    for table_id, collection in (payload or {}).items():
        analyses = (collection or {}).get("analyses", [])
        table_text = None
        for index, analysis in enumerate(analyses):
            if (analysis.get("metadata") or {}).get("source") == "prose":
                context = prose_context.build(analysis, passages)
                yield table_id, index, "text", prose_context, context
                continue
            if table_text is None:
                table_text = _table_text(analysis) or ""
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
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The payload with every analysis's role decided by its origin's classifier, and a summary.

    `classifiers` maps an origin (`table`, `text`) to its classifier. A set
    whose origin has none raises `MissingRoleModel`: no set is given a role
    any other way. Each analysis gains `metadata.set_role` (`SET_ROLE_FIELDS`).
    Sets of a role that is not uploaded -- a reference, a bare slice position
    (other), a localization, numbers that are not coordinates -- move from the
    collection's `analyses` to its `held`, which upload and space do not read,
    so they stay with the article without becoming this study's analyses.
    """
    found = list(set_contexts(payload, passages, text))
    by_origin: Dict[str, list] = collections.defaultdict(list)
    for item in found:
        by_origin[item[2]].append(item)
    chosen = {}
    for origin in sorted(by_origin):
        # The stage's mapping raises with the reason its model cannot be used.
        chosen[origin] = classifiers.get(origin)
        if chosen[origin] is None:
            raise MissingRoleModel(f"no {_ORIGIN_NAMES[origin]} role model")
    decisions = {}
    for origin, items in by_origin.items():
        classifier = chosen[origin]
        predictions = classifier.predict([module.serialize(ctx) for *_, module, ctx in items])
        if len(predictions) != len(items):
            raise ValueError(
                f"the {_ORIGIN_NAMES[origin]} role model answered {len(predictions)} of "
                f"{len(items)} sets"
            )
        for (table_id, index, _, module, context), prediction in zip(items, predictions):
            decisions[(table_id, index)] = decide(
                prediction,
                source=classifier.source,
                origin=origin,
                evidence=module.prior_evidence(context),
            )
    out: Dict[str, Any] = {}
    roles, held = collections.Counter(), 0
    for table_id, collection in (payload or {}).items():
        kept, set_aside = [], list((collection or {}).get("held", []))
        for index, analysis in enumerate((collection or {}).get("analyses", [])):
            decision = decisions[(table_id, index)]
            metadata = decision.to_metadata()
            metadata["prior_study_evidence"] = [
                _span(e["text"], text) for e in metadata["prior_study_evidence"]
            ]
            analysis = {
                **analysis,
                "metadata": {**(analysis.get("metadata") or {}), "set_role": metadata},
            }
            # An anchor by its kind; numbers set aside as not coordinates as such.
            roles[decision.anchor_kind or decision.role or "not_coordinates"] += 1
            if decision.uploaded:
                kept.append(analysis)
            else:
                held += 1
                set_aside.append(analysis)
        out[table_id] = {
            **collection,
            "analyses": kept,
            **({"held": set_aside} if set_aside else {}),
        }
    summary = {
        "tables": sum(1 for c in out.values() if (c or {}).get("analyses")),
        "sets": len(found),
        "sets_by_origin": {o: len(items) for o, items in sorted(by_origin.items())},
        "roles": dict(roles),
        "held": held,
        "sources": {o: c.source for o, c in chosen.items()},
    }
    return out, summary


def unassigned(payload: Mapping[str, Any]) -> List[str]:
    """`table_id#index` of every set, uploaded or held, without a role the roles stage decided.

    What upload and sync check before writing anything: a set missing its
    `metadata.set_role`, or carrying one that is incomplete or not
    study_schema's, is never written with a guessed role.
    """
    out = []
    for table_id, collection in (payload or {}).items():
        sets = [
            *((collection or {}).get("analyses") or []),
            *((collection or {}).get("held") or []),
        ]
        for index, analysis in enumerate(sets):
            role = ((analysis or {}).get("metadata") or {}).get("set_role")
            if (
                not isinstance(role, Mapping)
                or any(k not in role for k in SET_ROLE_FIELDS)
                or not role.get("role_source")
                or role_error(role)
            ):
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
        """(`name@version`, None) of the origin's model, read without loading it; or (None, why).

        A model for another origin, or trained on other context versions, is
        refused: it would read inputs shaped differently from the ones it learned on.
        """
        setting = MODEL_SETTINGS[origin]
        path = getattr(self.settings, setting, None)
        name = _ORIGIN_NAMES[origin]
        if not path:
            return None, f"no {name} role model is configured ({setting})"
        try:
            meta = read_meta(Path(path))
            check_meta(meta, origin, path)
        except Exception as exc:  # noqa: BLE001 - recorded as the articles' failure
            return None, f"the {name} role model at {path} cannot be used: {exc}"
        return f"{meta['name']}@{meta['version']}", None

    def model_fingerprint(self, origin: str, state: Tuple[Optional[str], Optional[str]]) -> str:
        """One origin's part of the fingerprint: its context version and model, or its absence."""
        source, reason = state
        return fingerprint("roles-model", origin, CONTEXT_VERSIONS[origin], source or reason)

    def classifier(self, origin: str):
        """The origin's loaded classifier; `MissingRoleModel` with the reason when it has none."""
        if origin not in self._classifiers:
            with self._lock:
                if origin not in self._classifiers:
                    source, reason = self.model_state(origin)
                    if source is None:
                        raise MissingRoleModel(reason)
                    from ingestion_workflow.services.set_roles import (
                        EncoderClassifier,  # noqa: PLC0415 - torch
                    )

                    self._classifiers[origin] = EncoderClassifier(
                        Path(getattr(self.settings, MODEL_SETTINGS[origin])),
                        origin,
                        device=self.settings.role_device,
                    )
        return self._classifiers[origin]

    def models(self) -> Dict[str, str]:
        """Each origin's part of the fingerprint, by origin."""
        return {o: self.model_fingerprint(o, self.model_state(o)) for o in self.origins()}

    def fingerprint_for(self, upstream: Artifact, models: Mapping[str, str]) -> str:
        # Where resolve added nothing from the prose, it names the analyses
        # artifact it passed through, and the article has table sets only: it
        # stays fresh when prose is switched on, and the prose model is not its input.
        basis = (upstream.summary or {}).get("basis") if upstream.stage == "resolve" else None
        if basis:
            models = {o: fp for o, fp in models.items() if o == "table"}
        return fingerprint(
            "roles", ROLES_VERSION, sorted(models.items()), upstream=basis or upstream.fingerprint
        )

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        models = self.models()
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            parent = upstream.get(ref.id, {}).get("")
            if parent is None or parent.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(parent, models)
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
                text = _article_text(
                    triaged.get(work.article_id, {}).get(""),
                    extractions.get(work.article_id, {}),
                    ctx,
                    read,
                )
                assigned, summary = assign_roles(
                    payload,
                    (read_payload or {}).get("passages", []),
                    text,
                    _Classifiers(self),
                )
            except MissingRoleModel as exc:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"no role for its sets: {exc}",
                    fingerprint=work.fingerprint,
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
