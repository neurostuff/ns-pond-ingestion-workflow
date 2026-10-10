"""Decide what each coordinate set is for: this study's result, an anchor, a quoted peak.

A table set is proposed `result` and a prose set its prose model's role; a
fine-tuned classifier reads each set with its context (`services.set_roles`)
and overrides the proposal only where it is confident. Runs only when
`role_model` is set, between `resolve` (or `analyses`, without prose) and
`space`. Table and prose sets are read by their own context builders.
"""

from __future__ import annotations

import collections
import logging
import re
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.services.set_roles import decide, prose_context, table_context
from ingestion_workflow.services.set_roles.model import CONTEXT_VERSIONS, check_meta, read_meta

from ..plan import StagePlan, Work
from ..stage import Context
from .space import _article_text

logger = logging.getLogger(__name__)

#: Bump when what the stage writes changes, apart from the model and the context.
ROLES_VERSION = 1


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
    """(table id, analysis index, builder module, context) for every analysis of a payload."""
    for table_id, collection in (payload or {}).items():
        analyses = (collection or {}).get("analyses", [])
        table_text = None
        for index, analysis in enumerate(analyses):
            if (analysis.get("metadata") or {}).get("source") == "prose":
                yield table_id, index, prose_context, prose_context.build(analysis, passages)
                continue
            if table_text is None:
                table_text = _table_text(analysis) or ""
            yield (
                table_id,
                index,
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
    classifier,
    *,
    min_confidence: float,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The payload with every analysis's role decided, and a summary.

    Each analysis gains `metadata.set_role` (CoordinateParse's role fields plus
    the proposal). Sets of a role that is not uploaded -- a reference, a
    bare slice position (other), a localization, numbers that are not coordinates -- move
    from the collection's `analyses` to its `held`, which upload and space do
    not read, so they stay with the article without becoming this study's
    analyses.
    """
    found = list(set_contexts(payload, passages, text))
    texts = [module.serialize(context) for _, _, module, context in found]
    predictions = classifier.predict(texts) if found else []
    decisions = {}
    for (table_id, index, module, context), prediction in zip(found, predictions):
        decisions[(table_id, index)] = decide(
            context.proposed,
            prediction,
            source=classifier.source,
            min_confidence=min_confidence,
            evidence=module.prior_evidence(context),
        )
    out: Dict[str, Any] = {}
    roles, overridden, held = collections.Counter(), 0, 0
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
            overridden += (decision.role, decision.anchor_kind) != (
                decision.proposed.role,
                decision.proposed.anchor_kind,
            )
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
        "roles": dict(roles),
        "overridden": overridden,
        "held": held,
        "source": classifier.source,
    }
    return out, summary


class RolesStage:
    name = "roles"
    requires = "analyses"
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings
        self.requires, self.requires_flag = self.upstream_for(settings)
        self._classifier = None
        self._lock = threading.Lock()

    @classmethod
    def upstream_for(cls, settings):
        """Read `resolve` when prose is on: it holds the tables' sets and the prose's."""
        if getattr(settings, "prose_model", None):
            return "resolve", "tables"
        return cls.requires, cls.requires_flag

    def classifier(self):
        if self._classifier is None:
            with self._lock:
                if self._classifier is None:
                    from ingestion_workflow.services.set_roles import (
                        EncoderClassifier,  # noqa: PLC0415 - torch
                    )

                    self._classifier = EncoderClassifier(
                        Path(self.settings.role_model), device=self.settings.role_device
                    )
        return self._classifier

    def model_source(self) -> str:
        """`name@version` of the configured model, read from its metadata without loading it.

        Refuses a model trained on other context versions: it would read
        inputs shaped differently from the ones it learned on.
        """
        meta = read_meta(Path(self.settings.role_model))
        check_meta(meta, self.settings.role_model)
        return f"{meta['name']}@{meta['version']}"

    def fingerprint_for(self, upstream: Artifact, source: str) -> str:
        return fingerprint(
            "roles",
            ROLES_VERSION,
            sorted(CONTEXT_VERSIONS.items()),
            source,
            self.settings.role_min_confidence,
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
        source = self.model_source()
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            parent = upstream.get(ref.id, {}).get("")
            if parent is None or parent.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(parent, source)
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
                    self.classifier(),
                    min_confidence=self.settings.role_min_confidence,
                )
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
