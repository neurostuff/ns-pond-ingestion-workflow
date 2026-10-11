"""Upload service orchestrating metadata and coordinate persistence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from sqlalchemy import JSON, Boolean, Text, column, delete, inspect, null, select, table, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import selectinload
from study_schema import keys

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleMetadata,
    BaseStudyPayload,
    PreparedAnalysis,
    StudyPayload,
    TablePayload,
    UploadOutcome,
    UploadWorkItem,
)
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.coordinate_flags import declared_split, is_placeholder
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.logging import console_kwargs, get_logger
from ingestion_workflow.services.study_level import level_for
from ingestion_workflow.services.upload_models import Analysis as DbAnalysis
from ingestion_workflow.services.upload_models import Annotation as DbAnnotation
from ingestion_workflow.services.upload_models import (
    AnnotationAnalysis as DbAnnotationAnalysis,
)
from ingestion_workflow.services.upload_models import BaseStudy as DbBaseStudy
from ingestion_workflow.services.upload_models import Point as DbPoint
from ingestion_workflow.services.upload_models import PointValue as DbPointValue
from ingestion_workflow.services.upload_models import Study as DbStudy
from ingestion_workflow.services.upload_models import StudysetStudy as DbStudysetStudy
from ingestion_workflow.services.upload_models import Table as DbTable
from ingestion_workflow.utils.console import progress_bar as create_progress_bar

logger = get_logger(__name__)


@dataclass
class ReconcilePlan:
    """What upload intends to do with one study's existing analyses.

    An existing analysis no incoming one is matched to is deleted, unless someone
    annotated it: then it stays in the study. `decisions` records which, and why.
    """

    updated: List[Tuple[object, int]]     # (existing row, index into incoming)
    inserted: List[int]                   # indices into incoming
    deleted: List[object]                 # unmatched and unannotated: removed
    left: List[object]                    # unmatched but annotated: kept in place
    #: One per incoming analysis and per existing row: `decision`, `reason`, and the
    #: keys and names it was decided on. Incoming entries carry `index`, existing ones
    #: `analysis_id`; `upload` fills in the id of each inserted row.
    decisions: List[Dict[str, object]]


def _normalize_analysis_name(name: Optional[str]) -> str:
    """Strip the -2, -3 suffixes extraction adds to repeated names.

    Each pass must consume a hyphen, or a name that is entirely digits ("42")
    would rsplit to itself and loop forever.
    """
    text = (name or "").strip().lower()
    while "-" in text:
        head, tail = text.rsplit("-", 1)
        if not tail.strip().isdigit():
            break
        text = head.strip()
    return " ".join(text.split())


def _point_key(x, y, z) -> Optional[Tuple[float, float, float]]:
    try:
        return (float(x), float(y), float(z))
    except (TypeError, ValueError):
        return None


def _point_set(points: Iterable) -> frozenset:
    """The (x, y, z) an analysis reports, unrounded: legacy matching asks for identical points."""
    found = set()
    for point in points or []:
        key = _point_key(
            getattr(point, "x", None), getattr(point, "y", None), getattr(point, "z", None)
        )
        if key is not None:
            found.add(key)
    return frozenset(found)


def _base_name(name: Optional[str]) -> str:
    """study_schema's normalized name, without the suffix upload gives a repeated name."""
    return keys.normalize_name(_normalize_analysis_name(name))


class _Place(NamedTuple):
    """Where a parse read an analysis, in the terms neurostore compares superseded analyses by."""

    place: Tuple[str, str]       # (origin, table_id)
    slot: Tuple[str, str, str]   # (origin, table_id, cell or span locator)
    parts: frozenset             # cells, or character offsets covered by a span
    name: str                    # study_schema's normalized name


def _place_of(metadata, name) -> Optional[_Place]:
    """Where the parse read an analysis, from its `origin`, `table_id`, `cells`, `text_spans`."""
    metadata = metadata or {}
    origin = metadata.get("origin")
    if not origin:
        return None
    table_id = metadata.get("table_id") or ""
    if origin == "table":
        cells = [(c["row"], c["column_group"]) for c in metadata.get("cells") or []]
        locator, parts = keys.cell_locator(cells), frozenset(cells)
    else:
        spans = [(s["start_char"], s["end_char"]) for s in metadata.get("text_spans") or []]
        locator = keys.span_locator(spans)
        parts = frozenset(o for a, b in spans for o in range(a, b))
    return _Place(
        (origin, table_id), (origin, table_id, locator), parts, keys.normalize_name(name or "")
    )


def _unique_pairs(olds: Sequence[int], news: Sequence[int], match) -> List[Tuple[int, int]]:
    """Pairs where each is the other's only match: neurostore's rule for a re-read analysis."""
    forward = {o: [n for n in news if match(o, n)] for o in olds}
    backward = {n: [o for o in olds if n in forward[o]] for n in news}
    return [
        (o, found[0])
        for o, found in forward.items()
        if len(found) == 1 and len(backward[found[0]]) == 1
    ]


class UploadSourceNotSet(ValueError):
    """`upload_source` was not configured and an upload was attempted.

    It is the key neurostore versions a study by, so it decides which version
    `update` reconciles against -- and reconciling deletes the analyses it
    replaces. With the old default of "llm", a run that forgot to set it
    resolved to another extractor's version and deleted its work. Refusing is
    the only safe reading of silence: there is no source that is right to
    guess.
    """


def resolve_upload_source(settings) -> str:
    source = getattr(settings, "upload_source", None)
    if source is None or not str(source).strip():
        raise UploadSourceNotSet(
            "upload_source is not set. It names the extractor on the study "
            "version, and `upload` reconciles against the version carrying it "
            "-- deleting the analyses it replaces. Set it in the config "
            "(for example `upload_source: nuextract-v19`)."
        )
    return str(source).strip()


def plan_reconciliation(
    existing: Sequence,
    incoming_names: Sequence[Optional[str]],
    incoming_coords: Sequence[set],
    annotated_ids: set,
    incoming_places: Optional[Sequence[Optional[Mapping]]] = None,
    incoming_bases: Optional[Sequence[Optional[str]]] = None,
) -> ReconcilePlan:
    """Decide, for one study, which existing analysis each incoming one is.

    An analysis is identified by its parse key (`Analysis.source_id`), which covers
    where it was read and its normalized name, so two contrasts read from the same
    rows are never one analysis. In order:

    1. The same key: the same analysis.
    2. An existing analysis whose key is gone and an incoming one read under the same
       normalized name from other, overlapping cells or spans of the same table or
       text, each the other's only such match: the same analysis re-read. Its key
       moves. neurostore aliases the entity of such a pair the same way.
    3. A row with no key (uploaded before keys were), or an incoming analysis the
       parse holds no key for: matched only on the same name and identical points.
       A stored name may carry the suffix upload gives a repeated name
       (`incoming_bases` are the names before it).
    4. Anything else is new. An existing analysis left unmatched is deleted when no
       one annotated it, since a new reading of the paper replaced it and keeping it
       would count the contrast twice; an annotated one stays where it is. One read
       from exactly the place of a gone one under another name is recorded as a
       rename candidate, as neurostore records one; whether it is the same analysis
       is a curator's call, so nothing of the old one moves to it.

    A matched row is updated in place, so it keeps its id and what is annotated
    against it.
    """
    n_incoming = len(incoming_names)
    incoming_bases = list(incoming_bases or incoming_names)
    places_in = list(incoming_places or [None] * n_incoming)
    incoming_keys = [(place or {}).get("key") for place in places_in]
    incoming_where = [
        _place_of(place, incoming_names[i]) if place else None for i, place in enumerate(places_in)
    ]
    existing_keys = [getattr(row, "source_id", None) for row in existing]
    existing_where = [
        _place_of(getattr(row, "metadata_", None), getattr(row, "name", None)) for row in existing
    ]
    incoming_points = [frozenset(c) for c in incoming_coords]
    existing_points = [_point_set(getattr(row, "points", [])) for row in existing]

    pairs: Dict[int, Tuple[int, str, str]] = {}   # existing -> (incoming, decision, reason)
    taken: set = set()

    def pair(e_index, i_index, decision, reason):
        pairs[e_index] = (i_index, decision, reason)
        taken.add(i_index)

    def free_existing():
        return [e for e in range(len(existing)) if e not in pairs]

    def free_incoming():
        return [i for i in range(n_incoming) if i not in taken]

    # 1. key. One key held twice is two reads the parse merges; they pair in order.
    by_key: Dict[str, List[int]] = {}
    for i_index in range(n_incoming):
        if incoming_keys[i_index]:
            by_key.setdefault(incoming_keys[i_index], []).append(i_index)
    for e_index in sorted(
        range(len(existing)),
        key=lambda e: (
            getattr(existing[e], "order", None) or 0, getattr(existing[e], "id", "") or ""
        ),
    ):
        queue = by_key.get(existing_keys[e_index] or "")
        if queue:
            pair(e_index, queue.pop(0), "matched_key", "the same parse key")

    # 2. the same analysis re-read from other cells or spans
    gone = [e for e in free_existing() if existing_keys[e] and existing_where[e]]
    added = [i for i in free_incoming() if incoming_keys[i] and incoming_where[i]]

    def same_analysis(e, i):
        a, b = existing_where[e], incoming_where[i]
        return a.place == b.place and a.name == b.name and bool(a.parts & b.parts)

    for e_index, i_index in _unique_pairs(gone, added, same_analysis):
        pair(
            e_index,
            i_index,
            "matched_cells_changed",
            f"read again under its name from other, overlapping "
            f"{'cells' if existing_where[e_index].place[0] == 'table' else 'spans'}; "
            f"its key was {existing_keys[e_index]}",
        )

    # 3. no key on one side: the same name and identical points
    def full_name(name):
        return keys.normalize_name(name or "")

    def same_name(e_index, i_index):
        stored = getattr(existing[e_index], "name", None)
        if full_name(stored) == full_name(incoming_names[i_index]):
            return True
        # The suffix upload gives a repeated name is positional: "X-2" is "X" once
        # the first "X" is gone.
        return _base_name(stored) == full_name(incoming_bases[i_index])

    legacy_first = sorted(free_existing(), key=lambda e: existing_keys[e] is not None)
    for e_index in legacy_first:
        fits = [
            i for i in free_incoming()
            # A keyed row is the same as a keyed incoming analysis only by key or re-read.
            if not (existing_keys[e_index] and incoming_keys[i])
            and existing_points[e_index] == incoming_points[i]
            and same_name(e_index, i)
        ]
        stored = full_name(getattr(existing[e_index], "name", None))
        fits.sort(key=lambda i: full_name(incoming_names[i]) != stored)
        if fits:
            pair(
                e_index,
                fits[0],
                "matched_exact",
                "no key on the "
                + ("existing row" if existing_keys[e_index] is None else "incoming analysis")
                + "; the same name and identical points",
            )

    # 4. rename candidates, recorded only
    gone = [e for e in free_existing() if existing_keys[e] and existing_where[e]]
    added = [i for i in free_incoming() if incoming_keys[i] and incoming_where[i]]
    renamed = dict(
        _unique_pairs(
            gone,
            added,
            lambda e, i: existing_where[e].slot == incoming_where[i].slot
            and existing_where[e].name != incoming_where[i].name,
        )
    )
    renamed_from = {i: e for e, i in renamed.items()}

    decisions: List[Dict[str, object]] = []
    updated = []
    for e_index in sorted(pairs, key=lambda e: pairs[e][0]):
        i_index, decision, reason = pairs[e_index]
        updated.append((existing[e_index], i_index))
        decisions.append(
            {
                "decision": decision,
                "index": i_index,
                "analysis_id": getattr(existing[e_index], "id", None),
                "name": incoming_names[i_index],
                "key": incoming_keys[i_index],
                **({"previous_key": existing_keys[e_index]}
                   if existing_keys[e_index] != incoming_keys[i_index] else {}),
                **({"previous_name": getattr(existing[e_index], "name", None)}
                   if getattr(existing[e_index], "name", None) != incoming_names[i_index] else {}),
                "reason": reason,
            }
        )
    inserted = free_incoming()
    for i_index in inserted:
        if i_index in renamed_from:
            old = renamed_from[i_index]
            reason = (
                f"read from exactly the place of {existing_keys[old]} "
                f"({getattr(existing[old], 'name', None)!r}) under another name: a rename "
                "candidate, kept apart until a curator confirms it"
            )
        elif incoming_keys[i_index]:
            reason = "no existing analysis has its key"
        else:
            why = (places_in[i_index] or {}).get("unplaced") or "upload was given no parse key"
            reason = f"no key ({why}); no existing analysis has its name and identical points"
        decisions.append(
            {
                "decision": "new",
                "index": i_index,
                "name": incoming_names[i_index],
                "key": incoming_keys[i_index],
                **({"rename_candidate_of": existing_keys[renamed_from[i_index]]}
                   if i_index in renamed_from else {}),
                "reason": reason,
            }
        )
    deleted, left = [], []
    for e_index in free_existing():
        row = existing[e_index]
        annotated = getattr(row, "id", None) in annotated_ids
        if existing_keys[e_index] is None:
            reason = "uploaded with no key; no incoming analysis has its name and identical points"
        else:
            reason = "its key is not among this upload's"
        if e_index in renamed:
            reason += f"; {incoming_keys[renamed[e_index]]} is a rename candidate for it"
        (left if annotated else deleted).append(row)
        decisions.append(
            {
                "decision": "unmatched_existing" if annotated else "deleted",
                "analysis_id": getattr(row, "id", None),
                "name": getattr(row, "name", None),
                "key": existing_keys[e_index],
                "points": len(existing_points[e_index]),
                **({"rename_candidate": incoming_keys[renamed[e_index]]}
                   if e_index in renamed else {}),
                "annotated": annotated,
                "reason": reason + ("; annotated, so left in place" if annotated
                                    else "; not annotated, so deleted"),
            }
        )
    return ReconcilePlan(
        updated=updated, inserted=inserted, deleted=deleted, left=left, decisions=decisions
    )


def _default_note_for(note_keys) -> dict:
    """Mirror of neurostore's build_default_note, for recognising an untouched note.

    A row created by neurostore's backfill carries the annotation's defaults,
    which means the analysis merely belongs to an annotated studyset. Only a
    note that differs from those defaults represents work someone did.
    """
    if not note_keys:
        return {}
    if not isinstance(note_keys, dict):
        return {key: None for key in note_keys}
    defaults = {}
    for key, descriptor in note_keys.items():
        if isinstance(descriptor, dict):
            if "default" in descriptor:
                defaults[key] = descriptor.get("default")
                continue
            note_type = descriptor.get("type")
        else:
            note_type = descriptor
        defaults[key] = (key == "included") if note_type == "boolean" else None
    return defaults


def _note_is_substantive(note, note_keys) -> bool:
    """True when a note holds something other than the annotation's defaults."""
    if not note:
        return False
    if not isinstance(note, dict):
        return True
    defaults = _default_note_for(note_keys)
    for key, value in note.items():
        if value in (None, "", [], {}):
            continue
        if key in defaults and value == defaults[key]:
            continue
        return True
    return False



#: The parse's fields that say where an analysis was read, kept in its metadata as
#: neurostore keeps them; the key itself is `source_id`.
_PLACE_METADATA = ("origin", "table_id", "cells", "text_spans")


def _point_row(coord, prepared: PreparedAnalysis, order: int):
    """A point's columns and its (kind, value) pairs, comparable with `_stored_points`."""
    fields = (
        ("x", coord.x),
        ("y", coord.y),
        ("z", coord.z),
        ("space", coord.space.value if coord.space else prepared.coordinate_space),
        ("cluster_size", coord.cluster_size),
        ("subpeak", coord.is_subpeak),
        # `deactivation` is left at its default. A negative set is
        # its own analysis, the inverse contrast, so direction is
        # the analysis's and no point is marked; NiMARE never read
        # the flag anyway.
        ("order", order),
    )
    values = ()
    if coord.statistic_type or coord.statistic_value is not None:
        try:
            value = float(coord.statistic_value) if coord.statistic_value is not None else None
        except (TypeError, ValueError):
            value = None
        values = ((coord.statistic_type, value),)
    return fields, values


def _stored_points(row) -> list:
    """`row`'s points as `_point_row`s, so a re-upload of the same points rewrites none."""
    return [
        (
            tuple((name, getattr(point, name)) for name in
                  ("x", "y", "z", "space", "cluster_size", "subpeak", "order")),
            tuple((v.kind, v.value) for v in point.values),
        )
        for point in sorted(row.points, key=lambda p: (p.order is None, p.order))
    ]


def _sanitize_text(value: str | None) -> str | None:
    """Strip NULL bytes from text fields to keep Postgres happy."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value  # type: ignore[return-value]
    if "\x00" in value:
        return value.replace("\x00", "")
    return value


def _sanitize_mapping(obj):
    """Recursively strip NULL bytes from all string values in mappings/lists."""
    if isinstance(obj, dict):
        return {k: _sanitize_mapping(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_mapping(v) for v in obj]
    if isinstance(obj, str):
        return _sanitize_text(obj)
    return obj


@dataclass
class RetractOutcome:
    """What retracting one article did to its study version."""

    slug: str
    base_study_id: str
    study_id: Optional[str] = None
    #: `deleted`, `emptied` (kept because a studyset or an annotation uses it),
    #: `held` (left untouched for review because a studyset holds it), or
    #: `absent` (this source never had a version there).
    action: str = ""
    removed: int = 0
    kept_annotated: int = 0
    studysets: List[str] = None  # type: ignore[assignment]
    success: bool = False
    error: Optional[str] = None


class UploadService:
    """High-level coordination of upload operations."""

    def __init__(self, settings: Settings, session_factory: SessionFactory) -> None:
        self.settings = settings
        self.session_factory = session_factory

    def prepare_work_items(
        self,
        analyses: Mapping[str, Mapping[str, AnalysisCollection]],
        metadata: Mapping[str, ArticleMetadata],
        *,
        metadata_mode: UploadMetadataMode,
        identifiers: Optional[Mapping[str, Identifier]] = None,
    ) -> List[UploadWorkItem]:
        """Build UploadWorkItem payloads from cached analyses and metadata with progress.

        `identifiers` are the articles' own, by slug: the ids of an article
        whose collections carry none -- a prose collection need not.
        """
        if not analyses:
            return []

        work_items: List[UploadWorkItem] = []
        progress = create_progress_bar(
            self.settings,
            total=len(analyses),
            desc="Prepare upload",
            unit="article",
        )

        for slug, per_table in analyses.items():
            try:
                if slug not in metadata:
                    logger.warning(
                        "Metadata missing for %s; proceeding with available fields",
                        slug,
                        extra=console_kwargs(),
                    )
                item = self._build_work_item(
                    slug,
                    per_table,
                    metadata.get(slug),
                    identifier=(identifiers or {}).get(slug),
                    metadata_mode=metadata_mode,
                )
                if item is not None:
                    work_items.append(item)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "Failed to prepare upload work item for %s: %s",
                    slug,
                    exc,
                    extra=console_kwargs(),
                )
            finally:
                if progress is not None:
                    progress.update(1)

        if progress is not None:
            progress.close()

        return work_items

    def run(
        self,
        work_items: Iterable[UploadWorkItem],
        *,
        behavior: UploadBehavior,
        metadata_only: bool,
        metadata_mode: UploadMetadataMode,
    ) -> List[UploadOutcome]:
        """Execute upload operations in a single transaction when possible."""
        outcomes: List[UploadOutcome] = []
        work_items_list = list(work_items)
        if not work_items_list:
            return outcomes

        self.session_factory.configure()

        with self.session_factory.session() as session:
            outer_tx = session.begin()
            progress = create_progress_bar(
                self.settings,
                total=len(work_items_list),
                desc="Upload",
                unit="article",
            )
            try:
                for item in work_items_list:
                    try:
                        with session.begin_nested():
                            outcome = self._process_item(
                                session,
                                item,
                                behavior=behavior,
                                metadata_only=metadata_only,
                                metadata_mode=metadata_mode,
                            )
                            outcomes.append(outcome)
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.error(
                            "Upload failed for %s: %s",
                            item.slug,
                            exc,
                            extra=console_kwargs(),
                        )
                        outcomes.append(
                            UploadOutcome(slug=item.slug, success=False, error=str(exc))
                        )
                    finally:
                        if progress is not None:
                            progress.update(1)
                outer_tx.commit()
            except Exception:
                outer_tx.rollback()
                raise
            finally:
                if progress is not None:
                    progress.close()

        return outcomes

    def retract(self, targets: Sequence[Tuple[str, str]], *,
                hold_studyset_members: bool = False) -> List["RetractOutcome"]:
        """Take this source's study version back for each (slug, base_study_id).

        Used when a person has marked every table of an article as holding no
        coordinates, so there is nothing left to upload. The version is deleted
        only when nothing anyone made depends on it: deleting a study cascades
        through `studyset_studies`, which would silently drop it from a user's
        studyset in Compose, and through `annotation_analyses`, which would
        discard an annotator's notes. Otherwise its unannotated analyses are
        removed and the version is kept, reported as `emptied`.

        Only the configured source's version is touched; other extractors'
        versions and the base study itself are left alone.

        With `hold_studyset_members`, a version any studyset holds is not touched
        at all and comes back `held`: a retraction nobody asked for must not change
        the coordinates under someone's meta-analysis, so a person reviews it.
        """
        source = resolve_upload_source(self.settings)
        outcomes: List[RetractOutcome] = []
        if not targets:
            return outcomes
        self.session_factory.configure()
        with self.session_factory.session() as session:
            outer = session.begin()
            try:
                for slug, base_study_id in targets:
                    try:
                        with session.begin_nested():
                            outcomes.append(self._retract_one(session, slug, base_study_id, source,
                                                              hold_studyset_members))
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.error("Retraction failed for %s: %s", slug, exc, extra=console_kwargs())
                        outcomes.append(RetractOutcome(slug=slug, base_study_id=base_study_id,
                                                       success=False, error=str(exc)))
                outer.commit()
            except Exception:
                outer.rollback()
                raise
        return outcomes

    def mark_retracted(
        self, targets: Mapping[str, Optional[dict]], clear: Sequence[str] = ()
    ) -> Optional[int]:
        """Set `is_retracted` and `retraction_notice` on each base study id given.

        `clear` lists base studies whose retraction was withdrawn: the flag goes
        to False and the notice to NULL, as neurostore's own ingester does.

        The columns arrive with neurostore's migration f1a3c5e7b9d2. Until
        a database has them this logs and returns None, changing nothing; the
        study itself is never deleted or emptied for a retraction.
        """
        if not targets and not clear:
            return 0
        self.session_factory.configure()
        with self.session_factory.session() as session:
            columns = inspect(session.connection()).get_columns("base_studies")
            names = {c["name"] for c in columns}
            if not {"is_retracted", "retraction_notice"} <= names:
                logger.warning(
                    "base_studies has no is_retracted column (neurostore migration f1a3c5e7b9d2); "
                    "%d retraction(s) not recorded", len(targets) + len(clear), extra=console_kwargs())
                return None
            notice_type = JSON().with_variant(postgresql.JSONB(), "postgresql")
            base = table("base_studies", column("id", Text), column("is_retracted", Boolean),
                         column("retraction_notice", notice_type))
            for base_study_id, notice in targets.items():
                session.execute(update(base).where(base.c.id == base_study_id)
                                .values(is_retracted=True, retraction_notice=notice))
            for base_study_id in clear:
                session.execute(update(base).where(base.c.id == base_study_id)
                                .values(is_retracted=False, retraction_notice=null()))
            session.commit()
        return len(targets) + len(clear)

    def _retract_one(self, session, slug: str, base_study_id: str, source: str,
                     hold_studyset_members: bool = False) -> "RetractOutcome":
        study = session.execute(
            select(DbStudy).where(DbStudy.base_study_id == base_study_id, DbStudy.source == source)
        ).scalars().first()
        if study is None:
            return RetractOutcome(slug=slug, base_study_id=base_study_id, action="absent", success=True)
        studysets = sorted(set(session.execute(
            select(DbStudysetStudy.studyset_id).where(DbStudysetStudy.study_id == study.id)
        ).scalars()))
        if studysets and hold_studyset_members:
            logger.warning("[retract id=%s] held version %s for review: in %d studysets",
                           slug, study.id, len(studysets))
            return RetractOutcome(slug=slug, base_study_id=base_study_id, study_id=study.id,
                                  action="held", studysets=studysets, success=True)
        analysis_ids = list(session.execute(
            select(DbAnalysis.id).where(DbAnalysis.study_id == study.id)
        ).scalars())
        annotated = self._annotated_analysis_ids(session, analysis_ids)
        removable = [a for a in analysis_ids if a not in annotated]
        for analysis_id in removable:
            self._delete_points(session, analysis_id)
        if removable:
            session.execute(delete(DbAnalysis).where(DbAnalysis.id.in_(removable)))
        study_id = study.id
        if annotated or studysets:
            session.flush()
            study.has_coordinates = bool(session.execute(
                select(DbPoint.id).join(DbAnalysis, DbAnalysis.id == DbPoint.analysis_id)
                .where(DbAnalysis.study_id == study_id).limit(1)).first())
            self._refresh_base_flag(session, base_study_id)
            logger.info(
                "[retract id=%s] kept version %s: %d analyses removed, %d annotated kept, in %d studysets",
                slug, study_id, len(removable), len(annotated), len(studysets),
            )
            return RetractOutcome(slug=slug, base_study_id=base_study_id, study_id=study_id,
                                  action="emptied", removed=len(removable), kept_annotated=len(annotated),
                                  studysets=studysets, success=True)
        session.execute(delete(DbTable).where(DbTable.study_id == study_id))
        session.expunge(study)
        session.execute(delete(DbStudy).where(DbStudy.id == study_id))
        self._refresh_base_flag(session, base_study_id)
        logger.info("[retract id=%s] deleted version %s (%d analyses)", slug, study_id, len(removable))
        return RetractOutcome(slug=slug, base_study_id=base_study_id, study_id=study_id,
                              action="deleted", removed=len(removable), success=True)

    def _apply_metadata(
        self,
        base_payload: BaseStudyPayload,
        study_payload: StudyPayload,
        article_metadata: ArticleMetadata,
        mode: UploadMetadataMode,
    ) -> None:
        """Map article metadata into payloads according to the update mode."""
        if article_metadata is None:
            return

        def _authors_str(authors) -> str:
            names = [a.name for a in authors if getattr(a, "name", None)]
            return "; ".join(names) if names else ""

        scalar_fields = {
            "name": article_metadata.title,
            "description": article_metadata.abstract,
            "publication": article_metadata.journal,
            "authors": _authors_str(article_metadata.authors),
            "year": article_metadata.publication_year,
        }
        for target in (base_payload, study_payload):
            self._set_fields(target, scalar_fields, mode)

        if article_metadata.open_access is not None:
            if mode == UploadMetadataMode.OVERWRITE or base_payload.is_oa is None:
                base_payload.is_oa = article_metadata.open_access

        # stash remaining metadata into metadata_ json
        extra_meta = {
            "keywords": article_metadata.keywords,
            "license": article_metadata.license,
            "source": article_metadata.source,
            "raw_metadata": article_metadata.raw_metadata,
        }
        for target in (base_payload, study_payload):
            target.metadata = self._merge_metadata_blob(
                getattr(target, "metadata", {}) or {},
                extra_meta,
                mode,
            )

    def _build_work_item(
        self,
        slug: str,
        per_table: Mapping[str, AnalysisCollection],
        article_metadata: Optional[ArticleMetadata],
        *,
        metadata_mode: UploadMetadataMode,
        identifier: Optional[Identifier] = None,
    ) -> Optional[UploadWorkItem]:
        """Create a single UploadWorkItem; returns None on skip."""
        if not per_table:
            logger.warning("No analyses found for %s; skipping.", slug, extra=console_kwargs())
            return None

        # A collection's identifier, else the article's own. Without either,
        # the base study is created with no doi, pmid or pmcid -- and cannot
        # be matched to the paper's existing base study, so it duplicates it.
        # 11,892 prose-only articles were uploaded so on 2026-10-07.
        identifier = next(
            (c.identifier for c in per_table.values() if c.identifier is not None), identifier
        )

        base_payload = BaseStudyPayload()
        study_payload = StudyPayload()
        if identifier is not None:
            base_payload.doi = identifier.doi
            base_payload.pmid = identifier.pmid
            base_payload.pmcid = identifier.pmcid
            study_payload.doi = identifier.doi
            study_payload.pmid = identifier.pmid
            study_payload.pmcid = identifier.pmcid

        if article_metadata is not None:
            self._apply_metadata(base_payload, study_payload, article_metadata, metadata_mode)

        # Track slug for reference in metadata blobs
        base_payload.metadata["slug"] = slug
        study_payload.metadata["slug"] = slug

        prepared_analyses: List[PreparedAnalysis] = []
        for collection in per_table.values():
            # A stored no-coordinates placeholder is a table reading, not an
            # analysis; `is_placeholder` says why.
            kept = [a for a in collection.analyses if not is_placeholder(a.name, a.coordinates)]
            if not kept:
                logger.warning(
                    "No analyses found in collection for %s; skipping collection.",
                    slug,
                    extra=console_kwargs(),
                )
                continue
            for a_index, analysis in enumerate(kept, start=1):
                # An analysis with no coordinates is uploaded on purpose. A
                # table that names a contrast and reports `n.s.` has run that
                # contrast and found nothing, and that is a result someone
                # meta-analysing will want. What is not uploaded is a table
                # the extractor returned no analyses for at all -- the empty
                # collection skipped just above.
                table_meta = analysis.metadata.get("table_metadata", {}) if analysis.metadata else {}
                sanitized_id = None
                if analysis.metadata:
                    sanitized_id = analysis.metadata.get("sanitized_table_id")
                table_id = analysis.table_id or sanitized_id or f"table-{a_index}"
                analysis_name = _sanitize_text(analysis.name)
                analysis_description = _sanitize_text(analysis.description)
                table_payload = TablePayload(
                    table_id=_sanitize_text(table_id) or f"table-{a_index}",
                    caption=_sanitize_text(analysis.table_caption) or "",
                    footer=_sanitize_text(analysis.table_footer) or "",
                    title=_sanitize_text(analysis.table_caption) or table_id or "",
                    label=_sanitize_text(sanitized_id or analysis.table_id),
                    metadata=_sanitize_mapping(
                        {
                            "table_metadata": table_meta,
                            "table_number": analysis.table_number,
                            "original_table_id": analysis.table_id,
                        }
                    ),
                )
                cleaned_analysis = replace(
                    analysis,
                    name=analysis_name,
                    description=analysis_description,
                    # The stage's `split` pairs halves by their place in its
                    # collection, which no neurostore analysis has, so the half
                    # goes up as a SignSplit without a key.
                    metadata=_sanitize_mapping(
                        {
                            **{k: v for k, v in (analysis.metadata or {}).items() if k != "split"},
                            **(
                                {"split": split}
                                if (split := declared_split(analysis.metadata))
                                else {}
                            ),
                        }
                    ),
                )
                prepared_analyses.append(
                    PreparedAnalysis(
                        table=table_payload,
                        analysis=cleaned_analysis,
                        coordinate_space=collection.coordinate_space.value
                        if collection.coordinate_space
                        else None,
                    )
                )

        if not prepared_analyses and not getattr(
            self.settings, "upload_metadata_only", False
        ):
            # Every collection was empty. Returning an item here creates a base
            # study and a study holding nothing, which says the paper was read
            # and reports no coordinates -- and that is a different claim from
            # the one the extractor made, which is that these tables are not
            # coordinate tables. 22 of 49,778 articles in the v19 corpus run
            # land here. The `n.s.` case is unaffected: a named analysis with
            # no points is a prepared analysis, so the list is not empty.
            logger.warning(
                "No analyses in any collection for %s; skipping.",
                slug,
                extra=console_kwargs(),
            )
            return None

        return UploadWorkItem(
            slug=slug,
            identifier=identifier,
            base_study=base_payload,
            study=study_payload,
            analyses=prepared_analyses,
        )

    # ---- internal helpers -------------------------------------------------

    def _process_item(
        self,
        session,
        item: UploadWorkItem,
        *,
        behavior: UploadBehavior,
        metadata_only: bool,
        metadata_mode: UploadMetadataMode,
    ) -> UploadOutcome:
        base_study = self._get_or_create_base_study(
            session,
            item.base_study,
            metadata_mode,
        )
        study = self._get_or_create_study(
            session,
            item.study,
            base_study,
            behavior,
            metadata_mode,
        )

        # Analyses are reconciled below rather than cleared: dropping them all
        # would cascade to annotation_analyses and discard annotator work.

        table_map: Dict[str, DbTable] = {}
        analysis_ids: List[str] = []
        # Prepare or reuse tables
        for prepared in item.analyses:
            t_id = prepared.table.table_id
            if t_id not in table_map:
                table_map[t_id] = self._upsert_table(session, study.id, prepared.table, metadata_mode)

        # Build deterministic analysis names per table (table label with numeric suffix if needed)
        name_counters: Dict[str, int] = {}
        if metadata_only:
            session.flush()
            return UploadOutcome(
                slug=item.slug,
                base_study_id=base_study.id,
                study_id=study.id,
                analysis_ids=[],
                success=True,
            )

        # Describe what this extraction wants the study to contain, before
        # touching any row, so it can be matched against what is already there.
        desired = []
        order_counter = 1
        for prepared in item.analyses:
            table_ref = table_map.get(prepared.table.table_id)
            base_name = self._resolve_analysis_base_name(prepared)
            counter_key = base_name or prepared.table.table_id
            count = name_counters.get(counter_key, 0) + 1
            name_counters[counter_key] = count
            metadata = dict(prepared.analysis.metadata or {})
            # The parse's place goes where neurostore keeps it: the key in source_id,
            # what it is made from in the metadata.
            parse = metadata.pop("parse", None) or {}
            desired.append(
                {
                    "name": base_name if count == 1 else f"{base_name}-{count}",
                    "table_id": table_ref.id if table_ref else None,
                    "description": prepared.analysis.description or prepared.table.caption or "",
                    "metadata_": {
                        **metadata,
                        **{k: parse[k] for k in _PLACE_METADATA if k in parse},
                        "table": prepared.table.metadata,
                    },
                    "order": order_counter,
                    "prepared": prepared,
                    "parse": parse,
                    "base_name": base_name,
                }
            )
            order_counter += 1

        # selectinload, not lazy: matching reads every analysis's points, and
        # over an SSH tunnel one query per analysis dominates the whole stage.
        existing_rows = (
            list(
                session.execute(
                    select(DbAnalysis)
                    .where(DbAnalysis.study_id == study.id)
                    .options(selectinload(DbAnalysis.points).selectinload(DbPoint.values))
                ).scalars()
            )
            if study.id and behavior == UploadBehavior.UPDATE
            else []
        )
        annotated_ids = self._annotated_analysis_ids(
            session, [row.id for row in existing_rows]
        )
        plan = plan_reconciliation(
            existing_rows,
            [entry["name"] for entry in desired],
            [_point_set(entry["prepared"].analysis.coordinates) for entry in desired],
            annotated_ids,
            [entry["parse"] for entry in desired],
            [entry["base_name"] for entry in desired],
        )
        if existing_rows:
            counts: Dict[str, int] = {}
            for decision in plan.decisions:
                counts[decision["decision"]] = counts.get(decision["decision"], 0) + 1
            logger.info(
                "[upload id=%s] analyses: %s",
                item.slug,
                ", ".join(f"{n} {d}" for d, n in sorted(counts.items())),
            )

        for row in plan.deleted:
            self._delete_points(session, row.id)
            # Core delete rather than session.delete: the ORM would try to
            # cascade to points this has already removed and warn about it.
            session.expunge(row)
            session.execute(delete(DbAnalysis).where(DbAnalysis.id == row.id))

        assignments = [(row, desired[index], False) for row, index in plan.updated]
        inserted_rows: Dict[int, DbAnalysis] = {}
        for index in plan.inserted:
            new_row = DbAnalysis(study_id=study.id)
            session.add(new_row)
            inserted_rows[index] = new_row
            assignments.append((new_row, desired[index], True))

        for analysis_row, entry, is_new in assignments:
            prepared = entry["prepared"]
            key = entry["parse"].get("key")
            if key:
                analysis_row.source_id = key
            analysis_row.table_id = entry["table_id"]
            analysis_row.name = entry["name"]
            analysis_row.description = entry["description"]
            analysis_row.metadata_ = entry["metadata_"]
            analysis_row.order = entry["order"]
            n_points = len(prepared.analysis.coordinates or ())
            analysis_row.point_count = n_points
            analysis_row.has_coordinates = bool(n_points)
            session.flush()  # ensure id for points
            analysis_ids.append(analysis_row.id)

            wanted = [_point_row(coord, prepared, p_index)
                      for p_index, coord in enumerate(prepared.analysis.coordinates, start=1)]
            if is_new or _stored_points(analysis_row) != wanted:
                self._write_points(session, analysis_row.id, wanted)

        for decision in plan.decisions:
            index = decision.pop("index", None)
            if decision["decision"] == "new":
                decision["analysis_id"] = inserted_rows[index].id

        # The flag lives on both rows and the API reads each; setting only the
        # base study left the study itself claiming no coordinates. An analysis left
        # in place is still the study's.
        any_points = any(
            (prepared.analysis.coordinates or ())
            for prepared in (entry["prepared"] for _, entry, _ in assignments)
        ) or any(row.points for row in plan.left)
        study.has_coordinates = any_points
        session.flush()
        self._refresh_base_flag(session, base_study.id)
        if item.analyses:
            study.level = level_for(study.name or base_study.name, study.level)

        session.flush()
        return UploadOutcome(
            slug=item.slug,
            base_study_id=base_study.id,
            study_id=study.id,
            analysis_ids=analysis_ids,
            reconcile=plan.decisions,
            success=True,
        )

    def _write_points(self, session, analysis_id: str, wanted) -> None:
        """Replace an analysis's points with `wanted` (`_point_row`s)."""
        self._delete_points(session, analysis_id)
        for fields, values in wanted:
            point = DbPoint(analysis_id=analysis_id, **dict(fields))
            session.add(point)
            for kind, value in values:
                session.add(DbPointValue(point=point, kind=kind, value=value))

    def _resolve_analysis_base_name(self, prepared: PreparedAnalysis) -> str:
        candidate = _sanitize_text(prepared.analysis.name)
        if candidate and candidate.upper() != "UNKNOWN":
            return candidate
        for fallback in (
            prepared.table.label,
            prepared.table.title,
            prepared.table.table_id,
        ):
            sanitized = _sanitize_text(fallback)
            if sanitized:
                return sanitized
        return _sanitize_text(prepared.table.table_id) or "analysis"

    def _get_or_create_base_study(
        self,
        session,
        payload: BaseStudyPayload,
        metadata_mode: UploadMetadataMode,
    ) -> DbBaseStudy:
        base = None
        if payload.doi:
            base = self._select_single_base_study(session, doi=payload.doi)
        if base is None and payload.pmid:
            base = self._select_single_base_study(session, pmid=payload.pmid)

        if base is None:
            base = DbBaseStudy(
                level=level_for(payload.name),
                public=True,
            )
            session.add(base)

        # A meta-analysis is stored as one, whatever level it had; otherwise
        # the level already there stands, so a correction is not undone.
        base.level = level_for(payload.name or base.name, base.level)

        self._apply_payload_fields(base, payload, metadata_mode)
        session.flush()
        return base

    def _get_or_create_study(
        self,
        session,
        payload: StudyPayload,
        base_study: DbBaseStudy,
        behavior: UploadBehavior,
        metadata_mode: UploadMetadataMode,
    ) -> DbStudy:
        # The source names the extractor that produced the analyses, and it
        # is what neurostore versions a study by -- so a different extractor
        # adds a version beside the old one rather than replacing it.
        payload.source = payload.source or resolve_upload_source(self.settings)
        study = next(
            (version for version in getattr(base_study, "versions", []) if version.source == payload.source),
            None,
        )

        if study is None or behavior == UploadBehavior.INSERT_NEW:
            study = DbStudy(
                base_study_id=base_study.id,
                source=payload.source,
                source_id=payload.metadata.get("source_id") if payload.metadata else None,
                level=level_for(payload.name or base_study.name),
            )
            session.add(study)

        self._apply_payload_fields(study, payload, metadata_mode)
        study.level = level_for(payload.name or base_study.name, study.level)
        study.source = payload.source
        study.source_updated_at = datetime.now(timezone.utc)
        session.flush()
        return study

    def _select_single_base_study(self, session, *, doi: str | None = None, pmid: str | None = None) -> DbBaseStudy | None:
        """Fetch a single base study; if duplicates exist, pick the first and log."""
        if doi is None and pmid is None:
            return None
        query = select(DbBaseStudy)
        label = ""
        value = ""
        if doi is not None:
            query = query.where(DbBaseStudy.doi == doi)
            label = "doi"
            value = doi
        elif pmid is not None:
            query = query.where(DbBaseStudy.pmid == pmid)
            label = "pmid"
            value = pmid

        results = session.execute(
            query.order_by(DbBaseStudy.created_at).limit(2)
        ).scalars().all()
        if len(results) > 1:
            logger.warning(
                "Multiple base studies found for %s=%s; using first (id=%s)",
                label,
                value,
                results[0].id,
                extra=console_kwargs(),
            )
        return results[0] if results else None

    def _apply_payload_fields(self, target, payload, mode: UploadMetadataMode) -> None:
        scalars = {
            "name": payload.name,
            "description": payload.description,
            "publication": payload.publication,
            "doi": payload.doi,
            "pmid": payload.pmid,
            "pmcid": payload.pmcid,
            "authors": payload.authors,
            "year": payload.year,
        }
        self._set_fields(target, {k: _sanitize_text(v) for k, v in scalars.items()}, mode)

        if getattr(payload, "metadata", None) is not None:
            target.metadata_ = self._merge_metadata_blob(
                getattr(target, "metadata_", {}) or {},
                _sanitize_mapping(payload.metadata or {}),
                mode,
            )

        if hasattr(payload, "is_oa") and payload.is_oa is not None:
            if mode == UploadMetadataMode.OVERWRITE or getattr(target, "is_oa", None) is None:
                target.is_oa = payload.is_oa

    def _set_fields(self, target, fields: Dict[str, object], mode: UploadMetadataMode) -> None:
        for field, incoming in fields.items():
            if incoming is None or incoming == "":
                continue
            current = getattr(target, field, None)
            if mode == UploadMetadataMode.FILL:
                if current in (None, ""):
                    setattr(target, field, incoming)
            else:
                setattr(target, field, incoming)

    def _merge_metadata_blob(
        self,
        existing: Dict[str, object],
        incoming: Dict[str, object],
        mode: UploadMetadataMode,
    ) -> Dict[str, object]:
        merged = dict(existing or {})
        if mode == UploadMetadataMode.OVERWRITE:
            merged.update(incoming)
        else:
            for key, value in incoming.items():
                if merged.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
                    merged[key] = value
        return merged

    def _refresh_base_flag(self, session, base_study_id: str) -> None:
        """Set the base study's `has_coordinates` from what its versions now hold.

        Upload used to set it true and nothing ever set it false, so retracting a
        study or re-uploading one with no points left the flag claiming
        coordinates: 287 retracted base studies and 78 others still came back from
        the API's coordinate search with nothing in them.
        """
        base = session.get(DbBaseStudy, base_study_id)
        if base is None:
            return
        base.has_coordinates = bool(session.execute(
            select(DbPoint.id)
            .join(DbAnalysis, DbAnalysis.id == DbPoint.analysis_id)
            .join(DbStudy, DbStudy.id == DbAnalysis.study_id)
            .where(DbStudy.base_study_id == base_study_id)
            .limit(1)
        ).first())

    def _delete_points(self, session, analysis_id: str) -> None:
        """Remove an analysis's points and their values.

        point_values is cleared explicitly rather than left to the database's
        cascade so the behaviour is the same on any backend.
        """
        point_ids = select(DbPoint.id).where(DbPoint.analysis_id == analysis_id)
        session.execute(delete(DbPointValue).where(DbPointValue.point_id.in_(point_ids)))
        session.execute(delete(DbPoint).where(DbPoint.analysis_id == analysis_id))

    def _annotated_analysis_ids(self, session, analysis_ids: Sequence[str]) -> set:
        """Which of these analyses carry a note someone actually filled in.

        Belonging to an annotated studyset is not enough: neurostore backfills a
        row of defaults for every analysis it contains. Only a note that differs
        from those defaults counts, otherwise nothing would ever be removable.
        On any error this reports every analysis as annotated, so a failure here
        can only make upload more conservative.
        """
        if not analysis_ids:
            return set()
        try:
            rows = session.execute(
                select(
                    DbAnnotationAnalysis.analysis_id,
                    DbAnnotationAnalysis.note,
                    DbAnnotation.note_keys,
                )
                .join(
                    DbAnnotation,
                    DbAnnotation.id == DbAnnotationAnalysis.annotation_id,
                    isouter=True,
                )
                .where(DbAnnotationAnalysis.analysis_id.in_(list(analysis_ids)))
            ).all()
        except Exception:
            logger.warning(
                "Could not read annotation_analyses; treating every analysis as "
                "annotated and removing none.",
                exc_info=True,
            )
            return set(analysis_ids)

        annotated = set()
        for analysis_id, note, note_keys in rows:
            if _note_is_substantive(note, note_keys):
                annotated.add(analysis_id)
        return annotated

    def _upsert_table(
        self,
        session,
        study_id: str,
        payload: TablePayload,
        metadata_mode: UploadMetadataMode,
    ) -> DbTable:
        existing = session.execute(
            select(DbTable).where(DbTable.study_id == study_id, DbTable.t_id == payload.table_id)
        ).scalar_one_or_none()
        if existing is None:
            existing = DbTable(
                study_id=study_id,
                t_id=payload.table_id,
            )
            session.add(existing)
        fields = {
            "name": payload.title,
            "caption": payload.caption,
            "footer": payload.footer,
        }
        self._set_fields(existing, fields, metadata_mode)
        session.flush()
        return existing
