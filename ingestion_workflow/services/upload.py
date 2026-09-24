"""Upload service orchestrating metadata and coordinate persistence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

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
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.logging import console_kwargs, get_logger
from ingestion_workflow.services.upload_models import Analysis as DbAnalysis
from ingestion_workflow.services.upload_models import Annotation as DbAnnotation
from ingestion_workflow.services.upload_models import (
    AnnotationAnalysis as DbAnnotationAnalysis,
)
from ingestion_workflow.services.upload_models import BaseStudy as DbBaseStudy
from ingestion_workflow.services.upload_models import Point as DbPoint
from ingestion_workflow.services.upload_models import PointValue as DbPointValue
from ingestion_workflow.services.upload_models import Study as DbStudy
from ingestion_workflow.services.upload_models import Table as DbTable
from ingestion_workflow.utils.console import progress_bar as create_progress_bar

logger = get_logger(__name__)


# An analysis is matched to an existing one mostly on the coordinates it
# reports: names drift between extractions ("Table 2" vs "Table 2-1") while the
# peaks do not. Names only break ties, and carry a match on their own when
# neither side has coordinates to compare.
_COORD_MATCH_THRESHOLD = 0.5
_NAME_MATCH_THRESHOLD = 0.9
_COORD_ROUNDING = 0


@dataclass
class ReconcilePlan:
    """What upload intends to do with one study's existing analyses."""

    updated: List[Tuple[object, int]]     # (existing row, index into incoming)
    inserted: List[int]                   # indices into incoming
    deleted: List[object]                 # existing rows to remove
    kept_annotated: List[object]          # unmatched, but annotated: left alone


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


def _coordinate_key(x, y, z) -> Optional[Tuple[float, float, float]]:
    try:
        return (
            round(float(x), _COORD_ROUNDING),
            round(float(y), _COORD_ROUNDING),
            round(float(z), _COORD_ROUNDING),
        )
    except (TypeError, ValueError):
        return None


def _coordinate_set(points: Iterable) -> set:
    keys = set()
    for point in points or []:
        key = _coordinate_key(
            getattr(point, "x", None), getattr(point, "y", None), getattr(point, "z", None)
        )
        if key is not None:
            keys.add(key)
    return keys


def _jaccard(left: set, right: set) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _name_similarity(left: Optional[str], right: Optional[str]) -> float:
    a, b = _normalize_analysis_name(left), _normalize_analysis_name(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def _match_score(existing_coords: set, existing_name, incoming_coords: set, incoming_name):
    """Score a candidate pairing, or None when the pair is not a match.

    Coordinates decide it when either side has them. Two analyses with no
    coordinates at all can still pair on a near-identical name, which is the
    only evidence available for them.

    Every pair in a study is scored, so the cheap test comes first: set
    intersection rules out almost all pairs, and the name comparison -- a
    SequenceMatcher, quadratic in name length -- runs only for the few that
    survive it.
    """
    if not existing_coords and not incoming_coords:
        name = _name_similarity(existing_name, incoming_name)
        return name if name >= _NAME_MATCH_THRESHOLD else None
    overlap = _jaccard(existing_coords, incoming_coords)
    if overlap < _COORD_MATCH_THRESHOLD:
        return None
    # name only breaks ties between equally good coordinate matches
    return overlap + _name_similarity(existing_name, incoming_name) / 100.0


def plan_reconciliation(
    existing: Sequence,
    incoming_names: Sequence[Optional[str]],
    incoming_coords: Sequence[set],
    annotated_ids: set,
) -> ReconcilePlan:
    """Decide, for one study, what to update, insert, delete and leave alone.

    Matched pairs are updated in place so the analysis keeps its id and any
    annotation attached to it survives. An unmatched existing analysis is
    removed only when nothing is annotated against it; when something is, it
    stays and the new analysis is appended alongside.
    """
    existing_coords = [_coordinate_set(getattr(row, "points", [])) for row in existing]

    candidates = []
    for e_index, row in enumerate(existing):
        for i_index, name in enumerate(incoming_names):
            score = _match_score(
                existing_coords[e_index],
                getattr(row, "name", None),
                incoming_coords[i_index],
                name,
            )
            if score is not None:
                candidates.append((score, e_index, i_index))
    candidates.sort(key=lambda c: (-c[0], c[1], c[2]))

    taken_existing: set = set()
    taken_incoming: set = set()
    updated = []
    for _score, e_index, i_index in candidates:
        if e_index in taken_existing or i_index in taken_incoming:
            continue
        taken_existing.add(e_index)
        taken_incoming.add(i_index)
        updated.append((existing[e_index], i_index))

    deleted, kept = [], []
    for e_index, row in enumerate(existing):
        if e_index in taken_existing:
            continue
        if getattr(row, "id", None) in annotated_ids:
            kept.append(row)
        else:
            deleted.append(row)

    inserted = [i for i in range(len(incoming_names)) if i not in taken_incoming]
    return ReconcilePlan(
        updated=updated, inserted=inserted, deleted=deleted, kept_annotated=kept
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
    ) -> List[UploadWorkItem]:
        """Build UploadWorkItem payloads from cached analyses and metadata with progress."""
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
    ) -> Optional[UploadWorkItem]:
        """Create a single UploadWorkItem; returns None on skip."""
        if not per_table:
            logger.warning("No analyses found for %s; skipping.", slug, extra=console_kwargs())
            return None

        identifier = None
        for collection in per_table.values():
            if collection.identifier is not None:
                identifier = collection.identifier
                break

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
            if not collection.analyses:
                logger.warning(
                    "No analyses found in collection for %s; skipping collection.",
                    slug,
                    extra=console_kwargs(),
                )
                continue
            for a_index, analysis in enumerate(collection.analyses, start=1):
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
                    metadata=_sanitize_mapping(analysis.metadata or {}),
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
            desired.append(
                {
                    "name": base_name if count == 1 else f"{base_name}-{count}",
                    "table_id": table_ref.id if table_ref else None,
                    "description": prepared.analysis.description or prepared.table.caption or "",
                    "metadata_": {
                        **(prepared.analysis.metadata or {}),
                        "table": prepared.table.metadata,
                    },
                    "order": order_counter,
                    "prepared": prepared,
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
                    .options(selectinload(DbAnalysis.points))
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
            [
                _coordinate_set(entry["prepared"].analysis.coordinates)
                for entry in desired
            ],
            annotated_ids,
        )
        if existing_rows:
            logger.info(
                "[upload id=%s] analyses: %d updated, %d added, %d removed, "
                "%d kept for their annotations",
                item.slug,
                len(plan.updated),
                len(plan.inserted),
                len(plan.deleted),
                len(plan.kept_annotated),
            )

        for row in plan.deleted:
            self._delete_points(session, row.id)
            # Core delete rather than session.delete: the ORM would try to
            # cascade to points this has already removed and warn about it.
            session.expunge(row)
            session.execute(delete(DbAnalysis).where(DbAnalysis.id == row.id))

        assignments = [(row, desired[index]) for row, index in plan.updated]
        for index in plan.inserted:
            new_row = DbAnalysis(study_id=study.id)
            session.add(new_row)
            assignments.append((new_row, desired[index]))

        for analysis_row, entry in assignments:
            prepared = entry["prepared"]
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

            # Replace coordinates: insert fresh set.
            self._delete_points(session, analysis_row.id)
            for p_index, coord in enumerate(prepared.analysis.coordinates, start=1):
                point = DbPoint(
                    analysis_id=analysis_row.id,
                    x=coord.x,
                    y=coord.y,
                    z=coord.z,
                    space=coord.space.value if coord.space else prepared.coordinate_space,
                    cluster_size=coord.cluster_size,
                    subpeak=coord.is_subpeak,
                    deactivation=coord.is_deactivation,
                    order=p_index,
                )
                session.add(point)
                if coord.statistic_type or coord.statistic_value is not None:
                    try:
                        value = (
                            float(coord.statistic_value)
                            if coord.statistic_value is not None
                            else None
                        )
                    except (TypeError, ValueError):
                        value = None
                    session.add(
                        DbPointValue(
                            point=point,
                            kind=coord.statistic_type,
                            value=value,
                        )
                    )

        # The flag lives on both rows and the API reads each; setting only the
        # base study left the study itself claiming no coordinates.
        any_points = any(
            (prepared.analysis.coordinates or ())
            for prepared in (entry["prepared"] for _, entry in assignments)
        )
        study.has_coordinates = any_points
        if any_points:
            base_study.has_coordinates = True
        if item.analyses:
            study.level = "group"  # ensure level is set

        session.flush()
        return UploadOutcome(
            slug=item.slug,
            base_study_id=base_study.id,
            study_id=study.id,
            analysis_ids=analysis_ids,
            success=True,
        )

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
                level="group",
                public=True,
            )
            session.add(base)

        # ensure level
        if base.level != "group":
            base.level = "group"

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
        # Always treat uploads as coming from the LLM pipeline
        payload.source = payload.source or "llm"
        study = next(
            (version for version in getattr(base_study, "versions", []) if version.source == payload.source),
            None,
        )

        if study is None or behavior == UploadBehavior.INSERT_NEW:
            study = DbStudy(
                base_study_id=base_study.id,
                source=payload.source,
                source_id=payload.metadata.get("source_id") if payload.metadata else None,
                level="group",
            )
            session.add(study)

        self._apply_payload_fields(study, payload, metadata_mode)
        study.level = "group"
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
