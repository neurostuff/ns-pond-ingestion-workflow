"""Upload service orchestrating metadata and coordinate persistence."""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, Iterable, List, Mapping, Optional
from datetime import datetime, timezone

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from sqlalchemy import delete, select

from ingestion_workflow.models import (
    Analysis,
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
from ingestion_workflow.services.logging import get_logger
from ingestion_workflow.workflow.common import create_progress_bar
from ingestion_workflow.services.logging import console_kwargs
from ingestion_workflow.services.upload_models import Analysis as DbAnalysis
from ingestion_workflow.services.upload_models import BaseStudy as DbBaseStudy
from ingestion_workflow.services.upload_models import Point as DbPoint
from ingestion_workflow.services.upload_models import PointValue as DbPointValue
from ingestion_workflow.services.upload_models import Study as DbStudy
from ingestion_workflow.services.upload_models import Table as DbTable

logger = get_logger(__name__)


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

        # When updating, replace analyses/points/tables content (skip in metadata-only mode)
        if not metadata_only and behavior == UploadBehavior.UPDATE and study.id:
            self._clear_study_content(session, study.id)

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

        order_counter = 1
        for prepared in item.analyses:
            table_ref = table_map.get(prepared.table.table_id)
            base_name = self._resolve_analysis_base_name(prepared)
            counter_key = base_name or prepared.table.table_id
            count = name_counters.get(counter_key, 0) + 1
            name_counters[counter_key] = count
            analysis_name = base_name if count == 1 else f"{base_name}-{count}"
            analysis_row = DbAnalysis(
                study_id=study.id,
                table_id=table_ref.id if table_ref else None,
                name=analysis_name,
                description=prepared.analysis.description or prepared.table.caption or "",
                metadata_={
                    **(prepared.analysis.metadata or {}),
                    "table": prepared.table.metadata,
                },
                order=order_counter,
            )
            order_counter += 1
            session.add(analysis_row)
            session.flush()  # ensure id for points
            analysis_ids.append(analysis_row.id)

            # Replace coordinates: insert fresh set.
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

        if item.analyses:
            base_study.has_coordinates = True
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
            base = session.execute(
                select(DbBaseStudy).where(DbBaseStudy.doi == payload.doi)
            ).scalar_one_or_none()
        if base is None and payload.pmid:
            base = session.execute(
                select(DbBaseStudy).where(DbBaseStudy.pmid == payload.pmid)
            ).scalar_one_or_none()

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

    def _clear_study_content(self, session, study_id: str) -> None:
        analysis_ids = select(DbAnalysis.id).where(DbAnalysis.study_id == study_id)
        session.execute(delete(DbPoint).where(DbPoint.analysis_id.in_(analysis_ids)))
        session.execute(delete(DbAnalysis).where(DbAnalysis.study_id == study_id))
        session.execute(delete(DbTable).where(DbTable.study_id == study_id))

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
