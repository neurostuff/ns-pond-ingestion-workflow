import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleMetadata,
    Author,
    BaseStudyPayload,
    Coordinate,
    CoordinateSpace,
    Identifier,
)
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.upload import UploadService
from ingestion_workflow.services.upload_models import (
    Analysis as DbAnalysis,
    Base as UploadBase,
    BaseStudy as DbBaseStudy,
    Point as DbPoint,
    Study as DbStudy,
    Table as DbTable,
)


def _settings(tmp_path, *, metadata_mode: UploadMetadataMode = UploadMetadataMode.FILL) -> Settings:
    """Create settings rooted in a temp directory."""
    return Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns",
        upload_use_ssh=False,
        upload_metadata_mode=metadata_mode,
    )


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    UploadBase.metadata.create_all(engine)
    return engine


def _sample_collection(identifier: Identifier) -> AnalysisCollection:
    analysis = Analysis(
        name="A1",
        description="desc",
        coordinates=[
            Coordinate(x=1.0, y=2.0, z=3.0, space=CoordinateSpace.MNI, statistic_type="t"),
            Coordinate(x=4.0, y=5.0, z=6.0, space=CoordinateSpace.MNI, statistic_type="t"),
        ],
        table_id="T1",
        table_number=1,
        table_caption="cap",
        table_footer="foot",
        metadata={"table_metadata": {"foo": "bar"}, "sanitized_table_id": "t1"},
    )
    collection = AnalysisCollection(
        slug="slug::t1",
        analyses=[analysis],
        coordinate_space=CoordinateSpace.MNI,
        identifier=identifier,
    )
    return collection


def _article_metadata(title: str = "TITLE", open_access: bool = True) -> ArticleMetadata:
    return ArticleMetadata(
        title=title,
        authors=[Author(name="Jane Doe"), Author(name="John Doe")],
        abstract="ABSTRACT",
        journal="JOURNAL",
        publication_year=2024,
        keywords=["k1", "k2"],
        license="CC",
        source="test",
        open_access=open_access,
        raw_metadata={"k": "v"},
    )


def test_prepare_work_items_builds_payload(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    analyses = {"slug": {"t1": _sample_collection(identifier)}}
    metadata = {"slug": _article_metadata()}
    settings = _settings(tmp_path)
    factory = SessionFactory(settings, engine=_engine())
    service = UploadService(settings, factory)

    items = service.prepare_work_items(
        analyses,
        metadata,
        metadata_mode=settings.upload_metadata_mode,
    )

    assert len(items) == 1
    item = items[0]
    assert item.slug == "slug"
    assert item.base_study.doi == "10.1/abc"
    assert item.study.pmid == "123"
    assert item.base_study.name == "TITLE"
    assert item.base_study.is_oa is True
    assert item.analyses and item.analyses[0].table.caption == "cap"


def test_run_upload_inserts_records(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    analyses = {"slug": {"t1": _sample_collection(identifier)}}
    metadata = {"slug": _article_metadata()}
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)

    items = service.prepare_work_items(
        analyses,
        metadata,
        metadata_mode=settings.upload_metadata_mode,
    )
    outcomes = service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=False,
        metadata_mode=settings.upload_metadata_mode,
    )

    assert outcomes and outcomes[0].success is True
    with Session(engine, future=True) as session:
        bs_count = session.scalar(select_count(DbBaseStudy))
        st_count = session.scalar(select_count(DbStudy))
        tbl_count = session.scalar(select_count(DbTable))
        anal_count = session.scalar(select_count(DbAnalysis))
        pt_count = session.scalar(select_count(DbPoint))
    assert bs_count == 1
    assert st_count == 1
    assert tbl_count == 1
    assert anal_count == 1
    assert pt_count == 2


def test_metadata_only_fill_and_overwrite(tmp_path):
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)
    identifier = Identifier(doi="10.1/abc", pmid="123")
    analyses = {"slug": {"t1": _sample_collection(identifier)}}
    items = service.prepare_work_items(
        analyses,
        {"slug": _article_metadata("NEW", open_access=True)},
        metadata_mode=settings.upload_metadata_mode,
    )

    # pre-seed existing records
    with Session(engine, future=True) as session:
        base = DbBaseStudy(id="bs1", name="OLD", level="group", public=True, doi="10.1/abc")
        study = DbStudy(id="st1", base_study_id="bs1", name="KEEP", source="llm", level="group")
        session.add_all([base, study])
        session.commit()

    # fill mode should not overwrite existing name/description, but should fill empty authors/year/is_oa
    outcomes = service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=True,
        metadata_mode=UploadMetadataMode.FILL,
    )
    assert outcomes[0].success is True
    with Session(engine, future=True) as session:
        base = session.get(DbBaseStudy, outcomes[0].base_study_id)
        study = session.get(DbStudy, outcomes[0].study_id)
        assert base.name == "OLD"
        assert study.name == "KEEP"
        assert base.is_oa is True
        assert study.authors == "Jane Doe; John Doe"
        # no analyses or points inserted in metadata-only path
        assert session.scalar(select_count(DbAnalysis)) == 0
        assert session.scalar(select_count(DbPoint)) == 0

    # overwrite mode should update name/title
    outcomes = service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=True,
        metadata_mode=UploadMetadataMode.OVERWRITE,
    )
    with Session(engine, future=True) as session:
        base = session.get(DbBaseStudy, outcomes[0].base_study_id)
        study = session.get(DbStudy, outcomes[0].study_id)
        assert base.name == "NEW"
        assert study.name == "NEW"


# --- helpers -----------------------------------------------------------------


def select_count(model):
    from sqlalchemy import func, select

    return select(func.count()).select_from(model)
