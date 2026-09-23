
import pytest
from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleMetadata,
    Author,
    Coordinate,
    CoordinateSpace,
    Identifier,
)
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.upload import (
    UploadService,
    _normalize_analysis_name,
    _note_is_substantive,
    plan_reconciliation,
)
from ingestion_workflow.services.upload_models import (
    Analysis as DbAnalysis,
)
from ingestion_workflow.services.upload_models import (
    Annotation as DbAnnotation,
)
from ingestion_workflow.services.upload_models import (
    AnnotationAnalysis as DbAnnotationAnalysis,
)
from ingestion_workflow.services.upload_models import (
    Base as UploadBase,
)
from ingestion_workflow.services.upload_models import (
    BaseStudy as DbBaseStudy,
)
from ingestion_workflow.services.upload_models import (
    Point as DbPoint,
)
from ingestion_workflow.services.upload_models import (
    PointValue as DbPointValue,
)
from ingestion_workflow.services.upload_models import (
    Study as DbStudy,
)
from ingestion_workflow.services.upload_models import (
    Table as DbTable,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


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


def _sample_collection(
    identifier: Identifier,
    *,
    analysis_name: str = "A1",
    statistic_values: tuple[float, float] = (1.0, 2.0),
    sanitized_table_id: str = "t1",
) -> AnalysisCollection:
    analysis = Analysis(
        name=analysis_name,
        description="desc",
        coordinates=[
            Coordinate(
                x=1.0,
                y=2.0,
                z=3.0,
                space=CoordinateSpace.MNI,
                statistic_type="t",
                statistic_value=statistic_values[0],
            ),
            Coordinate(
                x=4.0,
                y=5.0,
                z=6.0,
                space=CoordinateSpace.MNI,
                statistic_type="t",
                statistic_value=statistic_values[1],
            ),
        ],
        table_id="T1",
        table_number=1,
        table_caption="cap",
        table_footer="foot",
        metadata={
            "table_metadata": {"foo": "bar"},
            "sanitized_table_id": sanitized_table_id,
        },
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


def test_run_upload_records_point_values(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    stats = (4.25, 6.75)
    analyses = {"slug": {"t1": _sample_collection(identifier, statistic_values=stats)}}
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
    service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=False,
        metadata_mode=settings.upload_metadata_mode,
    )

    with Session(engine, future=True) as session:
        values = session.scalars(
            select(DbPointValue.value).order_by(DbPointValue.value)
        ).all()
        kinds = session.scalars(select(DbPointValue.kind)).all()
    assert values == list(stats)
    assert all(kind == "t" for kind in kinds)


def test_unknown_analysis_name_uses_table_label(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    custom_label = "custom-label"
    analyses = {
        "slug": {
            "t1": _sample_collection(
                identifier,
                analysis_name="UNKNOWN",
                sanitized_table_id=custom_label,
            )
        }
    }
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
    service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=False,
        metadata_mode=settings.upload_metadata_mode,
    )

    with Session(engine, future=True) as session:
        name = session.scalar(select(DbAnalysis.name))
    assert name == custom_label


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


# --------------------------------------------------------------------------
# Re-upload reconciliation: what happens to analyses that already exist.
# --------------------------------------------------------------------------


def _upload_once(service, settings, identifier, collection, metadata):
    items = service.prepare_work_items(
        {"slug": {"t1": collection}},
        {"slug": metadata},
        metadata_mode=settings.upload_metadata_mode,
    )
    return service.run(
        items,
        behavior=UploadBehavior.UPDATE,
        metadata_only=False,
        metadata_mode=settings.upload_metadata_mode,
    )


def _annotate(session, analysis_id, note, note_keys=None):
    session.add(DbAnnotation(id="ann1", name="a", note_keys=note_keys))
    session.add(
        DbAnnotationAnalysis(
            annotation_id="ann1", analysis_id=analysis_id, note=note
        )
    )
    session.commit()


def _coords(*triples):
    return [
        Coordinate(x=x, y=y, z=z, space=CoordinateSpace.MNI)
        for x, y, z in triples
    ]


def _collection(identifier, analyses):
    return AnalysisCollection(
        slug="slug::t1",
        analyses=analyses,
        coordinate_space=CoordinateSpace.MNI,
        identifier=identifier,
    )


def _analysis(name, coordinates, sanitized_table_id="t1"):
    return Analysis(
        name=name,
        description="desc",
        coordinates=coordinates,
        table_id="T1",
        table_number=1,
        table_caption="cap",
        table_footer="foot",
        metadata={"sanitized_table_id": sanitized_table_id},
    )


def test_reupload_updates_a_matching_analysis_in_place(tmp_path):
    """Matching on coordinates keeps the analysis id, so annotations survive."""
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)

    first = _collection(identifier, [_analysis("Table 1", _coords((1, 2, 3), (4, 5, 6)))])
    _upload_once(service, settings, identifier, first, _article_metadata())

    with Session(engine, future=True) as session:
        original_id = session.execute(select(DbAnalysis.id)).scalar_one()
        _annotate(session, original_id, {"included": False})

    # same peaks, renamed, one extra coordinate
    second = _collection(
        identifier, [_analysis("Table 1 renamed", _coords((1, 2, 3), (4, 5, 6), (7, 8, 9)))]
    )
    _upload_once(service, settings, identifier, second, _article_metadata())

    with Session(engine, future=True) as session:
        rows = list(session.execute(select(DbAnalysis)).scalars())
        assert len(rows) == 1
        assert rows[0].id == original_id, "analysis was replaced, annotation would be lost"
        assert rows[0].name == "Table 1 renamed"
        points = list(
            session.execute(
                select(DbPoint).where(DbPoint.analysis_id == original_id)
            ).scalars()
        )
        assert len(points) == 3
        link = session.execute(select(DbAnnotationAnalysis)).scalar_one()
        assert link.analysis_id == original_id


def test_reupload_keeps_an_unmatched_annotated_analysis_and_appends(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)

    first = _collection(identifier, [_analysis("Old", _coords((1, 2, 3), (4, 5, 6)))])
    _upload_once(service, settings, identifier, first, _article_metadata())

    with Session(engine, future=True) as session:
        original_id = session.execute(select(DbAnalysis.id)).scalar_one()
        _annotate(session, original_id, {"included": False, "note": "checked by hand"})

    # wholly different peaks and name: no match
    second = _collection(
        identifier, [_analysis("New", _coords((50, 51, 52), (53, 54, 55)))]
    )
    _upload_once(service, settings, identifier, second, _article_metadata())

    with Session(engine, future=True) as session:
        rows = list(session.execute(select(DbAnalysis)).scalars())
        names = sorted(r.name for r in rows)
        assert names == ["New", "Old"], "annotated analysis should have been kept"
        assert original_id in {r.id for r in rows}
        assert session.execute(select(DbAnnotationAnalysis)).scalar_one() is not None


def test_reupload_deletes_an_unmatched_analysis_with_no_annotation(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)

    first = _collection(identifier, [_analysis("Old", _coords((1, 2, 3), (4, 5, 6)))])
    _upload_once(service, settings, identifier, first, _article_metadata())
    with Session(engine, future=True) as session:
        original_id = session.execute(select(DbAnalysis.id)).scalar_one()

    second = _collection(
        identifier, [_analysis("New", _coords((50, 51, 52), (53, 54, 55)))]
    )
    _upload_once(service, settings, identifier, second, _article_metadata())

    with Session(engine, future=True) as session:
        rows = list(session.execute(select(DbAnalysis)).scalars())
        assert [r.name for r in rows] == ["New"]
        assert original_id not in {r.id for r in rows}
        orphans = list(
            session.execute(
                select(DbPoint).where(DbPoint.analysis_id == original_id)
            ).scalars()
        )
        assert orphans == []


def test_reupload_treats_a_default_note_as_unannotated(tmp_path):
    """A backfilled note of defaults means nobody annotated the analysis."""
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    factory = SessionFactory(settings, engine=engine)
    service = UploadService(settings, factory)

    first = _collection(identifier, [_analysis("Old", _coords((1, 2, 3)))])
    _upload_once(service, settings, identifier, first, _article_metadata())
    with Session(engine, future=True) as session:
        original_id = session.execute(select(DbAnalysis.id)).scalar_one()
        _annotate(
            session,
            original_id,
            {"included": True, "comment": None},
            note_keys={"included": "boolean", "comment": "string"},
        )

    second = _collection(identifier, [_analysis("New", _coords((50, 51, 52)))])
    _upload_once(service, settings, identifier, second, _article_metadata())

    with Session(engine, future=True) as session:
        rows = list(session.execute(select(DbAnalysis)).scalars())
        assert [r.name for r in rows] == ["New"]


@pytest.mark.parametrize(
    ("note", "note_keys", "expected"),
    [
        (None, None, False),
        ({}, None, False),
        ({"included": True}, {"included": "boolean"}, False),
        ({"included": None, "x": None}, {"included": "boolean", "x": "string"}, False),
        ({"included": False}, {"included": "boolean"}, True),
        ({"comment": "looks wrong"}, {"comment": "string"}, True),
        ({"n": 3}, None, True),
    ],
)
def test_note_is_substantive(note, note_keys, expected):
    assert _note_is_substantive(note, note_keys) is expected


def test_plan_reconciliation_matches_on_coordinates_not_order():
    class _Row:
        def __init__(self, id, name, points):
            self.id = id
            self.name = name
            self.points = points

    class _P:
        def __init__(self, x, y, z):
            self.x, self.y, self.z = x, y, z

    existing = [
        _Row("a", "Table 2", [_P(10, 10, 10), _P(11, 11, 11)]),
        _Row("b", "Table 1", [_P(1, 1, 1), _P(2, 2, 2)]),
    ]
    incoming_names = ["Table 1", "Table 2"]
    incoming_coords = [
        {(1.0, 1.0, 1.0), (2.0, 2.0, 2.0)},
        {(10.0, 10.0, 10.0), (11.0, 11.0, 11.0)},
    ]

    plan = plan_reconciliation(existing, incoming_names, incoming_coords, set())

    assert plan.inserted == []
    assert plan.deleted == []
    pairs = {row.id: index for row, index in plan.updated}
    assert pairs == {"b": 0, "a": 1}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Table 1", "table 1"),
        ("Table 1-2", "table 1"),
        ("Table 1-2-3", "table 1"),
        ("a-1-b", "a-1-b"),
        ("-3", ""),
        ("", ""),
        (None, ""),
        # an all-digit name used to rsplit to itself and spin forever
        ("42", "42"),
        ("7-7", "7"),
    ],
)
def test_normalize_analysis_name_terminates(raw, expected):
    assert _normalize_analysis_name(raw) == expected
