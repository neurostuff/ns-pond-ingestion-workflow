"""The discovery query, against the real Neurostore table shapes.

Built on sqlite from the same ORM the upload stage uses, so the joins and the
column names are the ones production has.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.services.discover import unprocessed_base_studies
from ingestion_workflow.services.upload_models import (
    Base,
    BaseStudy,
    Study,
    StudysetStudy,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


@pytest.fixture()
def session():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def add(session, *, bs_id, level="group", doi=None, pmid=None, pmcid=None,
        studies=(), in_studyset=True):
    session.add(BaseStudy(id=bs_id, level=level, doi=doi, pmid=pmid, pmcid=pmcid))
    session.flush()
    for n, source in enumerate(studies):
        study_id = f"{bs_id}-s{n}"
        session.add(Study(id=study_id, base_study_id=bs_id, source=source))
        session.flush()
        if in_studyset:
            session.add(StudysetStudy(study_id=study_id, studyset_id="set-1"))
    session.flush()


def found(session):
    return {i.neurostore for i in unprocessed_base_studies(session)}


def test_picks_up_a_studyset_member_with_no_llm_study(session):
    add(session, bs_id="want", pmid="1", studies=["neurosynth"])
    assert found(session) == {"want"}


def test_skips_one_that_already_has_an_llm_study(session):
    add(session, bs_id="done", pmid="1", studies=["neurosynth", "llm"])
    assert found(session) == set()


def test_skips_one_that_is_in_no_studyset(session):
    add(session, bs_id="loose", pmid="1", studies=["neurosynth"], in_studyset=False)
    assert found(session) == set()


def test_skips_meta_analyses(session):
    add(session, bs_id="meta", level="meta", pmid="1", studies=["neurosynth"])
    assert found(session) == set()


def test_skips_one_with_no_downloadable_identifier(session):
    """Nothing any source could address, so registering it would create an
    article the pipeline can never act on."""
    add(session, bs_id="blank", studies=["neurosynth"])
    assert found(session) == set()


def test_carries_every_identifier_it_has(session):
    """The base_study_id alone is inert; the doi and pmid are what make the
    article downloadable."""
    add(session, bs_id="rich", doi="10.1/x", pmid="123", pmcid="PMC9",
        studies=["neurosynth"])
    (identifier,) = list(unprocessed_base_studies(session))
    assert identifier.neurostore == "rich"
    assert identifier.doi == "10.1/x"
    assert identifier.pmid == "123"
    assert identifier.pmcid == "PMC9"


def test_a_base_study_with_no_studies_at_all_is_not_in_a_studyset(session):
    add(session, bs_id="bare", pmid="1", studies=())
    assert found(session) == set()


def test_limit_caps_the_result(session):
    for n in range(5):
        add(session, bs_id=f"b{n}", pmid=str(n), studies=["neurosynth"])
    assert len(list(unprocessed_base_studies(session, limit=2))) == 2


def test_mixed_population(session):
    add(session, bs_id="a", pmid="1", studies=["neurosynth"])              # want
    add(session, bs_id="b", pmid="2", studies=["llm"])                     # done
    add(session, bs_id="c", level="meta", pmid="3", studies=["neurosynth"])  # meta
    add(session, bs_id="d", pmid="4", studies=["neurosynth"], in_studyset=False)
    add(session, bs_id="e", doi="10.1/e", studies=["ace", "neurosynth"])   # want
    assert found(session) == {"a", "e"}


def test_a_discovered_article_is_actionable(session, tmp_path):
    """The point of carrying the doi and pmid: unlike a bare base_study_id, a
    discovered article has a handle some download source can use."""
    from ingestion_workflow.catalog import Catalog
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline import Context
    from ingestion_workflow.pipeline.stages import DownloadStage

    add(session, bs_id="rich", doi="10.1/x", pmid="123", pmcid="PMC9",
        studies=["neurosynth"])
    discovered = list(unprocessed_base_studies(session))

    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k"
    )
    with Catalog.open(settings.catalog_root) as catalog:
        refs = catalog.register_many(discovered)
        stage = DownloadStage(settings)
        plan = stage.plan(Context(settings, catalog), refs, {}, {})

        assert len(plan.pending) == 1, "a discovered article should be downloadable"
        assert plan.skipped == 0
        # and the Neurostore id came along as an alias
        assert catalog.identifier(refs[0].id).neurostore == "rich"
