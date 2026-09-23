"""An extraction is judged by what it produced, not by whether it spoke.

55,140 pubget articles were recorded `failed` for the crime of having no
tables, which burns retry attempts on an answer that cannot change and buries
the nine that were genuinely malformed.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import ArticleRef, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable, Identifier
from ingestion_workflow.models.analysis import Coordinate
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import ExtractStage

NO_TABLES = "Pubget tables metadata not found; returning empty extraction."
PARTIAL = "Pubget skipped tables: table_001_info.json: [Errno 2] No such file"
BROKEN = "Premature end of data in tag unclosed line 292, column 25"


@pytest.fixture()
def stage(tmp_path):
    return ExtractStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))


@pytest.fixture()
def work():
    return Work(
        ref=ArticleRef(id="a1", identifier=Identifier(pmid="1")),
        source="pubget",
        fingerprint="fp",
    )


def content(*, text=None, tables=(), error=None):
    return ExtractedContent(
        slug="s",
        source=DownloadSource.PUBGET,
        full_text_path=text,
        tables=list(tables),
        error_message=error,
    )


def a_table(with_coords: bool):
    return ExtractedTable(
        table_id="t1",
        raw_content_path=None,
        coordinates=[Coordinate(x=1.0, y=2.0, z=3.0)] if with_coords else [],
    )


def test_no_tables_but_full_text_is_a_success(stage, work, tmp_path):
    outcome = stage._outcome(
        work, "pubget", content(text=tmp_path / "article.txt", error=NO_TABLES)
    )
    assert outcome.status is Status.OK
    assert outcome.summary["tables"] == 0
    assert outcome.summary["has_text"] is True


def test_the_reason_is_kept_as_a_note(stage, work, tmp_path):
    outcome = stage._outcome(work, "pubget", content(text=tmp_path / "a.txt", error=NO_TABLES))
    assert outcome.summary["notes"] == NO_TABLES


def test_a_partial_extraction_is_a_success(stage, work, tmp_path):
    """Four of five tables parsed is four tables more than failing gives."""
    outcome = stage._outcome(
        work, "pubget", content(text=tmp_path / "a.txt", tables=[a_table(True)], error=PARTIAL)
    )
    assert outcome.status is Status.OK
    assert outcome.summary["tables"] == 1
    assert outcome.summary["notes"] == PARTIAL


def test_an_extraction_that_produced_nothing_is_a_failure(stage, work):
    outcome = stage._outcome(work, "pubget", content(error=BROKEN))
    assert outcome.status is Status.FAILED
    assert outcome.error == BROKEN


def test_nothing_at_all_is_a_failure(stage, work):
    outcome = stage._outcome(work, "pubget", None)
    assert outcome.status is Status.FAILED
    assert "returned nothing" in outcome.error


def test_a_clean_extraction_carries_no_note(stage, work, tmp_path):
    outcome = stage._outcome(
        work, "pubget", content(text=tmp_path / "a.txt", tables=[a_table(True)])
    )
    assert outcome.status is Status.OK
    assert "notes" not in outcome.summary
    assert outcome.summary["tables_with_coordinates"] == 1


def test_tables_without_text_still_count_as_produced(stage, work):
    outcome = stage._outcome(work, "pubget", content(tables=[a_table(False)], error=PARTIAL))
    assert outcome.status is Status.OK
    assert outcome.summary["has_text"] is False
