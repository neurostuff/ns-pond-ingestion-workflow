"""`--select errors[:<field>]`: the articles kept despite an error, to redo on request."""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Notes, Outcome
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Select, everything, narrow, parse_select


@pytest.fixture()
def cat(tmp_path, evidence):
    with Catalog.open(tmp_path / "cat") as cat:
        refs = cat.register_many([Identifier(pmid=str(n)) for n in range(4)], evidence)
        authors, licence = Notes(), Notes()
        authors.error(("authors", "affiliations"), "authors: ValueError: odd")
        licence.error("license", "crossref did not answer", transient=True)
        cat.record([
            Outcome(article_id=refs[0].id, stage="metadata", summary=authors.summary()),
            Outcome(article_id=refs[1].id, stage="metadata", summary=licence.summary()),
            Outcome(article_id=refs[2].id, stage="metadata", summary={"missing": ["abstract"]}),
            Outcome.failure(refs[3].id, "metadata", "", "catalog conflict",
                            summary=authors.summary()),
        ])
        yield cat, refs


@pytest.mark.parametrize("text, wanted", [
    ("errors", {0, 1}),
    ("errors:license", {1}),
    ("errors:affiliations", {0}),
    ("errors:abstract", set()),  # missing, but no error left it out
])
def test_errors_selects_the_kept_records_noting_one(cat, text, wanted):
    cat, refs = cat
    mode, field = parse_select(text)
    got = narrow(cat, everything(cat), mode, "metadata", field=field)
    assert {refs.index(r) for r in got.refs} == wanted


@pytest.mark.parametrize("text", ["failed:license", "errs"])
def test_a_field_is_taken_only_by_errors(text):
    with pytest.raises(ValueError):
        parse_select(text)


def test_select_without_a_field():
    assert parse_select("pending") == (Select.PENDING, None)
