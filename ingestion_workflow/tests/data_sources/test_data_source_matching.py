"""Each alias tier counts a mention only on the evidence it asks for.

The paper is Yang et al. 2022 (Front. Hum. Neurosci., PMID 36590062, CC BY), as pubget's
reader gives its text: its Methods name ADNI, and so does a heading of its back matter.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from ingestion_workflow.data_sources.matching import Dictionary, sections
from ingestion_workflow.data_sources.merge import curated_records, merge

PAPER = Path(__file__).parents[1] / "data" / "data_sources" / "36590062.txt"


@pytest.fixture(scope="module")
def curated():
    return Dictionary(merge(curated_records())[1])


def test_a_paper_names_the_data_source_it_used(curated):
    found = curated.find(PAPER.read_text(encoding="utf-8"))
    assert found["sectioned"]
    (adni,) = found["matches"]
    assert (adni["data_source_id"], adni["status"]) == ("adni", "used")
    assert {"methods"} <= adni["sections"].keys()
    assert "Alzheimer's Disease Neuroimaging Initiative" in adni["forms"]
    first = adni["examples"][0]
    text = PAPER.read_text(encoding="utf-8")
    assert text[first["start"]:first["end"]] == first["form"]


def test_a_context_acronym_needs_a_dataset_word_nearby(curated):
    assert not curated.find("Methods\nWe thank DIAN.\n")["matches"]
    found = curated.find("Methods\nWe used DIAN data.\n")["matches"]
    assert [(m["data_source_id"], m["status"]) for m in found] == [("dian", "used")]


def test_a_defined_acronym_needs_the_full_name_outside_the_references():
    names = Dictionary([("oasis", "Open Access Series of Imaging Studies", "name"),
                        ("oasis", "OASIS", "defined")])
    assert not names.find("Methods\nOASIS scans were used.\n")["matches"]
    alone = "Methods\nOASIS scans were used.\nReferences\nOpen Access Series of Imaging Studies\n"
    assert not [m for m in names.find(alone)["matches"] if "OASIS" in m["forms"]]
    found = names.find("Methods\nThe Open Access Series of Imaging Studies (OASIS).\n")
    assert found["matches"][0]["n_hits"] == 2


def test_a_qualified_name_needs_a_dataset_word_or_an_accession():
    names = Dictionary([("sherlock", "Sherlock", "qualified")])
    assert not names.find("Methods\nSherlock Holmes was read aloud.\n")["matches"]
    assert names.find("Methods\nThe Sherlock movie-watching data were used.\n")["matches"]
    assert names.find("Methods\nSherlock (ds001132) was used.\n")["matches"]


def test_lower_case_prose_is_not_a_full_name():
    names = Dictionary([("abcd", "Adolescent Brain Cognitive Development", "name")])
    assert not names.find("Methods\nadolescent brain cognitive development is slow.\n")["matches"]


def test_sections_are_read_from_markdown_and_numbered_headings():
    marks = sections("Title\n## 2. Materials and methods\nx\n### References\ny\n")
    assert [name for _, name in marks] == ["front", "methods", "references"]


def test_an_unknown_tier_is_refused():
    with pytest.raises(ValueError):
        Dictionary([("x", "X", "guess")])
