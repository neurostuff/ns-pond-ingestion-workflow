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


#: Sentences from the papers sampled for PR-190-Data-Sources, and how each names its source.
RELATIONS = [
    ("The data used in this study were obtained from the OpenNeuro database with accession "
     "number of ds000030.", "openneuro", "used"),
    ("Neuroimaging data from this study has been uploaded to OpenNeuro. "
     "doi:10.18112/openneuro.ds004219.v1.0.0", "openneuro", "deposited"),
    ("Thresholded and unthresholded images are available on Neurovault: "
     "https://neurovault.org/collections/15045/.", "neurovault", "deposited"),
    ("Based on the ADNI protocol, high-resolution 3-dimensional T1-weighted images were "
     "acquired.", "adni", "borrowed"),
    ("The MRI scanning sequences were taken from the protocol developed by the Alzheimer's "
     "Disease Neuroimaging Initiative for use with scanners.", "adni", "borrowed"),
    ("The present study used the social cognition paradigm from the Human Connectome Project "
     "(HCP).", "hcp", "borrowed"),
    ("80 subjects were retrieved from the publicly available Human Connectome Project (HCP) "
     "dataset.", "hcp", "used"),
    ("Wholebrain t-maps for all reported contrasts can be found in a neurovault online "
     "repository.", "neurovault", "deposited"),
    ("ROIs were derived from an independent-component analysis of resting-state data from the "
     "Human Connectome Project.", "hcp", "borrowed"),
    ("We excluded the studies that only used online fMRI datasets such as ABIDE.", "abide",
     "mentioned"),
    ("A measurement of correlation strength was obtained from 961 independent subjects from the "
     "1000 Functional Connectome project using identical preprocessing steps.", "fcp_indi",
     "used"),
]


@pytest.mark.parametrize("sentence, data_source, status", RELATIONS)
def test_a_hit_s_sentence_says_how_the_paper_names_its_source(curated, sentence, data_source,
                                                              status):
    (match,) = curated.find(f"Methods\n{sentence}\n")["matches"]
    assert (match["data_source_id"], match["status"]) == (data_source, status)


def test_a_use_anywhere_in_the_used_sections_makes_the_match_used(curated):
    text = ("Methods\nData were obtained from the ADNI database.\n"
            "Scans followed the ADNI protocol.\n")
    (match,) = curated.find(text)["matches"]
    assert match["status"] == "used"
    assert sorted(e["relation"] for e in match["examples"]) == ["borrowed", "use"]


def test_a_name_inside_a_url_is_not_a_hit(curated):
    text = ("Methods\nMotion was inspected with ArtRepair "
            "(http://cibsr.stanford.edu/tools/human-brain-project/artrepair-software.html).\n")
    assert not curated.find(text)["matches"]


def test_a_funder_in_the_acknowledgments_is_mentioned(curated):
    text = "Methods\nx\nAcknowledgments\nFunded by AMED Brain/Minds Beyond (no. 19189621).\n"
    (match,) = curated.find(text)["matches"]
    assert (match["data_source_id"], match["status"]) == ("brain_minds_beyond", "mentioned")


def test_sections_are_read_from_markdown_and_numbered_headings():
    marks = sections("Title\n## 2. Materials and methods\nx\n### References\ny\n")
    assert [name for _, name in marks] == ["front", "methods", "references"]


def test_an_unknown_tier_is_refused():
    with pytest.raises(ValueError):
        Dictionary([("x", "X", "guess")])
