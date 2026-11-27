"""`medical.links` spells out the paper's own abbreviations on the whole item before triage reads it.

Before, triage split the item first and each head was expanded alone, so a short form the paper defined with a
slash ("(AD/HD)") was split into `AD` and `HD`, which MONDO reads as Alzheimer disease and Huntington disease.
"""

import pytest

pytest.importorskip("pondie")

from ingestion_workflow.normalization import medical  # noqa: E402

TEXT = ("We recruited children with attention-deficit/hyperactivity disorder (AD/HD), adults with "
        "posttraumatic stress disorder (PTSD), and healthy controls (HC).")


@pytest.fixture(scope="module")
def mondo():
    try:
        return medical.vocabularies()
    except Exception as exc:  # noqa: BLE001 - no MONDO release or encoder on this host
        pytest.skip(f"MONDO is not loadable here: {exc}")


def record(*items):
    return {"groups": [{"local_id": {"value": f"g{i}"}, "is_healthy": {"value": False},
                        "medical_condition": {"value": [item]}} for i, item in enumerate(items)]}


def test_a_short_form_the_paper_defined_is_spelled_out_before_the_split(mondo):
    got = medical.links(record("AD/HD in 46% of patients", "PTSD", "HC"), "p1", TEXT)
    assert sorted((x.group, x.head, x.label, x.sentinel) for x in got) == [
        ("g0", "attention-deficit/hyperactivity disorder", "attention deficit-hyperactivity disorder", ""),
        ("g1", "posttraumatic stress disorder", "post-traumatic stress disorder", ""),
        ("g2", "", "", "NO_CONDITION"),
    ]
    (adhd,) = [x for x in got if x.group == "g0"]
    assert adhd.text == "AD/HD in 46% of patients" and adhd.expansions


def test_without_the_paper_s_text_nothing_is_expanded(mondo):
    (got,) = [x for x in medical.links(record("PTSD"), "p1", "") if x.head]
    assert (got.head, got.label, got.expansions) == ("PTSD", "post-traumatic stress disorder", ())
