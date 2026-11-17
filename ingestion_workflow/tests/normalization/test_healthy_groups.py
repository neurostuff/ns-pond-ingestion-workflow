"""A group the model called healthy, on four production records' 27B groups
(`data/healthy_groups.json`).

7c47sgesb5wl (base study mSWczX8uDxiF, "Physically and psychologically healthy") and xd2xqhulniyq
(gNwTN4t6cPre, "Neurotypical") are healthy cohorts that pondie's wellness phrases do not cover;
uw6hfyq36rx2 (6odDzJ92sgPo) and ntuf47r2lzgf (ihRjXA2aAqD9) have a control group the model called
healthy beside a comorbidity in some of its members. `ROWS` are the linker's rows for them, as it
placed them against MONDO on beast (2026-11-12) with no paper text.
"""

import copy
import json
from pathlib import Path

import pytest

pytest.importorskip("pondie")

from ingestion_workflow.normalization import medical, sidecars  # noqa: E402
from ingestion_workflow.services.normalize_assign import apply_derived  # noqa: E402
from pondie.normalization import apply_derived as pondie_apply_derived  # noqa: E402
from pondie.normalization import medical_condition as mc  # noqa: E402

DATA = json.loads((Path(__file__).parent / "data" / "healthy_groups.json").read_text())
FIT = "fit-000000000000"
#: (article, group, item, text, head, mondo, method)
ROWS = [
    ("7c47sgesb5wl", "grp_bilinguals", 0, "Physically and psychologically healthy",
     "Physically and psychologically healthy", "", "rejected"),
    ("7c47sgesb5wl", "grp_monolinguals", 0, "Physically and psychologically healthy",
     "Physically and psychologically healthy", "", "rejected"),
    ("xd2xqhulniyq", "grp_young", 0, "Neurotypical", "Neurotypical", "", "rejected"),
    ("xd2xqhulniyq", "grp_old", 0, "Neurotypical", "Neurotypical", "", "rejected"),
    ("uw6hfyq36rx2", "grp_cn", 0, "Hyperlipidemia in 3 participants", "Hyperlipidemia in 3",
     "MONDO:0021187", "strip:lexical"),
    ("uw6hfyq36rx2", "grp_cn", 1, "Hypertension in 3 participants", "Hypertension in 3", "",
     "strip:review"),
    ("ntuf47r2lzgf", "grp_hc", 0, "Hypertension in 1/33", "Hypertension in 1", "", "strip:review"),
    ("ntuf47r2lzgf", "grp_hc", 0, "Hypertension in 1/33", "33", "", "rejected"),
]
CONTROLS = {"uw6hfyq36rx2": "grp_cn", "ntuf47r2lzgf": "grp_hc"}


def derived(article, apply=apply_derived):
    record = {"groups": copy.deepcopy(DATA[article]["groups"])}
    apply(record)
    return record


def healthy(record):
    return {g["local_id"]: g.get("is_healthy", {}).get("value") for g in record["groups"]}


def rows(article):
    return [mc.Link(study=a, group=g, text=t, item=i, head=h, mondo=m, method=how)
            for a, g, i, t, h, m, how in ROWS if a == article]


@pytest.mark.parametrize("article", sorted(DATA))
def test_pondie_alone_overrules_the_model_s_healthy_answer(article):
    model = healthy({"groups": DATA[article]["groups"]})
    pondie = healthy(derived(article, pondie_apply_derived))
    overruled = {g for g, v in model.items() if v is True and pondie[g] is False}
    assert overruled == ({CONTROLS[article]} if article in CONTROLS else set(model))


@pytest.mark.parametrize("article", sorted(DATA))
def test_the_model_s_healthy_answer_is_held_for_the_medical_step(article):
    model = healthy({"groups": DATA[article]["groups"]})
    held = healthy(derived(article))
    assert {g for g, v in held.items() if v} == {g for g, v in model.items() if v}


@pytest.mark.parametrize("article", ["7c47sgesb5wl", "xd2xqhulniyq"])
def test_a_healthy_group_whose_heads_nothing_resembles_stays_healthy(article):
    record = derived(article)
    settled = medical.settle_healthy(record, rows(article))
    assert set(healthy(record).values()) == {True}
    assert [(x.sentinel, x.scope, x.head) for x in settled] == [("NO_CONDITION", "flag", "")] * 2
    sidecars.write_medical(record, settled, FIT)
    for group in record["groups"]:
        written = [(e["vocabulary"], e["status"], e.get("method") or e.get("value"))
                   for e in group["medical_condition"]["normalized"]]
        assert written == [("MONDO", "absent", "absent by flag"),
                           ("astra medical role", "mapped", "none")]


@pytest.mark.parametrize("article", sorted(CONTROLS))
def test_a_healthy_group_with_a_placed_or_reviewed_head_has_a_condition(article):
    record = derived(article)
    given = rows(article)
    assert medical.settle_healthy(record, given) == given
    assert healthy(record)[CONTROLS[article]] is False
    assert healthy(record) == healthy(derived(article, pondie_apply_derived))


@pytest.fixture(scope="module")
def mondo():
    try:
        return medical.vocabularies()
    except Exception as exc:  # noqa: BLE001 - no MONDO release or encoder on this host
        pytest.skip(f"MONDO is not loadable here: {exc}")


@pytest.mark.parametrize("article", sorted(DATA))
def test_the_linker_places_the_groups_heads_as_recorded(article, mondo):
    recorded = [(g, i, h, m, how) for a, g, i, _t, h, m, how in ROWS if a == article]
    groups = {g for g, *_ in recorded}
    got = medical.links(derived(article), article, "")
    assert [(x.group, x.item, x.head, x.mondo, x.method) for x in got
            if x.head and x.group in groups] == recorded
