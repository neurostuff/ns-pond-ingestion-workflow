"""tct_sensory_stimulation split by sense, and perceptual_judgment's tasks of one sense in that sense's family.

The old task held touch, acupuncture, smell, taste and visceral stimulation (on Neurostore, 2026-11-19: 1,867 tactile,
761 olfactory, 329 gustatory and >= 508 acupuncture analyses of its 3,001), so a construct linked to it reached every
sense. The expected labels below are the split's specification, not read off the code.
"""
import json

import pytest
from ingestion_workflow.normalization import labels
from ingestion_workflow.normalization.records import INVENTORY
from ingestion_workflow.normalization.taskcat.conditions import normalize_condition_ctx
from ingestion_workflow.normalization.taskcat.lexicon import Lexicon, split_sensory


@pytest.fixture(scope="module")
def lexicon():
    return Lexicon()


@pytest.mark.parametrize(
    "name, task, family",
    [
        ("acupuncture stimulation", "tct_acupuncture_stimulation", "bodily_sensation"),
        ("electroacupuncture", "tct_acupuncture_stimulation", "bodily_sensation"),
        ("tactile stimulation", "tct_touch_stimulation", "bodily_sensation"),
        ("median nerve stimulation", "tct_touch_stimulation", "bodily_sensation"),
        ("affective touch", "tct_touch_stimulation", "bodily_sensation"),
        ("rectal distension", "tct_visceral_stimulation", "bodily_sensation"),
        ("olfactory stimulation", "tct_olfactory_stimulation", "chemosensation"),
        ("odor presentation", "tct_olfactory_stimulation", "chemosensation"),
        ("taste stimulation", "tct_gustatory_stimulation", "chemosensation"),
        ("sucrose delivery", "tct_gustatory_stimulation", "chemosensation"),
        # BrainMap class names the crosswalk now points at the sense
        ("Olfactory Monitor/Discrimination", "tct_olfactory_stimulation", "chemosensation"),
        ("Acupuncture", "tct_acupuncture_stimulation", "bodily_sensation"),
        # unchanged tasks, in their new families
        ("taste test", "tct_food_intake", "chemosensation"),
        ("painful heat stimulation", "tct_pain_stimulation", "bodily_sensation"),
        ("vestibular stimulation", "tct_bm_vestibular_stimulation", "bodily_sensation"),
        ("dichotic listening", "tct_dichotic_listening", "auditory_perception"),
        ("passive viewing", "tct_bm_passive_viewing", "visual_perception"),
        ("random dot kinematogram", "hedtsk_random_dot_kinematogram", "visual_perception"),
        # a stimulation that names no sense stays the unspecified task
        ("sensory stimulation", "tct_sensory_stimulation", "perceptual_judgment"),
    ],
)
def test_a_name_reaches_its_senses_task_and_family(lexicon, name, task, family):
    assert lexicon.match(name)[0] == task
    assert lexicon.family[task] == [family]
    assert labels.FAM[task] == family


@pytest.mark.parametrize(
    "evidence, task",
    [
        ("real needle vs sham at ST36 with de qi ratings", "tct_acupuncture_stimulation"),
        ("vanillin and hydrogen sulfide odorants, sniffing cue", "tct_olfactory_stimulation"),
        ("sweet (sucrose) and bitter (quinine) solutions", "tct_gustatory_stimulation"),
        ("balloon distension of the rectum at 40 mmHg", "tct_visceral_stimulation"),
        ("brushing of the forearm; vibration at 130 Hz", "tct_touch_stimulation"),
        ("block 1; block 2; baseline", "tct_sensory_stimulation"),
    ],
)
def test_a_sensory_label_takes_the_sense_its_evidence_names(evidence, task):
    assert split_sensory("tct_sensory_stimulation", "stimulation", evidence) == task


def test_other_labels_pass_through():
    assert split_sensory("hedtsk_n_back", "odor needle taste") == "hedtsk_n_back"
    assert split_sensory(None, "odor") is None


def test_route_b_sensory_label_keeps_its_canonical_under_the_contrast_route():
    # the contrast route keeps taskcat's canonical only when it is in the classifier's family: without the split,
    # tct_sensory_stimulation (perceptual_judgment) never agrees with a chemosensation call and the label is lost
    smell = "odorant odor smell olfactory sniffing"
    touch = "tactile brushing vibration touch forearm"
    named = [{"route": "A:name", "family": f, "evidence": e} for f, e in
             [("chemosensation", smell)] * 3 + [("bodily_sensation", touch)] * 3]
    classifier = labels.fit_contrast_classifier(named)
    t = {"study": "s", "task": "t1", "name": "stimulation runs", "entity_kind": "task", "route": "B",
         "canonical": "tct_sensory_stimulation", "evidence": "pleasant odorant vs odorless air, sniffing"}
    got = labels.decide(t, {}, Lexicon(), classifier)
    assert (got["canonical"], got["family"], got["decided_by"]) == (
        "tct_olfactory_stimulation", "chemosensation", "contrast")


def test_inventory_and_lexicon_agree_on_every_family(lexicon):
    inv = json.load(open(INVENTORY))
    assert {t["family"] for t in inv["tasks"]} == set(inv["families"])
    assert "ingestive_behavior" not in inv["families"]
    assert inv["n_tasks"] == len(inv["tasks"])
    assert {t["id"]: lexicon.family[t["id"]][0] for t in inv["tasks"]} == {t["id"]: t["family"] for t in inv["tasks"]}


@pytest.mark.parametrize("family", ["visual_perception", "auditory_perception", "bodily_sensation", "chemosensation"])
def test_a_sense_family_keeps_perceptions_intensity_axis(family):
    assert normalize_condition_ctx("high", family=family)["atoms"] == ["intensity:high"]
