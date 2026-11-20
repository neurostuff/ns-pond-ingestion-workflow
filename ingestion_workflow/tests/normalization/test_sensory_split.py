"""tct_sensory_stimulation split by sense, and perceptual_judgment's tasks of one sense in that sense's family.

The old task held touch, acupuncture, smell, taste and visceral stimulation (on Neurostore, 2026-11-19: 1,867 tactile,
761 olfactory, 329 gustatory and >= 508 acupuncture analyses of its 3,001), so a construct linked to it reached every
sense. The expected labels below are the split's specification, not read off the code.
"""
import json

import numpy as np
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


# Task names and descriptions from ns-pond records that the first split (fit-4d612236d8f6,
# 2026-11-19) left as tct_sensory_stimulation, so the contrast route's bodily_sensation call
# dropped their task.
TOUCH = "tct_touch_stimulation"
UNNAMED = "tct_sensory_stimulation"


@pytest.mark.parametrize(
    "name, evidence, task",
    [
        ("median-nerve electrical stimulation",
         "Right median nerve stimulation at the wrist at 3 and 10 Hz", TOUCH),
        ("Right (injured) hindpaw stimulation task",
         "Electrical stimulation of the right injured hindpaw during fMRI", TOUCH),
        ("Electrical stimulation of the forepaw",
         "Left forepaw electrical stimulation in a block design", TOUCH),
        ("Left-Side (Unaffected Leg) Stimulation",
         "Electrical stimulation of the unaffected hind leg", TOUCH),
        ("fMRI electrical finger stimulation", "D1 and D5 of both hands were stimulated", TOUCH),
        ("Mechanical stimulation",
         "punctate stimulation of the shaved right whisker pad with a nylon filament", TOUCH),
        ("Mechanical stimulation",
         "mechanical hindpaw stimulation at the von Frey withdrawal threshold", TOUCH),
        # a painful stimulation is pain stimulation, not touch, whatever else it names
        ("stimulation", "painful heat and pressure on the forearm", "tct_pain_stimulation"),
        # stimulation of the brain or a cranial nerve names no sense
        ("Ventral versus dorsal STN-DBS",
         "deep brain stimulation of the subthalamic nucleus", UNNAMED),
        ("acute VNS", "vagus nerve stimulation in an off-on-off-on sequence", UNNAMED),
        ("iTBS", "intermittent theta burst stimulation, active and sham", UNNAMED),
        ("electrical stimulation", "electrical stimulation blocks alternating with rest", UNNAMED),
    ],
)
def test_somatosensory_stimulation_wording_reaches_touch(name, evidence, task):
    assert split_sensory(UNNAMED, name, evidence) == task


def test_other_labels_pass_through():
    assert split_sensory("hedtsk_n_back", "odor needle taste") == "hedtsk_n_back"
    assert split_sensory(None, "odor") is None


class _Says:
    """A contrast classifier that calls every text `family`."""

    def __init__(self, family):
        self.classes_ = [family]

    def transform(self, texts):
        return texts

    def predict_proba(self, X):
        return np.ones((len(X), 1))


def _says(family):
    says = _Says(family)
    return (says, says)


def _task(name, evidence, canonical=None, route="B-low"):
    """A pass-one task row; `canonical` is taskcat's label."""
    return {"study": "s", "task": "t1", "name": name, "entity_kind": "task", "route": route,
            "canonical": canonical, "evidence": evidence}


def test_the_contrast_classifier_learns_the_families_before_the_sense_split():
    rows = [{"route": "A:name", "family": f, "evidence": e} for f, e in
            [("visual_perception", "faces houses checkerboard flicker")] * 3
            + [("auditory_perception", "tones noise bursts listening")] * 3
            + [("language", "sentences words reading semantic")] * 3]
    _vec, clf = labels.fit_contrast_classifier(rows)
    assert set(clf.classes_) == {"perceptual_judgment", "language"}


def test_route_b_sensory_label_keeps_its_canonical_under_the_contrast_route():
    # taskcat's tct_sensory_stimulation, split by the smell its evidence names, agrees with a
    # perceptual call
    t = _task("stimulation runs", "pleasant odorant vs odorless air, sniffing", UNNAMED, "B")
    got = labels.decide(t, {}, Lexicon(), _says("perceptual_judgment"))
    assert (got["canonical"], got["family"], got["decided_by"]) == (
        "tct_olfactory_stimulation", "chemosensation", "contrast")


@pytest.mark.parametrize("canonical, family", [
    ("hedtsk_random_dot_kinematogram", "visual_perception"),
    ("tct_perceptual_decision", "perceptual_judgment"),
    ("tct_dichotic_listening", "auditory_perception"),
])
def test_a_perceptual_call_keeps_taskcats_task_of_any_sense_family(canonical, family):
    # one classifier class for every sense: a perceptual task keeps its own task and family
    t = _task("Experiment 2", "block design with two conditions", canonical)
    got = labels.decide(t, {}, Lexicon(), _says("perceptual_judgment"))
    assert (got["canonical"], got["family"], got["decided_by"]) == (canonical, family, "contrast")


@pytest.mark.parametrize(
    "name, task, family",
    [
        ("Touch", TOUCH, "bodily_sensation"),
        ("Von Frey filament stimulation", TOUCH, "bodily_sensation"),
        # a sense in the evidence alone is not enough: the classifier's class alone, as before
        ("sensory task", None, "perceptual_judgment"),
    ],
)
def test_the_contrast_route_takes_the_sense_its_name_names(name, task, family):
    t = _task(name, "pneumatic tactile stimulation of each hand, vibration")
    got = labels.decide(t, {}, Lexicon(), _says("perceptual_judgment"))
    assert (got["canonical"], got["family"], got["decided_by"]) == (task, family, "contrast")


def test_a_sense_outside_the_classifiers_family_gives_no_task():
    # a name that names smell, read as emotion: the family stands, the smell task does not
    t = _task("Chemosensory stimulation", "odorants given monorhinally to the nostril")
    got = labels.decide(t, {}, Lexicon(), _says("emotion"))
    assert (got["canonical"], got["family"], got["decided_by"]) == (None, "emotion", "contrast")


def test_inventory_and_lexicon_agree_on_every_family(lexicon):
    inv = json.load(open(INVENTORY))
    assert {t["family"] for t in inv["tasks"]} == set(inv["families"])
    assert "ingestive_behavior" not in inv["families"]
    assert inv["n_tasks"] == len(inv["tasks"])
    assert {t["id"]: lexicon.family[t["id"]][0] for t in inv["tasks"]} == {t["id"]: t["family"] for t in inv["tasks"]}


@pytest.mark.parametrize("family", ["visual_perception", "auditory_perception", "bodily_sensation", "chemosensation"])
def test_a_sense_family_keeps_perceptions_intensity_axis(family):
    assert normalize_condition_ctx("high", family=family)["atoms"] == ["intensity:high"]
