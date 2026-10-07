"""A meta-analysis is uploaded as level 'meta', named so by its title."""

from __future__ import annotations

import pytest

from ingestion_workflow.services.study_level import is_meta, level_for


@pytest.mark.parametrize("title", [
    "Neural correlates of reward: a meta-analysis of fMRI studies",
    "A coordinate-based meta‐analysis of working memory",          # unicode hyphen
    "Brain activation in depression: metaanalysis and review",
    "Meta analysis of emotion regulation",
    "Meta-analyses of fear conditioning",
    "Meta-analytic connectivity modeling of the insula",
    "Grey matter in OCD: a systematic review",
    "A systematic literature review of neurofeedback",
    "Pain processing: an umbrella review",
    "An activation likelihood estimation study of language",
    "Voxel-based morphometry in bipolar disorder: seed-based d mapping",
    "Signed differential mapping of anxiety",
    "Meta-regression of age effects on default-mode connectivity",
])
def test_a_title_naming_a_meta_analysis_or_review_is_meta(title):
    assert is_meta(title) and level_for(title) == "meta"


@pytest.mark.parametrize("title", [
    "Metacognition and the prefrontal cortex",
    "A mega-analysis of resting-state data from 1,000 subjects",   # pooled raw data, a primary study
    "Neural basis of meta-memory judgements",
    "A review of the amygdala",                                    # not a systematic review
    "Activation of the striatum during reward anticipation",
    "",
    None,
])
def test_anything_else_is_a_group_study(title):
    assert not is_meta(title) and level_for(title) == "group"


def test_a_level_already_set_stands_unless_the_title_says_meta():
    assert level_for("Reward in the striatum", current="meta") == "meta"
    assert level_for("Reward in the striatum", current="group") == "group"
    assert level_for("Reward: a meta-analysis", current="group") == "meta"
    assert level_for("Reward in the striatum", current="junk") == "group"
