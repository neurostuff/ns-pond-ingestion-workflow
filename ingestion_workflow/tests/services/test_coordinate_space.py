"""Reading a coordinate space from an article's own words.

Most sentences here are taken, lightly trimmed, from corpus articles the
reader once got wrong.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.models import CoordinateSpace
from ingestion_workflow.services.coordinate_space import read_space, sectionize, space_in

MNI, TAL = CoordinateSpace.MNI, CoordinateSpace.TALAIRACH


def _space(sentence):
    reading = space_in(sentence)
    return reading.space if reading else None


@pytest.mark.parametrize("sentence, expected", [
    ("Coordinates are reported in Talairach space.", TAL),
    ("All figures and extracted coordinates were reported in MNI space.", MNI),
    ("Images were spatially normalized to the MNI template.", MNI),
    ("Data were normalised into Tailarach space.", TAL),
    ("MNI coordinates were converted to Talairach space using mni2tal.", TAL),
    ("Peaks were converted with tal2icbm.", MNI),
    ("x, y, z: Talairach coordinates; T: T value.", TAL),
    ("The subcortical ROIs included the putamen (MNI coordinates: 28, 5, 2).", MNI),
    ("Talairach coordinates were reported into MNI space before the meta-analysis.", MNI),
    ("Peaks in TAL coordinates.", TAL),
])
def test_a_sentence_that_states_a_space(sentence, expected):
    assert _space(sentence) is expected


@pytest.mark.parametrize("sentence", [
    "MNI coordinates were converted to Talairach space for anatomical labeling.",
    "MNI coordinates were transformed into Talairach space to determine Brodmann areas.",
    "Brain locations are reported as x, y, z coordinates in MNI space with Brodmann areas "
    "identified by mathematical transformation into Talairach space.",
    "Anatomical labels were made by Talairach Daemon, after a nonlinear coordinate "
    "transformation from MNI to Talairach coordinates.",
])
def test_a_conversion_made_to_find_a_label_leaves_the_coordinates_where_they_were(sentence):
    assert _space(sentence) is MNI


def test_a_conversion_followed_by_a_separate_labelling_step_still_converts():
    """"and regions were localized" is the next step, not the conversion's purpose."""
    assert _space(
        "The peak MNI co-ordinates were converted to Talairach space, and regions were "
        "localized in reference to an atlas (Talairach & Tournoux, 1988)."
    ) is TAL


def test_a_report_that_is_the_condition_of_a_conversion_is_not_the_answer():
    assert _space(
        "When activations were reported in Talairach and Tournoux coordinates, they "
        "were transformed into MNI space using a nonlinear transformation."
    ) is MNI
    assert _space(
        "The Lancaster transformation (icbm2tal) was used to transform studies from MNI "
        "into Talairach space when the coordinates were reported in MNI space."
    ) is TAL


@pytest.mark.parametrize("sentence", [
    # FreeSurfer's affine step, whether or not FreeSurfer is named.
    "FreeSurfer recon-all included transformation to Talairach space and segmentation.",
    "The processing steps included removal of non-brain tissue, transformation to "
    "Talairach space, tessellation of the GM-WM boundary, automated topology correction.",
    "Processing includes a hybrid watershed/surface deformation procedure, automated "
    "Talairach transformation, and segmentation.",
    # A meta-analysis's inclusion criterion lists both.
    "Studies were included if they reported standardized MNI or Talairach coordinates.",
    "The results were reported in Talairach/Tournoux or in Montreal Neurological "
    "Institute (MNI) coordinates;",
    # The institute as a place, and tissue priors.
    "Images were acquired at the Montreal Neurological Institute (MNI), Brain Imaging Center.",
    "The T1 images were transformed to the ICBM tissue probabilistic atlases.",
    # A labelling conversion that names no source.
    "Maxima were transformed into Talairach space, anatomical labelling was performed "
    "using the Talairach Demon database.",
    # Words that only look like a space.
    "We ran the OMNIBUS test.",
])
def test_a_sentence_that_names_no_reporting_space(sentence):
    assert space_in(sentence) is None


def test_two_spaces_at_the_deciding_rank_are_no_answer():
    assert space_in(
        "Coordinates for experiment 1 are reported in MNI space. "
        "Coordinates for experiment 2 are reported in Talairach space."
    ) is None


def test_a_statement_outranks_a_mention():
    reading = space_in(
        "Labels came from the Talairach and Tournoux atlas. "
        "Images were normalized to the MNI template."
    )
    assert (reading.space, reading.rule) == (MNI, "normalised")


# -- sections ---------------------------------------------------------------

def test_markdown_headings_are_sections():
    text = "Title\n\n## Introduction\nx\n\n## Methods\n### Participants\ny\n\n## Results\nz\n"
    assert [label for _, _, label in sectionize(text)] == ["front", "intro", "methods", "results"]


def test_a_journal_with_no_methods_heading_still_has_methods():
    """Some journals open straight onto `## Participants`."""
    text = "## Introduction\nx\n## Participants\ny\n## Results\nz\n"
    assert [label for _, _, label in sectionize(text)] == ["intro", "methods", "results"]


def test_a_methods_like_subsection_inside_results_stays_results():
    text = "## Results\n### Task performance\nz\n## Discussion\nd\n"
    assert [label for _, _, label in sectionize(text)] == ["results", "discussion"]


def test_bare_line_headings_are_sections_when_there_is_no_markdown():
    """ACE and PDF text: a heading is a line of its own."""
    text = ("INTRODUCTION\nx\nMATERIALS AND METHODS\nParticipants\ny\n"
            "2. Results\nz\nREFERENCES\nr\n")
    assert [label for _, _, label in sectionize(text)] == [
        "intro", "methods", "results", "back"]


def test_text_with_no_headings_is_one_unknown_span():
    assert sectionize("just prose") == [(0, 10, "unknown")]


# -- the article ------------------------------------------------------------

ARTICLE = """A study
Montreal Neurological Institute, McGill University, Canada

## Introduction
Earlier work reported foci in Talairach space.

## Methods
Images were normalized to the MNI template.

## Results
Activation was found.

## References
Talairach J, Tournoux P (1988).
"""


def test_only_the_methods_and_results_are_read():
    reading = read_space(ARTICLE)
    assert (reading.space, reading.where) == (MNI, "methods")


def test_the_tables_own_caption_comes_first():
    reading = read_space(ARTICLE, footer="x, y, z: Talairach coordinates.")
    assert (reading.space, reading.where) == (TAL, "table")


def test_an_ambiguous_methods_section_is_not_outvoted_by_the_results():
    text = (
        "## Methods\nExperiment 1 coordinates are reported in MNI space. "
        "Experiment 2 coordinates are reported in Talairach space.\n\n"
        "## Results\nPeaks are reported in MNI space.\n"
    )
    assert read_space(text) is None


def test_an_article_that_never_names_a_space_has_none():
    assert read_space("## Methods\nWe sampled oak trees at 40 localities.\n") is None
    assert read_space(None) is None
