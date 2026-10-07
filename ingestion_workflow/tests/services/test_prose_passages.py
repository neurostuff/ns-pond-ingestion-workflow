"""The passage detector: the forms papers write coordinates in, and what is not one."""

from __future__ import annotations

import pytest

from ingestion_workflow.services.prose_passages import find, passages


def _xyz(sentence):
    return [(h.x, h.y, h.z) for h in find(sentence)]


@pytest.mark.parametrize("sentence, expected", [
    ("activation in the right IFG (x = 46, y = 20, z = 16; 57 voxels)", [(46, 20, 16)]),
    ("the left amygdala (MNI: -22, -4, -18)", [(-22, -4, -18)]),
    ("a peak at [-48 28 20] in the left IFG", [(-48, 28, 20)]),
    # statistics inside the brackets
    ("clusters in occipital cortex (−8, −96, 2, Z =9.15), paracingulate gyrus (−14, 48, 4, Z =5.27)",
     [(-8, -96, 2), (-14, 48, 4)]),
    ("bilateral insula (Left: -30, 22, -8, max z: 3.11, Right: 34, 24, -6, max z: 3.91)",
     [(-30, 22, -8), (34, 24, -6)]),
    ("VS/mOFC (t = 5.47; peak voxel, -6, 22, -8; P<0.0001)", [(-6, 22, -8)]),
    ("medial prefrontal cortex (−3, 53, −4; 1253; p < 0.001)", [(-3, 53, -4)]),
    ("centered at the voxel (tal, -4 -45 24, peak t value=5.3)", [(-4, -45, 24)]),
    # a minus written straight after a digit
    ("left ventral striatum (MNI -6–8 22)", [(-6, -8, 22)]),
    ("the ROIs that showed greater activity at 8/46/36 (MCC/dMPFC)", [(8, 46, 36)]),
])
def test_the_forms_papers_write(sentence, expected):
    assert _xyz(sentence) == expected


@pytest.mark.parametrize("sentence", [
    "These findings agree with earlier reports [ 12 , 14 , 23 ].",
    "Accuracy was 85% across blocks (70, 85, 92).",
    "References 12, 13, 14 and 15 were cited in the cortex.",
    "Sagittal slices at x = 30, y = 50, z = 16 are shown.",
    "Peak effects were described previously [ 24 , 32 – 34 ].",
    "most patients are state patients [ 4 ,  6 ,  27 ] in the cortex.",
    "reported different results ( 8 ,  17 ,  18 ,  24 ,  27 ,  29 ,  33 ) in the cortex",
    "temporal gyri (BA 20, 21, 22) as well as temporopolar cortex (BA 38)",
    "a multiband sequence (TR/TE = 1,000/34.0 ms, 66 slices, 2 mm voxels)",
    "VMPFC group: 0.37 ± 0.01, 95% CI = [0.34, 0.40] in the cortex",
])
def test_what_is_not_a_coordinate(sentence):
    assert _xyz(sentence) == []


def test_a_passage_keeps_its_heading_and_neighbours_and_drops_table_rows():
    text = (
        "## Results\n"
        "We compared the groups. Patients showed reduced activation than controls. "
        "The difference peaked in the ACC (x = 4, y = 30, z = 22; t = 3.9). It survived correction. "
        "Nothing else did.\n"
        "Region\tx\ty\tz\nACC\t4\t30\t22\n"
    )
    (p,) = passages(text)
    assert p.heading == "Results"
    assert [(h.x, h.y, h.z) for h in p.hits] == [(4, 30, 22)]
    assert p.text.startswith("Patients showed reduced activation")
    assert "We compared the groups." in p.before
    assert p.after == "Nothing else did."


def test_a_page_break_inside_a_triplet_is_closed_up():
    text = "Activity was found in the bilateral amygdala ([-21, -6,\n\n-27], T = 6.94). Nothing else."
    (p,) = passages(text)
    assert [(h.x, h.y, h.z) for h in p.hits] == [(-21, -6, -27)]
