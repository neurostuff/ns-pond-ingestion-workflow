import pytest

from ingestion_workflow.extractors.utils import coordinate_space_from_guess
from ingestion_workflow.models.analysis import CoordinateSpace
from ingestion_workflow.services.nuextract_payload import _space


@pytest.mark.parametrize(
    "label,expected",
    [
        ("MNI", CoordinateSpace.MNI),
        ("MNI152 2mm", CoordinateSpace.MNI),
        ("TAL", CoordinateSpace.TALAIRACH),
        ("TALAIRACH", CoordinateSpace.TALAIRACH),
        ("Talairach & Tournoux 1988", CoordinateSpace.TALAIRACH),
        ("TT88", CoordinateSpace.TALAIRACH),
        ("fsaverage", CoordinateSpace.OTHER),
        ("mni2tal", None),
        ("UNKNOWN", None),
        ("", None),
        (None, None),
    ],
)
def test_a_label_reads_through_study_schemas_alias_table(label, expected):
    assert CoordinateSpace.from_label(label) is expected


def test_an_unstated_guess_is_null_in_the_extractors():
    assert coordinate_space_from_guess("UNKNOWN") is None
    assert coordinate_space_from_guess("Talairach") is CoordinateSpace.TALAIRACH


def test_the_nuextract_space_keeps_null_for_no_space():
    assert _space("Talairach") == "TAL"
    assert _space(None) is None
    assert _space("n/a") is None
