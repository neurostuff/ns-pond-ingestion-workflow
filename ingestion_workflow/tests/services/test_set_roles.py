"""The set-role classifier's input and decision rule, without a model."""

from __future__ import annotations

from ingestion_workflow.prompts.prose_coordinates import ROLES, study_schema_role
from ingestion_workflow.services.set_roles import (
    ANCHOR_KINDS,
    COORDINATE_ROLES,
    Prediction,
    ProseSetContext,
    SetRole,
    UPLOADED_ROLES,
    decide,
    prose_context,
    table_context,
)
from ingestion_workflow.services.set_roles.common import cue_summary, point_summary
from ingestion_workflow.services.set_roles.labels import RESULT
from study_schema.models.paper_parse import AnchorKind, CoordinateRole

SEED = SetRole("anchor", "seed")

ARTICLE = (
    "Seeds were placed in both amygdalae. As shown in Table 2, seeds were placed bilaterally. "
    "Table 3 lists the results."
)


def _table_analysis(points, caption="Regions of interest used for the PPI analysis"):
    return {
        "name": "ROIs for PPI",
        "description": None,
        "table_caption": caption,
        "table_footer": "",
        "metadata": {"table_metadata": {"table_label": "Table 2"}},
        "coordinates": points,
    }


def _point(x, y, z, **extra):
    return {"x": x, "y": y, "z": z, **extra}


TABLE = """#Region | #x | #y | #z | #t
Amygdala L | -24 | -4 | -18 |
Amygdala R | 24 | -4 | -18 |
Insula | 38 | 12 | 2 | 5.1"""


def test_short_fields_come_first_and_truncation_keeps_them():
    context = ProseSetContext(
        name="amygdala seed",
        passage="word " * 2000,
        points=[_point(20, -4, -18)],
        proposed=SEED,
    )
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [PROPOSED] anchor seed [POINTS] n=1 ")
    assert text.index("[CUES]") < text.index("[NAME]") < text.index("[PASSAGE]")
    assert len(text) <= 2400 and text.endswith("…")


def test_the_input_does_not_read_the_point_seed_flag():
    """A seed is the set's role, which is what is predicted, not a point flag (S2 retires it)."""
    flagged = point_summary([_point(20, -4, -18, is_seed=True)])
    plain = point_summary([_point(20, -4, -18)])
    assert flagged == plain and "seeds=" not in plain


def test_point_shape_separates_roi_centres_from_peaks():
    rois = point_summary([_point(-24, -4, -18), _point(24, -4, -18)])
    peaks = point_summary(
        [
            _point(-23.5, 10, 4, statistic_type="t", statistic_value=5.1, cluster_size=120),
            _point(40, -60, 12, statistic_type="t", statistic_value=-4.2, is_subpeak=True),
        ]
    )
    assert "mirrored=2" in rois and "integral=2" in rois and "valued=0/2" in rois
    assert (
        "stats=t:2" in peaks
        and "negative=1" in peaks
        and "clusters=1" in peaks
        and "subpeaks=1" in peaks
    )


def test_table_sets_read_their_caption_header_rows_neighbours_and_citing_sentences():
    rois = _table_analysis([_point(-24, -4, -18), _point(24, -4, -18)])
    peaks = {**_table_analysis([_point(38, 12, 2)]), "name": "faces > houses"}
    context = table_context.build(
        rois, index=0, siblings=[rois, peaks], table_text=TABLE, article_text=ARTICLE
    )
    assert context.proposed == RESULT
    assert context.citing == ["As shown in Table 2, seeds were placed bilaterally."]
    assert context.header == ["#Region | #x | #y | #z | #t"]
    assert context.rows == ["Amygdala L | -24 | -4 | -18 |", "Amygdala R | 24 | -4 | -18 |"]
    assert context.neighbours == ["faces > houses (1)"]
    text = table_context.serialize(context)
    assert text.startswith("[ORIGIN] table [PROPOSED] result [POINTS] n=2 ")
    assert (
        "[TABLE] Table 2 [NEIGHBOURS] faces > houses (1) [CAPTION] Regions of interest used"
        in text
    )
    assert "[ROWS] Amygdala L | -24 | -4 | -18 | / Amygdala R" in text
    assert "[FOOTER]" not in text  # empty fields are dropped
    assert "[PASSAGE]" not in text  # a table set has no prose fields


def test_prose_sets_read_their_passage_heading_and_citation_markers():
    passages = [
        {
            "text": "We extracted the mPFC time series (x = -3, y = 49, z = 16) (41).",
            "heading": "Seed-based connectivity",
            "before": "Preprocessing.",
            "after": "Then.",
        }
    ]
    analysis = {
        "name": "mPFC seed",
        "coordinates": [_point(-3, 49, 16)],
        "metadata": {
            "source": "prose",
            "role": "anchor",
            "anchor_kind": "seed",
            "from_prior_study": False,
            "passages": [0],
        },
    }
    context = prose_context.build(analysis, passages)
    assert (context.proposed, context.heading) == (SEED, "Seed-based connectivity")
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [PROPOSED] anchor seed")
    assert (
        "[CITATIONS] (41) [PASSAGE] We extracted" in text
        and "[BEFORE] Preprocessing. [AFTER] Then." in text
    )
    assert "citations=1" in cue_summary(context.cue_text())
    assert prose_context.prior_evidence(context) == [passages[0]["text"]]
    assert context.evidence_sentences() == ["Preprocessing.", passages[0]["text"], "Then."]


def test_a_training_row_s_point_shapes_read_the_same_as_the_payload_s():
    payload = point_summary(
        [_point(1.0, 2.0, 3.0, statistic_type="Z", statistic_value=3.9, cluster_size=44)]
    )
    assert point_summary([[1.0, 2.0, 3.0, "Z", 3.9, 44]]) == payload
    assert (
        point_summary([{"xyz": [1.0, 2.0, 3.0], "stat": ["Z", 3.9], "cluster": [44, "voxels"]}])
        == payload
    )


def test_citations_are_not_coordinates():
    text = "Peaks at (4, 30, 22) and (-6, -60, 40), as in Lee and Kim (2008) [3]."
    assert "citations=2" in cue_summary(text)


def test_the_role_vocabulary_is_study_schema_s():
    assert COORDINATE_ROLES == tuple(r.value for r in CoordinateRole)
    assert ANCHOR_KINDS == tuple(k.value for k in AnchorKind)


def test_the_legacy_adapter_reads_the_prose_model_s_roles_as_study_schema_s():
    got = [study_schema_role(r) for r in (*ROLES, None)]
    assert [(g["role"], g["anchor_kind"], g["from_prior_study"]) for g in got] == [
        ("result", None, False),
        ("anchor", "roi", False),
        ("anchor", "seed", False),
        ("anchor", "stimulation_target", False),
        ("reference", None, True),
        ("display", None, False),
        (None, None, False),  # other: indistinguishable from not coordinates; see the adapter
        ("result", None, False),
    ]
    for fields in got:
        SetRole.of(fields)  # each is valid study_schema


def test_a_role_outside_study_schema_is_refused():
    for bad in (
        {"role": "simulation"},
        {"role": "anchor:roi"},
        {"role": "anchor", "anchor_kind": None},
        {"role": "result", "anchor_kind": "seed"},
    ):
        try:
            SetRole.of(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad} accepted")


def _prediction(role, p, prior=0.0, kind=None, coordinates=0.99):
    rest = (1 - p) / (len(COORDINATE_ROLES) - 1)
    return Prediction(
        coordinates,
        {name: (p if name == role else rest) for name in COORDINATE_ROLES},
        {k: (0.9 if k == kind else 0.1 / 3) for k in ANCHOR_KINDS},
        prior,
    )


def test_the_proposal_stands_below_the_threshold():
    decision = decide(RESULT, _prediction("reference", 0.7), source="enc@1", min_confidence=0.8)
    assert (decision.role, decision.source, decision.uploaded) == ("result", "proposal", True)
    assert decision.confidence < 0.1  # the classifier's probability for the label recorded


def test_a_confident_prediction_overrides_and_says_so():
    decision = decide(
        RESULT,
        _prediction("reference", 0.9),
        source="enc@1",
        min_confidence=0.8,
        evidence=["As reported by Lee et al. (2008)."],
    )
    meta = decision.to_metadata()
    assert meta == {
        "role": "reference",
        "anchor_kind": None,
        "from_prior_study": True,
        "prior_study_evidence": [{"text": "As reported by Lee et al. (2008)."}],
        "role_confidence": 0.9,
        "role_source": "enc@1",
        "proposal": {"role": "result", "anchor_kind": None, "from_prior_study": False},
    }
    assert not decision.uploaded


def test_a_borrowed_seed_is_a_seed_from_a_prior_study():
    decision = decide(
        SEED,
        _prediction("anchor", 0.95, prior=0.8, kind="seed"),
        source="enc@1",
        min_confidence=0.8,
        evidence=["The seed came from Smith et al. (2010)."],
    )
    assert (decision.role, decision.anchor_kind, decision.from_prior_study, decision.uploaded) == (
        "anchor",
        "seed",
        True,
        True,
    )


def test_no_evidence_is_recorded_for_the_study_s_own_set():
    decision = decide(
        RESULT,
        _prediction("result", 0.99),
        source="enc@1",
        min_confidence=0.8,
        evidence=["(Smith et al., 2010)"],
    )
    assert decision.prior_study_evidence == () and not decision.from_prior_study


def test_confident_not_coordinates_are_set_aside_without_a_role():
    decision = decide(
        SEED,
        _prediction("anchor", 0.9, kind="seed", coordinates=0.05),
        source="enc@1",
        min_confidence=0.8,
    )
    assert (decision.role, decision.anchor_kind, decision.uploaded) == (None, None, False)
    assert decision.to_metadata()["proposal"]["anchor_kind"] == "seed"


def test_other_is_a_role_that_is_not_uploaded():
    assert "other" in COORDINATE_ROLES and "other" not in UPLOADED_ROLES
    assert SetRole.of({"role": "other", "anchor_kind": None, "from_prior_study": False}).role == "other"
    assert not SetRole("other").uploaded
