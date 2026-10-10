"""The set-role classifier's input and decision rule, without a model."""

from __future__ import annotations

from ingestion_workflow.services.set_roles import (
    ROLE_LABELS,
    Prediction,
    ProseSetContext,
    decide,
    extractor_role,
    label_from_prose,
    prose_context,
    table_context,
)
from ingestion_workflow.services.set_roles.common import cue_summary, point_summary

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
        proposed="anchor:seed",
    )
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [PROPOSED] anchor:seed [POINTS] n=1 ")
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
    assert context.proposed == "result"
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
        "metadata": {"source": "prose", "role": "seed", "passages": [0]},
    }
    context = prose_context.build(analysis, passages)
    assert (context.proposed, context.heading) == ("anchor:seed", "Seed-based connectivity")
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [PROPOSED] anchor:seed")
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


def test_prose_roles_map_onto_labels():
    assert [
        label_from_prose(r)
        for r in ("result", "roi", "seed", "target", "prior_study", "figure", "other", None)
    ] == [
        "result",
        "anchor:roi",
        "anchor:seed",
        "anchor:stimulation_target",
        "reference",
        "display",
        "other",
        "result",
    ]
    assert set(map(label_from_prose, ("roi", "prior_study"))) <= set(ROLE_LABELS)


def test_every_label_has_an_extractor_role_and_prose_roles_round_trip():
    assert [extractor_role(label) for label in ROLE_LABELS] == [
        "result",
        "roi",
        "seed",
        "target",
        "roi",
        "other",
        "prior_study",
        "figure",
        "other",
    ]
    for role in ("result", "roi", "seed", "target", "prior_study", "figure", "other"):
        assert extractor_role(label_from_prose(role)) == role


def _prediction(label, p, prior=0.0):
    rest = (1 - p) / (len(ROLE_LABELS) - 1)
    return Prediction({name: (p if name == label else rest) for name in ROLE_LABELS}, prior)


def test_the_proposal_stands_below_the_threshold():
    decision = decide("result", _prediction("reference", 0.7), source="enc@1", min_confidence=0.8)
    assert (decision.label, decision.source, decision.uploaded) == ("result", "proposal", True)
    assert decision.confidence < 0.1  # the classifier's probability for the label recorded


def test_a_confident_prediction_overrides_and_says_so():
    decision = decide(
        "result",
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
        "proposal": "result",
    }
    assert not decision.uploaded


def test_a_borrowed_seed_is_a_seed_from_a_prior_study():
    decision = decide(
        "anchor:seed",
        _prediction("anchor:seed", 0.95, prior=0.8),
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
        "result",
        _prediction("result", 0.99),
        source="enc@1",
        min_confidence=0.8,
        evidence=["(Smith et al., 2010)"],
    )
    assert decision.prior_study_evidence == () and not decision.from_prior_study
