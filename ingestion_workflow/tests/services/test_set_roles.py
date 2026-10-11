"""The set-role classifier's input and decision rule, without a model."""

from __future__ import annotations

import pytest
from ingestion_workflow.services.set_roles import (
    ANCHOR_KINDS,
    COORDINATE_ROLES,
    UPLOADED_ROLES,
    Prediction,
    ProseSetContext,
    SetRole,
    decide,
    prose_context,
    table_context,
)
from ingestion_workflow.services.set_roles.common import cue_summary, point_summary
from study_schema.models.paper_parse import AnchorKind, CoordinateRole

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
    )
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [POINTS] n=1 ")
    assert "[PROPOSED]" not in text  # no role is proposed: the classifier decides
    assert text.index("[CUES]") < text.index("[NAME]") < text.index("[PASSAGE]")
    assert len(text) <= 2400 and text.endswith("…")


def test_the_input_does_not_read_the_point_seed_flag():
    """A seed is the set's role, which is what is predicted, not a point flag."""
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
    assert context.citing == ["As shown in Table 2, seeds were placed bilaterally."]
    assert context.header == ["#Region | #x | #y | #z | #t"]
    assert context.rows == ["Amygdala L | -24 | -4 | -18 |", "Amygdala R | 24 | -4 | -18 |"]
    assert context.neighbours == ["faces > houses (1)"]
    text = table_context.serialize(context)
    assert text.startswith("[ORIGIN] table [POINTS] n=2 ")
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
    assert context.heading == "Seed-based connectivity"
    text = prose_context.serialize(context)
    assert text.startswith("[ORIGIN] text [POINTS] n=1 ")
    assert (
        "[CITATIONS] (41) [LOCAL] We extracted" in text and "[PASSAGE] We extracted" in text
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


def test_the_role_is_the_model_s_answer_however_sure_it_is():
    decision = decide(_prediction("reference", 0.4), source="enc@1", origin="table")
    assert (decision.role, decision.source, decision.confidence) == ("reference", "enc@1", 0.4)
    assert not decision.uploaded


def test_a_decision_without_a_prediction_is_refused():
    with pytest.raises(TypeError):
        decide(None, source="enc@1", origin="table")


def test_the_decision_records_its_model_origin_and_confidence():
    decision = decide(
        _prediction("reference", 0.9),
        source="enc@1",
        origin="text",
        evidence=["As reported by Lee et al. (2008)."],
    )
    assert decision.to_metadata() == {
        "role": "reference",
        "anchor_kind": None,
        "from_prior_study": True,
        "prior_study_evidence": [{"text": "As reported by Lee et al. (2008)."}],
        "role_confidence": 0.9,
        "role_source": "enc@1",
        "role_origin": "text",
    }


def test_a_borrowed_seed_is_a_seed_from_a_prior_study():
    decision = decide(
        _prediction("anchor", 0.95, prior=0.8, kind="seed"),
        source="enc@1",
        origin="text",
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
        _prediction("result", 0.99),
        source="enc@1",
        origin="table",
        evidence=["(Smith et al., 2010)"],
    )
    assert decision.prior_study_evidence == () and not decision.from_prior_study


def test_not_coordinates_are_set_aside_without_a_role():
    decision = decide(
        _prediction("anchor", 0.9, kind="seed", coordinates=0.05), source="enc@1", origin="text"
    )
    assert (decision.role, decision.anchor_kind, decision.uploaded) == (None, None, False)
    assert decision.confidence == pytest.approx(0.95)


def test_other_is_a_role_that_is_not_uploaded():
    assert "other" in COORDINATE_ROLES and "other" not in UPLOADED_ROLES
    assert SetRole.of({"role": "other", "anchor_kind": None, "from_prior_study": False}).role == "other"


_TEXT = (
    "We placed a 6 mm sphere at the seed (x = -3, y = 49, z = 16). "
    "Activation peaked in the insula [-42 18 −6]. "
    "Peaks are listed in Table 2."
)


def _local(text, *points, **kw):
    analysis = {"name": "s", "coordinates": [_point(*p) for p in points]}
    return prose_context.build(analysis, passage={"text": text, **kw}).local


def test_local_is_exactly_the_sentence_holding_the_set_s_coordinates():
    assert _local(_TEXT, (-3, 49, 16)) == (
        "We placed a 6 mm sphere at the seed (x = -3, y = 49, z = 16)."
    )
    # Another set in the same passage reads its own sentence, not the seed's.
    assert _local(_TEXT, (-42, 18, -6)) == "Activation peaked in the insula [-42 18 −6]."


def test_local_holds_every_sentence_of_a_set_with_points_in_several():
    assert _local(_TEXT, (-3, 49, 16), (-42, 18, -6)) == (
        "We placed a 6 mm sphere at the seed (x = -3, y = 49, z = 16). "
        "Activation peaked in the insula [-42 18 \u22126]."
    )


def test_local_keeps_both_sentences_when_a_triple_is_cut_by_a_sentence_break():
    text = "Intro here. The peak was at x = -42, y = 18. Z = 6 in the insula. End of it."
    assert _local(text, (-42, 18, 6)) == "The peak was at x = -42, y = 18. Z = 6 in the insula."
    # A neighbouring set's sentence is not pulled in.
    assert _local(text + " Next (1, 2, 3).", (1, 2, 3)) == "Next (1, 2, 3)."


def test_local_is_empty_when_the_coordinates_are_not_in_the_text_and_never_a_longer_number():
    assert _local(_TEXT, (7, 7, 7)) == ""
    assert _local("Peak (-142, 18, 6).", (-42, 18, 6)) == ""
    assert _local("Peak (142, 18, 6).", (42, 18, 6)) == ""


def test_local_of_a_right_hemisphere_point_is_not_the_left_one_s_sentence():
    text = "Left peak (-42, 18, 6) in the insula. Right peak (42, 18, 6) in the putamen."
    assert _local(text, (42, 18, 6)) == "Right peak (42, 18, 6) in the putamen."
    assert _local(text, (-42, 18, 6)) == "Left peak (-42, 18, 6) in the insula."
    assert _local("Left peak (-42, 18, 6) in the insula.", (42, 18, 6)) == ""


def test_local_holds_a_sentence_once_however_many_of_the_set_s_points_it_prints():
    text = "Intro. Peaks (1, 2, 3) and (4, 5, 6) here. End."
    assert _local(text, (1, 2, 3), (4, 5, 6)) == "Peaks (1, 2, 3) and (4, 5, 6) here."


def test_local_skips_a_sentence_between_two_holding_the_set_s_points():
    text = "Seed (1, 2, 3) here. Nothing in this one. Peak (4, 5, 6) there."
    assert _local(text, (1, 2, 3), (4, 5, 6)) == "Seed (1, 2, 3) here. Peak (4, 5, 6) there."


def test_local_of_a_long_sentence_is_a_window_on_the_set_s_own_coordinates():
    # p:silver-1007: one sentence listing every network's ROI, each ROI a set.
    networks = ", ".join(f"network {i}: ({i}, {-50 - i}, {10 + i})" for i in range(40))
    text = f"Results. ROIs were {networks}. Next sentence here."
    locals_ = {}
    for i in (3, 20, 37):
        local = _local(text, (i, -50 - i, 10 + i))
        assert len(local) <= prose_context.LOCAL_CHARS
        assert f"({i}, {-50 - i}, {10 + i})" in local
        # Centred on the triple, but kept inside its sentence.
        at = local.index(f"({i}, {-50 - i}, {10 + i})")
        assert 150 < at < 250 if i == 20 else True
        assert local.startswith("ROIs were") == (i == 3)
        assert local.endswith("(39, -89, 49).") == (i == 37)
        assert local.split(" ")[0] in text.split(" ") and local.split(" ")[-1] in text.split(" ")
        locals_[i] = local
    assert len(set(locals_.values())) == 3
    # A set whose sentence fits keeps all of it.
    assert _local(text, (99, 99, 99)) == ""
    assert _local("Short one (1, 2, 3). " + text, (1, 2, 3)) == "Short one (1, 2, 3)."


def test_local_holds_the_set_s_coordinates_when_they_come_after_char_400():
    text = "The region " + "word " * 100 + "peaked at (-42, 18, 6) in the left insula."
    local = _local(text, (-42, 18, 6))
    assert local.endswith("peaked at (-42, 18, 6) in the left insula.")
    assert len(local) <= 400 and not local.startswith("The region")
    context = prose_context.build(
        {"name": "s", "coordinates": [_point(-42, 18, 6)]}, passage={"text": text}
    )
    assert "(-42, 18, 6)" in prose_context.serialize(context)


def test_local_shares_its_length_between_the_passages_holding_the_set_s_points():
    long = "Intro " + "word " * 120 + "at (1, 2, 3) end."
    points = [_point(1, 2, 3), _point(4, 5, 6)]
    analysis = {"name": "s", "coordinates": points, "metadata": {"passages": [0, 1]}}
    passages = [{"text": long}, {"text": long.replace("1, 2, 3", "4, 5, 6")}]
    local = prose_context.build(analysis, passages).local
    assert len(local) <= prose_context.LOCAL_CHARS
    assert "(1, 2, 3)" in local and "(4, 5, 6)" in local


def test_local_is_in_the_serialised_input_before_the_passage():
    context = prose_context.build(
        {"name": "s", "coordinates": [_point(-42, 18, -6)]}, passage={"text": _TEXT}
    )
    text = prose_context.serialize(context)
    assert "[LOCAL] Activation peaked in the insula" in text
    assert text.index("[LOCAL]") < text.index("[PASSAGE]")
    assert "[LOCAL]" not in prose_context.serialize(prose_context.ProseSetContext())


def test_a_model_trained_on_the_previous_prose_context_is_refused_with_the_reason():
    from ingestion_workflow.services.set_roles import model

    meta = {"origin": "text", "context_version": prose_context.PROSE_CONTEXT_VERSION - 1}
    with pytest.raises(ValueError, match=r"text context version 3; this code builds 4; retrain"):
        model.check_meta(meta, "text", "role_model_prose")
    # The table context is unchanged: its version is not bumped by this.
    assert model.CONTEXT_VERSIONS["table"] == 2 and model.CONTEXT_VERSIONS["text"] == 4
