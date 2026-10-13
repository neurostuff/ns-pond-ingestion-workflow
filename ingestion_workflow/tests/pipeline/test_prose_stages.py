"""`prose` reads coordinates from the text; `resolve` merges them with the tables'."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stage import NO_ANALYSES, NO_TEXT
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.passages import PassagesStage
from ingestion_workflow.pipeline.stages.prose import ProseStage
from ingestion_workflow.pipeline.stages.resolve import ResolveStage, resolve
from ingestion_workflow.pipeline.stages.roles import RolesStage
from ingestion_workflow.pipeline.stages.space import SpaceStage

TEXT = (
    "## Methods\nCoordinates are reported in MNI space.\n\n"
    "## Results\nPatients showed greater activation than controls in the left amygdala "
    "(x = -22, y = -4, z = -18; t = 4.1). The reverse contrast revealed no clusters.\n"
)


def _table(points):
    return {"slug": "a::t1", "coordinate_space": "MNI", "identifier": None, "analyses": [{
        "name": "patients > controls", "table_caption": "", "table_footer": "",
        "coordinates": [{"x": x, "y": y, "z": z, "space": "MNI"} for x, y, z in points]}]}


def _passage(*analyses, space="MNI"):
    return {"text": "...", "heading": None, "space": space, "error": None, "analyses": [
        {"name": name, "measure": None, "points": [
            {"x": x, "y": y, "z": z, "cluster_size": None, "role": role,
             # A seed is where an analysis is computed from; it has no statistic.
             **({"statistic": None, "value": None} if role == "seed"
                else {"statistic": "T", "value": 4.1})}
            for x, y, z, role in points]}
        for name, points in analyses]}


def test_both_extractions_then_metadata_then_both_model_stages():
    order = list(STAGE_ORDER)
    assert order.index("extract") < order.index("passages") < order.index("metadata")
    assert order.index("metadata") < order.index("analyses") < order.index("prose")
    assert order.index("prose") < order.index("resolve") < order.index("space") < order.index("upload")


def test_the_prose_stages_are_left_out_unless_switched_on(tmp_path):
    off = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    assert not {s.name for s in build(None, off)} & set(PROSE_STAGES)
    with pytest.raises(ValueError, match="prose_model"):
        build(["prose"], off)
    on = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose")
    assert set(PROSE_STAGES) <= {s.name for s in build(None, on)}


def test_roles_reads_resolve_only_when_prose_is_on_and_space_always_reads_roles(tmp_path):
    off = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    on = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose")
    assert (RolesStage(off).requires, RolesStage(on).requires) == ("analyses", "resolve")
    assert SpaceStage(off).requires == SpaceStage(on).requires == "roles"


def test_a_restated_table_peak_is_recorded_but_not_uploaded_twice():
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("patients > controls", [(-22, -4, -17, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert out == tables
    assert summary["restated"] == 1 and summary["restatements"] == 1 and summary["prose_analyses"] == 0
    assert summary["restated_points"] == [{
        "passages": [0], "analysis": "patients > controls", "restatement": True,
        "points": [{"x": -22.0, "y": -4.0, "z": -17.0,
                    "restates": [{"table": "t1", "analysis": "patients > controls"}]}]}]


def test_an_analysis_restating_a_table_under_another_name_is_recorded_not_uploaded():
    """"NHSDD > HSDD" in a Results sentence, its four peaks those of table 3."""
    peaks = [(-6, 24, 48), (40, 18, -4), (-38, 20, -2), (4, -60, 30)]
    tables = {"t3": _table(peaks)}
    tables["t3"]["analyses"][0]["name"] = "NHSDD > HSDD"
    prose = {"passages": [_passage(("erotic > non-erotic: NHSDD > HSDD",
                                    [(x, y, z, "result") for x, y, z in peaks]))]}
    out, summary = resolve(tables, prose, "slug")
    assert "prose" not in out
    (record,) = summary["restated_points"]
    assert record["restatement"] is True
    assert [p["restates"] for p in record["points"]] == [[{"table": "t3", "analysis": "NHSDD > HSDD"}]] * 4
    assert summary["restatements"] == 1 and summary["restated"] == 4


def test_an_analysis_with_new_points_keeps_them_and_records_the_restated_ones():
    """A correlation reported at one table peak and at a peak the table lacks."""
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("negative correlation with craving",
                                    [(-22, -4, -18, "result"), (36, 20, 2, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert [(c["x"], c["y"], c["z"]) for c in analysis["coordinates"]] == [(36.0, 20.0, 2.0)]
    assert analysis["metadata"]["restated_points"] == [{
        "x": -22.0, "y": -4.0, "z": -18.0,
        "restates": [{"table": "t1", "analysis": "patients > controls"}]}]
    assert summary["restated"] == 1 and summary["restatements"] == 0 and summary["prose_points"] == 1
    assert summary["restated_points"][0]["restatement"] is False


def test_every_prose_point_is_kept_under_its_analysis_and_its_role_word_is_not_read():
    """What a set is for is the roles stage's to decide: the prose model's own
    role word neither drops a point nor splits an analysis."""
    prose = {"passages": [_passage(
        ("amygdala seed", [(-22, -4, -18, "seed")]),
        ("Smith et al. (2010)", [(30, 20, 10, "prior_study")]),
        ("channel 12", [(1, 2, 3, "other")]),
        ("faces > houses", [(40, -50, -20, "result"), (12, 10, 8, "seed")]))]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], len(a["coordinates"])) for a in out["prose"]["analyses"]]
    assert got == [("amygdala seed", 1), ("Smith et al. (2010)", 1), ("channel 12", 1), ("faces > houses", 2)]
    for ordinal, analysis in enumerate(out["prose"]["analyses"]):
        assert analysis["metadata"] == {"source": "prose", "passages": [0], "ordinal": ordinal, "unwritten": 0}
    # No point carries a role or seed flag of its own.
    assert not any({"role", "is_seed"} & set(c) for a in out["prose"]["analyses"] for c in a["coordinates"])
    assert summary["prose_points"] == 5 and "dropped" not in summary


def test_a_seed_at_a_table_peak_is_the_tables_result_reused():
    """Named for the analysis run from it, the seed is still the table's peak."""
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("PPI with amygdala seed", [(-22, -4, -18, "seed")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert out == tables and summary["restatements"] == 1


def test_one_peak_reported_for_two_contrasts_stays_under_both():
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result")]),
                                   ("main effect of load", [(40, -50, -20, "result")]),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    analyses = out["prose"]["analyses"]
    assert [a["name"] for a in analyses] == ["faces > houses", "main effect of load"]
    assert [len(a["coordinates"]) for a in analyses] == [1, 1]
    assert summary["prose_points"] == 2


def test_a_seed_and_the_results_connected_to_it_stay_apart_across_passages():
    """A Methods passage's seed and a Results passage's PPI peaks share a name;
    they are different sets, and the peak at the seed's xyz is kept."""
    prose = {"passages": [_passage(("amygdala PPI", [(-20, -4, -18, "seed")])),
                          _passage(("amygdala PPI", [(-20, -4, -18, "result"), (30, 20, 4, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], len(a["coordinates"])) for a in out["prose"]["analyses"]]
    assert got == [("amygdala PPI", [0], 1), ("amygdala PPI", [1], 2)]
    assert summary["prose_points"] == 3 and summary["repeated"] == 0


def test_one_analysis_named_again_at_its_point_in_another_passage_is_one_analysis():
    """"In previous analyses, we have shown that the striatum (peak −18, 11, −2)":
    the Results' striatum ROI named again in the Discussion, under another heading."""
    prose = {"passages": [
        _headed("Results", ("striatum ROI", [(-18, 11, -2, "roi"), (20, 10, -4, "roi")])),
        _headed("Results", ("left striatum", [(-18, 11, -2, "roi")])),
        _headed("Discussion", ("Striatum  ROI", [(-18.5, 11, -2, "roi")])),
    ]}
    out, summary = resolve({}, prose, "slug")
    joined, other = out["prose"]["analyses"]
    assert (joined["name"], joined["metadata"]["passages"]) == ("striatum ROI", [0, 2])
    assert joined["metadata"]["joined"] == [{"passages": [0, 2], "by": "shared point"}]
    assert [(c["x"], c["y"], c["z"]) for c in joined["coordinates"]] == [(-18.0, 11.0, -2.0), (20.0, 10.0, -4.0)]
    assert joined["metadata"]["dropped"] == [
        {"x": -18.5, "y": 11.0, "z": -2.0, "reason": "repeats a point of this analysis"}]
    # Another name at the same point is another analysis.
    assert (other["name"], other["metadata"]["passages"]) == ("left striatum", [1])
    assert summary["joins"] == {"shared point": 1} and summary["prose_analyses"] == 2


def _stated(name, x, y, z, statistic, value, heading=None):
    point = {"x": x, "y": y, "z": z, "statistic": statistic, "value": value,
             "cluster_size": None, "role": "result"}
    return {**_passage((name, [])), "heading": heading,
            "analyses": [{"name": name, "measure": None, "points": [point]}]}


def test_the_same_point_at_the_same_statistic_joins():
    prose = {"passages": [_stated("insula", 38, 20, -4, "T", 4.1),
                          _stated("insula", 38, 20, -4, "t", 4.11)]}
    out, summary = resolve({}, prose, "slug")
    assert len(out["prose"]["analyses"]) == 1 and summary["joins"] == {"shared point": 1}


def test_a_point_with_a_statistic_and_one_without_stay_apart():
    prose = {"passages": [_stated("insula", 38, 20, -4, None, None),
                          _stated("insula", 38, 20, -4, "T", 4.1)]}
    out, summary = resolve({}, prose, "slug")
    assert [a["metadata"]["passages"] for a in out["prose"]["analyses"]] == [[0], [1]]
    assert summary["joins"] == {} and summary["prose_points"] == 2


def test_a_point_at_two_statistic_values_or_kinds_stays_apart():
    for other in (("T", 6.3), ("Z", 4.1)):
        prose = {"passages": [_stated("insula", 38, 20, -4, "T", 4.1),
                              _stated("insula", 38, 20, -4, *other)]}
        out, summary = resolve({}, prose, "slug")
        assert len(out["prose"]["analyses"]) == 2 and summary["joins"] == {}


def test_a_shared_heading_does_not_merge_a_seed_into_a_result_at_its_point():
    seed = _stated("insula PPI", 38, 20, -4, None, None, heading="Imaging")
    result = _stated("insula PPI", 38, 20, -4, "T", 4.1, heading="Imaging")
    elsewhere = _stated("insula PPI", 10, 10, 10, "T", 4.1, heading="Imaging")
    out, summary = resolve({}, {"passages": [seed, result]}, "slug")
    assert len(out["prose"]["analyses"]) == 2 and summary["joins"] == {}
    # The heading still joins sets that share no point.
    out, summary = resolve({}, {"passages": [result, elsewhere]}, "slug")
    assert len(out["prose"]["analyses"]) == 1 and summary["joins"] == {"heading": 1}


def test_a_name_seen_in_three_passages_joins_through_any_shared_point():
    prose = {"passages": [_passage(("FFA", [(40, -52, -18, "roi")])),
                          _passage(("FFA", [(-38, -50, -20, "roi")])),
                          _passage(("FFA", [(40, -52, -18, "roi"), (-38, -50, -20, "roi")]))]}
    out, summary = resolve({}, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert analysis["metadata"]["passages"] == [0, 1, 2] and len(analysis["coordinates"]) == 2
    assert summary["joins"] == {"shared point": 2}


def test_unnamed_analyses_are_each_their_own_set_in_the_model_s_order():
    """An unnamed peak quoted in the Introduction never joins an unnamed result."""
    prose = {"passages": [_passage((None, [(10, 10, 10, "prior_study")])),
                          _passage((None, [(40, -50, -20, "result")]), (None, [(1, 2, 3, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], a["coordinates"][0]["x"])
           for a in out["prose"]["analyses"]]
    # Numbered within their passage, so no two in one passage share a name.
    assert got == [("unnamed prose analysis", [0], 10.0), ("unnamed prose analysis", [1], 40.0),
                   ("unnamed prose analysis 2", [1], 1.0)]


def test_a_point_dropped_from_a_set_is_recorded_with_its_reason():
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result"), (40.2, -50, -20, "result"),
                                                       (-22, -4, -18, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    [analysis] = out["prose"]["analyses"]
    assert len(analysis["coordinates"]) == 1
    assert analysis["metadata"]["dropped"] == [
        {"x": 40.2, "y": -50.0, "z": -20.0, "reason": "repeats a point of this analysis"}]
    assert [(p["x"], p["y"], p["z"]) for p in analysis["metadata"]["restated_points"]] == [(-22.0, -4.0, -18.0)]
    assert (summary["repeated"], summary["restated"]) == (1, 1)


def test_two_unnamed_analyses_in_one_passage_stay_two_in_the_passages_order():
    prose = {"passages": [_passage((None, [(4, 52, 18, "result")]),
                                   (None, [(-38, 22, -6, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["ordinal"], a["coordinates"][0]["x"])
           for a in out["prose"]["analyses"]]
    assert got == [("unnamed prose analysis", 0, 4.0), ("unnamed prose analysis 2", 1, -38.0)]


def test_main_effect_in_two_experiments_is_two_analyses():
    """Experiment 1 and Experiment 2 each report a "Main effect" in their own passage."""
    prose = {"passages": [
        _passage(("Main effect", [(-44, 12, 28, "result")])),
        _passage(("main effect", [(4, 52, 18, "result"), (36, -58, 46, "result")])),
    ]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], len(a["coordinates"]))
           for a in out["prose"]["analyses"]]
    assert got == [("Main effect", [0], 1), ("main effect", [1], 2)]
    assert summary["prose_points"] == 3


def test_a_named_contrast_the_passage_reports_no_peak_for_is_kept():
    prose = {"passages": [_passage(("Placebo > Sham", []),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], len(a["coordinates"])) for a in out["prose"]["analyses"]]
    assert got == [("Placebo > Sham", 0), ("faces > houses", 1)]
    assert summary["prose_analyses"] == 2


def _stat_table(kind, value):
    blob = _table([(-22, -4, -18)])
    blob["analyses"][0]["coordinates"][0].update(statistic_type=kind, statistic_value=value)
    return {"t1": blob}


def _stat_prose(kind, value):
    passage = _passage(("dorsal DMN cluster", [(-22, -4, -18, "result")]))
    passage["analyses"][0]["points"][0].update(statistic=kind, value=value)
    return {"passages": [passage]}


def test_a_correlation_read_at_a_table_t_peak_is_its_own_analysis():
    """The five C-SSRS correlations (r) reported at the peaks of a T-map table."""
    out, summary = resolve(_stat_table("T", 2.375), _stat_prose("R", -0.342), "slug")
    (analysis,) = out["prose"]["analyses"]
    assert "restated_points" not in analysis["metadata"] and len(analysis["coordinates"]) == 1
    assert summary["restated"] == 0 and summary["prose_points"] == 1


def test_another_kind_of_statistic_with_the_same_number_is_its_own_analysis():
    out, summary = resolve(_stat_table("Z", 3.1), _stat_prose("T", 3.1), "slug")
    assert len(out["prose"]["analyses"]) == 1 and summary["restated"] == 0


def test_a_value_that_differs_from_the_table_row_is_its_own_analysis():
    out, summary = resolve(_stat_table("T", 2.375), _stat_prose("T", 5.1), "slug")
    assert len(out["prose"]["analyses"]) == 1 and summary["restated"] == 0


@pytest.mark.parametrize("kind, value", [("T", 2.38), ("t", -2.375), ("T", None), (None, 2.38),
                                         ("t-value", 2.375)])
def test_the_same_statistic_at_a_table_peak_is_still_a_restatement(kind, value):
    out, summary = resolve(_stat_table("T", 2.375), _stat_prose(kind, value), "slug")
    assert "prose" not in out
    assert summary["restated"] == 1 and summary["restatements"] == 1 and summary["prose_points"] == 0


def test_a_statistic_absent_from_the_table_row_is_a_restatement():
    out, summary = resolve(_stat_table(None, None), _stat_prose("R", -0.342), "slug")
    assert "prose" not in out and summary["restatements"] == 1


def _headed(heading, *analyses):
    return {**_passage(*analyses), "heading": heading}


def test_one_contrast_in_two_paragraphs_of_a_section_is_one_analysis():
    prose = {"passages": [
        _headed("Imaging data", ("single > two-tone", [(-38, 22, -6, "result")])),
        _headed("Imaging data", ("Single>Two\u2013tone", [(42, 18, -4, "result")])),
    ]}
    out, summary = resolve({}, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert analysis["metadata"]["passages"] == [0, 1] and len(analysis["coordinates"]) == 2
    assert analysis["metadata"]["joined"] == [{"passages": [0, 1], "by": "heading"}]
    assert summary["prose_analyses"] == 1 and summary["joins"] == {"heading": 1}


def test_the_same_name_under_two_section_headings_or_none_stays_apart():
    prose = {"passages": [
        _headed("Experiment 1", ("faces > houses", [(-38, 22, -6, "result")])),
        _headed("Experiment 2", ("faces > houses", [(42, 18, -4, "result")])),
        _headed(None, ("faces > houses", [(4, 52, 18, "result")])),
        _headed(None, ("faces > houses", [(-6, 50, 20, "result")])),
    ]}
    out, _ = resolve({}, prose, "slug")
    assert [a["metadata"]["passages"] for a in out["prose"]["analyses"]] == [[0], [1], [2], [3]]


def test_an_unnamed_analysis_never_joins_under_a_shared_heading_or_at_a_shared_point():
    prose = {"passages": [_headed("Results", (None, [(4, 52, 18, "result")])),
                          _headed("Results", (None, [(-38, 22, -6, "result")])),
                          _headed("Discussion", (None, [(4, 52, 18, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    assert [a["metadata"]["passages"] for a in out["prose"]["analyses"]] == [[0], [1], [2]]


def test_an_unstated_space_is_left_for_the_space_stage():
    out, _ = resolve({}, {"passages": [_passage(("a > b", [(1, 2, 3, "result")]), space=None)]}, "s")
    assert out["prose"]["coordinate_space"] is None
    assert out["prose"]["analyses"][0]["coordinates"][0]["space"] is None


def test_a_stated_other_table_gives_the_prose_that_space():
    """`OTHER` is a stated space, so the prose inherits it."""
    point = {"x": 9.0, "y": 9.0, "z": 9.0, "space": "OTHER"}
    tables = {"t1": {"slug": "s::t1", "coordinate_space": "OTHER", "identifier": None,
                     "analyses": [{"name": "x", "coordinates": [point]}]}}
    prose = {"passages": [_passage(("a > b", [(1, 2, 3, "result")]), space=None)]}
    out, _ = resolve(tables, prose, "s")
    assert out["prose"]["coordinate_space"] == "OTHER"


#: The extraction's text of ARTICLE: what passages read.
EXTRACTED = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = \u221222, y = \u22124, z = \u221218; t = 4.1). The reverse contrast revealed no clusters.\n\n"
    "Table 1\n-40\t20\t10\n\n"
    "## Discussion\n\nPrior work found the amygdala at (x = 30, y = 2, z = -20).\n"
)

ARTICLE = """<article><front><article-meta><title-group><article-title>T</article-title></title-group>
</article-meta></front><body>
<sec><title>Methods</title><p>Coordinates are reported in MNI space.</p></sec>
<sec><title>Results</title><p>Patients showed greater activation than controls in the left amygdala
(x = &#x02212;22, y = &#x02212;4, z = &#x02212;18; t = 4.1). The reverse contrast revealed no clusters.</p>
<table-wrap><table><tr><td>-40</td><td>20</td><td>10</td></tr></table></table-wrap></sec>
<sec><title>Discussion</title><p>Prior work found the amygdala at (x = 30, y = 2, z = -20).</p></sec>
</body></article>"""


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", prose_model="nu-prose-v2", llm_api_key="x")
    path = tmp_path / "article.xml"
    path.write_text(ARTICLE)
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, path


def _download(ref, path):
    """A download payload as the download stage writes it."""
    from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType

    return DownloadResult(identifier=ref.identifier, source=DownloadSource.PUBGET, success=True, files=[
        DownloadedFile(file_path=path, file_type=FileType.XML, content_type="application/xml",
                       source=DownloadSource.PUBGET)]).to_dict()


def _record_upstream(catalog, ref, path, *, tables=None, text=EXTRACTED):
    """The download, its extraction's text, and the metadata."""
    text_file = path.parent / f"{ref.id}.txt"
    text_file.write_text(text, encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    rows = [
        Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                payload=_download(ref, path), summary={}),
        Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-1",
                payload={"title": "T", "abstract": "A"}, summary={}),
    ]
    if tables:
        rows.append(Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload=tables, summary={"tables": len(tables)}))
    catalog.record(rows)


class _Results:
    """A role classifier that calls every set a result."""

    source = "results@1"

    def predict(self, texts):
        from ingestion_workflow.services.set_roles import COORDINATE_ROLES, Prediction

        return [Prediction("result", None, {r: float(r == "result") for r in COORDINATE_ROLES})
                for _ in texts]


def _roles(settings, monkeypatch):
    stage = RolesStage(settings)
    monkeypatch.setattr(stage, "classifier", lambda origin: _Results())
    monkeypatch.setattr(stage, "model_state", lambda origin: (f"fake-{origin}@1", None))
    return stage


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def _read_prose(ctx, catalog, ref, reader):
    """passages, then prose with `reader` standing in for the model."""
    _run(PassagesStage(ctx.settings), ctx, catalog, ref)
    prose = ProseStage(ctx.settings)
    prose.client = lambda: reader
    return _run(prose, ctx, catalog, ref)


class _Reader:
    """Stands in for nu-prose-v2: reports every coordinate the passage holds, in one analysis."""

    def __init__(self, fail=False):
        self.fail = fail

    def read(self, passage, *, title="", abstract=""):
        if self.fail:
            raise RuntimeError("server down")
        return {"space": None, "analyses": [{"name": "patients > controls", "measure": None, "points": [
            {"x": h.x, "y": h.y, "z": h.z, "statistic": "T", "value": 4.1, "cluster_size": None}
            for h in passage.hits]}]}


def test_prose_reads_the_extraction_text_s_methods_and_results_only(env, monkeypatch):
    """Not the tables, not the discussion's cited peak."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="1"))
    _record_upstream(catalog, ref, path)
    _, (read,) = _read_prose(Context(settings, catalog), catalog, ref, _Reader())
    assert read.status is Status.OK and read.summary["read"] == "methods+results"
    points = [(q["x"], q["y"], q["z"]) for p in read.payload["passages"] for a in p["analyses"] for q in a["points"]]
    assert points == [(-22.0, -4.0, -18.0)]
    assert read.payload["passages"][0]["heading"] == "Results"
    assert read.payload["space"] == "MNI"   # read from the Methods


def test_a_text_with_no_coordinate_is_filtered_before_the_detector_reads_it(env, monkeypatch, tmp_path):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="5"))
    _record_upstream(catalog, ref, path, text="## Results\n\nAccuracy was 85% (70, 85, 92).\n")
    from ingestion_workflow.services import prose_passages

    monkeypatch.setattr(prose_passages, "passages", lambda *a, **k: (_ for _ in ()).throw(AssertionError("read")))
    ctx = Context(settings, catalog)
    _, (found,) = _run(PassagesStage(settings), ctx, catalog, ref)
    summary = dict(found.summary)
    summary.pop("spans")
    assert summary == {"source": "pubget", "read": "filtered", "passages": 0, "omitted": 0, "hits": 0}
    # and prose records the empty result without a call
    prose = ProseStage(settings)
    prose.client = lambda: (_ for _ in ()).throw(AssertionError("called"))
    _, (read,) = _run(prose, ctx, catalog, ref)
    assert read.status is Status.OK and read.summary["coordinates"] == 0


def test_a_prose_only_article_reaches_space_with_its_space_read(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="2"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.summary["prose_points"] == 1 and merged.summary["basis"] == ""
    assert merged.payload["prose"]["identifier"]["pmid"] == "2"   # upload matches the paper by it
    _, (roled,) = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    assert roled.summary["sets_by_origin"] == {"text": 1}
    _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
    assert spaced.payload["prose"]["analyses"][0]["coordinates"][0]["x"] == -22.0
    assert spaced.payload["prose"]["analyses"][0]["metadata"]["set_role"]["role_origin"] == "text"
    assert spaced.payload["prose"]["coordinate_space"] == "MNI"


def test_switching_prose_on_leaves_a_tables_only_article_fresh(env, monkeypatch):
    """Resolve names the analyses it passed through, so roles -- and space and
    upload after it -- keep the fingerprints they had before prose existed."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="3"))
    _record_upstream(catalog, ref, path, tables={"t1": _table([(-22, -4, -18)])})
    ctx = Context(settings, catalog)
    before = RolesStage(Settings(data_root=settings.data_root, cache_root=settings.cache_root))
    expected = before.fingerprint_for(catalog.artifact(ref.id, "analyses", ""), before.models())
    _read_prose(ctx, catalog, ref, _Reader())
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.summary["restated"] == 1
    assert merged.summary["basis"] == "an-1"
    after = RolesStage(settings)
    assert after.fingerprint_for(catalog.artifact(ref.id, "resolve", ""), after.models()) == expected


def test_an_article_with_a_failed_passage_is_retried_whole(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="4"))
    _record_upstream(catalog, ref, path)
    _, (outcome,) = _read_prose(Context(settings, catalog), catalog, ref, _Reader(fail=True))
    assert outcome.status is Status.FAILED




def test_prose_reads_again_once_the_title_and_abstract_arrive(env):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="9"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                            payload={"files": [{"file_path": str(path), "file_type": "xml"}]}, summary={})])
    text_file = path.parent / "nine.txt"
    text_file.write_text(EXTRACTED, encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    ctx = Context(settings, catalog)
    seen = []

    class Recorder(_Reader):
        def read(self, passage, *, title="", abstract=""):
            seen.append(title)
            return super().read(passage, title=title, abstract=abstract)

    _read_prose(ctx, catalog, ref, Recorder())
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-1",
                            payload={"title": "T", "abstract": "A"}, summary={})])
    plan, _ = _read_prose(ctx, catalog, ref, Recorder())
    assert len(plan.pending) == 1 and seen == ["", "T"]
    plan, _ = _read_prose(ctx, catalog, ref, Recorder())
    assert plan.fresh == 1 and seen == ["", "T"]


def test_an_article_whose_extraction_has_no_table_is_synced_from_its_prose(env, monkeypatch, tmp_path):
    """Sync writes the extraction's text, the one the passages index, and the prose's analyses."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.services.nspond_schema import read_record

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="11"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS11", "study_id": "S11"})])
    sync = SyncStage(settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    record = read_record(settings.ns_pond_root, "BS11")
    processed = record.processed["pubget"]
    passages = catalog.payload(catalog.artifact(ref.id, "passages", ""))
    assert processed.text == open(passages["full_text_path"], encoding="utf-8").read()
    assert "greater activation than controls" in processed.text
    assert [(a["table_id"], a["role"], a["role_source"]) for a in record.stage1["analyses"]] == [
        ("prose", "result", "results@1")]
    assert (settings.ns_pond_root / "pmids.tsv").read_text().split("\t")[:2] == ["11", "BS11"]
    # Beside stage1, the paper-parse files, which agree with each other and the text.
    from pyarty import read_bundle
    from study_schema.layouts import PaperParse, check_paper

    parse = read_bundle(PaperParse, settings.ns_pond_root / "BS11" / "parse")
    assert check_paper(parse, settings.ns_pond_root / "BS11") == []
    assert [(a.origin, a.role, a.role_source) for a in parse.coordinate_parse.analyses] == [
        ("text", "result", "results@1")]
    assert synced.summary["parse_id"] == parse.coordinate_parse.parse_id


def test_a_prose_restatement_of_a_table_is_among_the_written_parse_s_omissions(env, monkeypatch, tmp_path):
    """Its points are the table's, so it is not uploaded; the parse says which table analysis it restates."""
    from pyarty import read_bundle
    from study_schema.layouts import PaperParse

    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.services.nspond_schema import read_record

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="13"))
    _record_upstream(catalog, ref, path)
    tables = {"t1": {"slug": "s::t1", "analyses": [{"name": "Group effect", "table_id": "t1", "coordinates": [
        {"x": -22.0, "y": -4.0, "z": -18.0, "statistic_value": 4.1, "statistic_type": "T"}]}]}}
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload=tables, summary={"tables": 1})])
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _, (resolved,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert resolved.summary["restatements"] == 1 and "prose" not in resolved.payload
    _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS13", "study_id": "S13"})])
    sync = SyncStage(settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    assert synced.status is Status.OK, synced.error
    parse = read_bundle(PaperParse, settings.ns_pond_root / "BS13" / "parse").coordinate_parse
    assert [a.origin for a in parse.analyses if a.origin == "text"] == []
    (omission,) = parse.omitted_analyses
    assert omission.name == "patients > controls"
    assert omission.reason == "every point restates a table's result: table t1's 'Group effect' at (-22, -4, -18)"
    (span,) = omission.text_spans
    text = read_record(settings.ns_pond_root, "BS13").processed["pubget"].text
    assert text[span.start_char:span.end_char] == "\u221222, y = \u22124, z = \u221218"


def test_a_parse_that_fails_fails_the_sync(env, monkeypatch, tmp_path):
    """stage1 is written, but the article is not synced until its parse is."""
    from ingestion_workflow.pipeline.stages import sync as sync_stage
    from ingestion_workflow.services import paper_parse

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="12"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS12", "study_id": "S12"})])

    def broken(*args, **kwargs):
        raise ValueError("the parse addresses a different text")

    monkeypatch.setattr(sync_stage.paper_parse, "write", broken)
    sync = sync_stage.SyncStage(settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.FAILED
    assert "the parse addresses a different text" in synced.error
    assert (settings.ns_pond_root / "BS12" / "stage1").exists()

    # A new parser version re-syncs every article.
    upload = catalog.artifacts([ref.id], "upload")[ref.id][""]
    before = sync.fingerprint_for(upload)
    monkeypatch.setattr(paper_parse, "PAPER_PARSE_VERSION", paper_parse.PAPER_PARSE_VERSION + 1)
    assert sync.fingerprint_for(upload) != before


def test_an_article_whose_sets_have_no_role_is_neither_synced_nor_parsed(env, monkeypatch, tmp_path):
    """Without the roles stage's decision the article is blocked, and no parse is written."""
    from ingestion_workflow.pipeline.stages import sync as sync_stage

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="13"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    space = catalog.artifacts([ref.id], "resolve")[ref.id][""]
    # A space artifact carrying the resolve payload as it was before roles ran.
    catalog.record([
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="sp-1",
                payload=ctx.payload(space)),
        Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                summary={"base_study_id": "BS13", "study_id": "S13"}),
    ])
    sync = sync_stage.SyncStage(settings)
    upload = catalog.artifacts([ref.id], "upload")
    plan = sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"), upload)
    assert (plan.blocked, plan.pending) == (1, [])

    # Even with a roles artifact recorded, a set without its decision is refused before writing.
    catalog.record([Outcome(article_id=ref.id, stage="roles", source="", fingerprint="ro-1")])
    plan = sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"), upload)
    (synced,) = list(sync.execute(ctx, plan.pending))
    sync.finish()
    assert synced.status is Status.FAILED
    assert "sets without a role from the roles stage" in synced.error
    assert not (settings.ns_pond_root / "BS13" / "parse").exists()


def test_resolve_runs_again_when_the_tables_arrive(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="6"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-2",
                            payload={"t1": _table([(-22, -4, -18)])}, summary={"tables": 1})])
    plan, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert len(plan.pending) == 1 and merged.summary["restated"] == 1


def _synced_names(env, tmp_path, monkeypatch, declared, rerun=None, tables=None,
                  parse_write=None):
    """Sync one article whose stored tables carry the split marker when `declared`.

    `rerun(ref)`, when given, is an analyses outcome recorded after space ran,
    so the analyses artifact no longer matches the payload sync writes.
    """
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="12"))
    pts = [{"x": 1.0, "y": 2.0, "z": 3.0, "statistic_value": v, "statistic_type": "T"}
           for v in (4.0,)]
    neg = [{**pts[0], "statistic_value": -4.0}]
    tables = tables or {"t1": {"slug": "s::t1", "analyses": [
        {"name": "Load", "table_id": "t1", "coordinates": pts},
        {"name": "Load (negative)", "table_id": "t1", "coordinates": neg},
    ]}}
    _record_upstream(catalog, ref, path)
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload=tables, summary={"tables": 1})])
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
    if rerun is not None:
        catalog.record([rerun(ref, tables)])
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS12", "study_id": "S12"})])
    sync = SyncStage(settings)
    seen = {}
    assemble = sync._assemble

    def spy(*args, **kwargs):
        bundle, per_table, files = assemble(*args, **kwargs)
        seen.update(per_table)
        return bundle, per_table, files

    sync._assemble = spy
    if parse_write is not None:
        from ingestion_workflow.pipeline.stages import sync as sync_stage

        monkeypatch.setattr(sync_stage.paper_parse, "write", parse_write(seen))
    _, (synced,) = _run(sync, ctx, catalog, ref)
    assert synced.status is Status.OK, synced.error
    return [(x.name, x.metadata.get("split")) for x in seen["t1"].analyses]


def test_stage1_carries_the_split_as_the_parse_declares_it(env, tmp_path, monkeypatch):
    """stage1 is written after the parse, so its inverse half names the original by key."""
    from ingestion_workflow.pipeline.stages import sync as sync_stage

    point = {"x": 1.0, "y": 2.0, "z": 3.0, "statistic_value": 4.0, "statistic_type": "T"}
    tables = {"t1": {"slug": "s::t1", "analyses": [
        {"name": "Load > Rest", "table_id": "t1", "coordinates": [point],
         "metadata": {"split": {"half": "original", "index": 0}}},
        {"name": "Rest > Load", "table_id": "t1", "coordinates": [{**point, "x": 9.0}],
         "metadata": {"split": {"half": "inverse", "original_index": 0}}},
    ]}}
    real = sync_stage.paper_parse.write

    def parse_write(seen):
        def write(*args, splits=None, **kwargs):
            out = real(*args, splits=splits, **kwargs)
            # Whatever the parse declares is what stage1 carries.
            original, inverse = seen["t1"].analyses
            splits[id(inverse)] = {"half": "inverse", "original_analysis": "k-orig",
                                   "rule": "sign_of_directional_statistic"}
            return out
        return write

    _synced_names(env, tmp_path, monkeypatch, declared=True, tables=tables,
                  parse_write=parse_write)
    stage1 = json.loads((tmp_path / "pond" / "BS12" / "stage1" / "analyses.json").read_text())
    by_name = {a["name"]: a for a in stage1["analyses"]}
    assert by_name["Load > Rest"]["split"] == {
        "half": "original", "rule": "sign_of_directional_statistic"}
    assert by_name["Rest > Load"]["split"] == {
        "half": "inverse", "original_analysis": "k-orig",
        "rule": "sign_of_directional_statistic"}
    assert by_name["Rest > Load"]["points"][0]["values"] == [{"value": 4.0, "kind": "T"}]


def test_a_printed_negative_name_is_not_a_split_in_a_native_payload(env, tmp_path, monkeypatch):
    names = _synced_names(env, tmp_path, monkeypatch, declared=True)
    assert names == [("Load", None), ("Load (negative)", None)]


def test_a_declared_payload_stays_declared_after_analyses_fails(env, tmp_path, monkeypatch):
    """A failed re-run replaces the analyses artifact; the payload sync writes is still declared."""
    def rerun(ref, tables):
        return Outcome.failure(ref.id, "analyses", "", "model unavailable", fingerprint="an-2")

    names = _synced_names(env, tmp_path, monkeypatch, declared=True, rerun=rerun)
    assert names == [("Load", None), ("Load (negative)", None)]


def _record_extraction(catalog, ref, text_file, figure_captions=(), source="pubget", tables=0, hashed=True):
    from ingestion_workflow.pipeline.stages.extract import file_sha256

    summary = {"tables": tables, "has_text": True}
    if hashed:
        summary["text_sha256"] = file_sha256(text_file)
    catalog.record([Outcome(article_id=ref.id, stage="extract", source=source, fingerprint="ex-1",
                            payload={"full_text_path": str(text_file), "tables": [],
                                     "figure_captions": list(figure_captions), "slug": ref.identifier.slug,
                                     "source": source, "identifier": ref.identifier.to_dict(),
                                     "extracted_at": "2026-10-10T00:00:00"},
                            summary=summary)])


def test_passages_index_the_extraction_s_text_and_store_none_of_it(env, tmp_path):
    from ingestion_workflow.pipeline.stages.passages import passage_from

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="20"))
    _record_upstream(catalog, ref, path)
    text_file = tmp_path / "article.txt"
    # A table inlined in the text, its rows tab-separated, is left to the table path.
    text_file.write_text(TEXT + "Table 1\nAmygdala\t-22\t-4\t-18\n", encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    ctx = Context(settings, catalog)
    _, (found,) = _run(PassagesStage(settings), ctx, catalog, ref)
    payload = found.payload
    text = text_file.read_text()
    assert payload["full_text_path"] == str(text_file)
    (stored,) = payload["passages"]
    assert "text" not in stored
    passage = passage_from(stored, text)
    a, b = stored["span"]
    assert passage.text.startswith("Patients showed") and text[a:b].startswith("Patients showed")
    assert [text[h["span"][0]:h["span"][1]] for h in stored["hits"]] == ["x = -22, y = -4, z = -18"]
    assert passage.heading == "Results"

    # The text rewritten in place, and its hash with it: the passages are stale.
    text_file.write_text(text.replace("greater", "stronger"), encoding="utf-8")
    plan = PassagesStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "passages"),
                                        catalog.artifacts([ref.id], "extract"))
    assert plan.fresh == 1  # the summary still names the old text
    _record_extraction(catalog, ref, text_file)
    plan = PassagesStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "passages"),
                                        catalog.artifacts([ref.id], "extract"))
    assert len(plan.pending) == 1


def test_prose_reads_no_passage_of_a_text_that_changed_under_it(env, tmp_path):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="21"))
    _record_upstream(catalog, ref, path)
    text_file = tmp_path / "article.txt"
    text_file.write_text(TEXT, encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    ctx = Context(settings, catalog)
    _run(PassagesStage(settings), ctx, catalog, ref)
    text_file.write_text("## Results\nSomething else entirely.\n", encoding="utf-8")
    prose = ProseStage(settings)
    prose.client = lambda: _Reader()
    _, (read,) = _run(prose, ctx, catalog, ref)
    assert read.status is Status.FAILED and "changed" in read.error


LEGEND = ("Figure 2. Greater insula activation in patients than controls "
          "(x = &#x02212;34, y = 16, z = &#x02212;6; p &lt; 0.05 corrected).")


def test_a_legend_outside_the_results_is_read_and_marked(env, tmp_path):
    """A legend the download sets in the Discussion, which the extraction text sets after it."""
    from ingestion_workflow.pipeline.stages.passages import passage_from

    settings, catalog, _ = env
    xml = tmp_path / "legend.xml"
    xml.write_text(ARTICLE.replace("</p></sec>\n</body>", f"</p><fig><caption><p>{LEGEND}</p></caption></fig></sec>\n</body>"))
    ctx = Context(settings, catalog)

    # The legend where the extractor wrote it, after the Discussion.
    from ingestion_workflow.extractors.figure_captions import append_legends

    caption = "Figure 2. Greater insula activation in patients than controls (x = -34, y = 16, z = -6; p < 0.05 corrected)."
    ref = catalog.register(Identifier(pmid="31"))
    _record_upstream(catalog, ref, xml)
    text_file = tmp_path / "legend.txt"
    text, spans = append_legends(TEXT + "\n## Discussion\nPrior work found the amygdala at (x = 30, y = 2, z = -20).\n",
                                 [(["F2"], caption)])
    text_file.write_text(text, encoding="utf-8")
    _record_extraction(catalog, ref, text_file, spans)
    _, (found,) = _run(PassagesStage(settings), ctx, catalog, ref)
    stored = found.payload["passages"]
    assert [(p["hits"][0]["x"], p["from_legend"]) for p in stored] == [(-22.0, False), (-34.0, True)]
    assert passage_from(stored[1], text).from_legend
    assert text[slice(*stored[1]["span"])].startswith("Figure 2.")


def test_a_caption_is_read_where_the_extractor_wrote_it_not_where_the_text_first_quotes_it(env, tmp_path):
    """A caption the Results also print, and a second figure captioned alike: the caption is
    read once, at its span under "Figure legends", never searched for."""
    from ingestion_workflow.extractors.figure_captions import append_legends, dedupe

    settings, catalog, path = env
    caption = "Figure 2. Greater insula activation in patients than controls (x = -34, y = 16, z = -6)."
    body = TEXT.replace("The reverse contrast", caption + " The reverse contrast")
    text, spans = append_legends(body, dedupe([("F2", caption), ("F3", caption)]))
    assert [c["ids"] for c in spans] == [["F2", "F3"]]
    ref = catalog.register(Identifier(pmid="32"))
    _record_upstream(catalog, ref, path)
    text_file = tmp_path / "quoted.txt"
    text_file.write_text(text, encoding="utf-8")
    _record_extraction(catalog, ref, text_file, spans)
    _, (found,) = _run(PassagesStage(settings), ctx := Context(settings, catalog), catalog, ref)
    legend = [p for p in found.payload["passages"] if p["from_legend"]]
    assert len(legend) == 1
    assert legend[0]["span"][0] >= spans[0]["span"][0]
    # the body's copy is a body passage, read in the Results
    assert any(not p["from_legend"] and p["hits"][0]["x"] == -34.0 for p in found.payload["passages"])


def test_an_article_without_an_extraction_text_has_no_passages(env):
    """The extraction's text is the only one read: a download with no extraction is not
    read some other way, and sync has no text of its own to fall back on."""
    from ingestion_workflow.pipeline.stages.sync import _synced_extraction

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="40"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                            payload=_download(ref, path), summary={})])
    plan, outcomes = _run(PassagesStage(settings), Context(settings, catalog), catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    ctx = Context(settings, catalog)
    assert _synced_extraction(ctx, {}, catalog.artifacts([ref.id], "download")[ref.id], None) is None


def test_passages_index_the_judged_extraction_not_the_first(env, tmp_path):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="41"))
    ace, pubget = tmp_path / "ace.txt", tmp_path / "pubget.txt"
    ace.write_text("## Results\n\nNothing at (x = 1, y = 2, z = 3).\n", encoding="utf-8")
    pubget.write_text(TEXT, encoding="utf-8")
    _record_extraction(catalog, ref, ace, source="ace")  # first in the catalog's order
    _record_extraction(catalog, ref, pubget, source="pubget", tables=1)
    _, (found,) = _run(PassagesStage(settings), Context(settings, catalog), catalog, ref)
    assert (found.payload["source"], found.payload["full_text_path"]) == ("pubget", str(pubget))


def test_old_passages_of_an_article_with_no_extraction_text_are_taken_back(env, tmp_path):
    """Passages a download reader wrote, for an article whose extractor ran and found no
    text: the article shows as blocked, and prose takes back what it read from them."""
    from ingestion_workflow.pipeline.stages.passages import NO_TEXT

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="42"))
    old = tmp_path / "passages" / f"{ref.id}.txt"
    old.parent.mkdir()
    old.write_text(EXTRACTED, encoding="utf-8")
    catalog.record([
        Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                payload=_download(ref, path), summary={}),
        Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                payload={"full_text_path": None}, summary={"tables": 0, "has_text": False}),
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="old-reader",
                payload={"full_text_path": str(old), "passages": []}, summary={"passages": 1}),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={}, summary={}),
    ])
    ctx = Context(settings, catalog)
    plan, (taken,) = _run(PassagesStage(settings), ctx, catalog, ref)
    assert (taken.status, taken.error) == (Status.FAILED, NO_TEXT)
    assert catalog.artifacts([ref.id], "passages")[ref.id][""].status is Status.FAILED
    again, outcomes = _run(PassagesStage(settings), ctx, catalog, ref)
    assert (again.blocked, outcomes) == (1, [])
    prose, (back,) = _run(ProseStage(settings), ctx, catalog, ref)
    assert (prose.fresh, back.status, back.error) == (0, Status.FAILED, NO_TEXT)


def test_an_uploaded_article_whose_passages_are_taken_back_leaves_the_corpus_and_needs_a_retraction(
        env, monkeypatch, tmp_path):
    """Old prose with kept points, and the resolve, roles, space, upload and sync made from it:
    once passages are taken back, each stage takes back its own and sync moves the article out
    of the corpus. Neurostore's ingester has no route that withdraws a study, so upload records
    that a retraction is needed, with the base study and what it last sent, and then waits."""
    from ingestion_workflow.pipeline.upstream import RETRACTION_NEEDED

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="44"))
    pond = tmp_path / "pond"
    (pond / "bs-1" / "parse").mkdir(parents=True)
    (pond / "bs-1" / "parse" / "coordinate_parse.json").write_text("{}")
    catalog.record([
        Outcome.failure(ref.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={},
                summary={"kept": 2}),
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="old-resolve",
                payload={}, summary={"tables": 1}),
        Outcome(article_id=ref.id, stage="roles", source="", fingerprint="old-roles", payload={},
                summary={"tables": 1}),
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="old-space", payload={},
                summary={"tables": 1}),
        Outcome(article_id=ref.id, stage="upload", source="", fingerprint="old-upload",
                summary={"base_study_id": "bs-1", "analyses": 2}),
        Outcome(article_id=ref.id, stage="sync", source="", fingerprint="old-sync",
                summary={"base_study_id": "bs-1", "path": str(pond / "bs-1")}),
    ])
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error) == (stage.name, Status.FAILED, NO_TEXT)
    resolved = ResolveStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "resolve"),
                                           catalog.artifacts([ref.id], "prose"))
    assert (resolved.fresh, resolved.pending, resolved.blocked) == (0, [], 1)

    synced, uploaded, again = _sync_then_upload(settings, ctx, catalog, ref, pond=pond)
    assert (synced.status, synced.error, synced.fingerprint) == (Status.FAILED, NO_TEXT, NO_TEXT)
    assert not (pond / "bs-1").exists() and Path(synced.summary["moved_to"]).exists()
    assert (uploaded.error, uploaded.fingerprint) == (f"{RETRACTION_NEEDED}: {NO_TEXT}", NO_TEXT)
    assert uploaded.summary["base_study_id"] == "bs-1"
    assert uploaded.summary["last_upload"]["analyses"] == 2
    assert (again.blocked, again.reasons) == (1, {RETRACTION_NEEDED: 1})


def test_prose_taken_back_is_read_again_as_soon_as_its_passages_return(env):
    from ingestion_workflow.pipeline.stage import NO_TEXT

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="45"))
    catalog.record([
        Outcome.failure(ref.id, "prose", "", NO_TEXT, fingerprint=NO_TEXT),
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="new-text",
                payload={"passages": []}, summary={"passages": 0}),
    ])
    ctx = Context(settings, catalog, max_attempts=1)  # the take-back is not a failed try
    plan = ProseStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "prose"),
                                     catalog.artifacts([ref.id], "passages"))
    assert [w.fingerprint != NO_TEXT for w in plan.pending] == [True]


def test_an_extraction_that_records_no_text_hash_waits_and_its_file_is_not_hashed(env, tmp_path, monkeypatch):
    from ingestion_workflow.pipeline.stages import extract

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="43"))
    text_file = tmp_path / "t.txt"
    text_file.write_text(TEXT, encoding="utf-8")
    _record_extraction(catalog, ref, text_file, hashed=False)
    monkeypatch.setattr(extract, "file_sha256", lambda p: pytest.fail("the plan hashed a text"))
    plan, outcomes = _run(PassagesStage(settings), Context(settings, catalog), catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])


def _uploaded_chain(catalog, ref, *, upload=None, without=()):
    """passages taken back, with the prose, resolve, roles, space and upload made from the old
    text, less the stages named in `without`."""
    from ingestion_workflow.pipeline.stage import NO_TEXT

    rows = [
        Outcome.failure(ref.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="old-resolve", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="roles", source="", fingerprint="old-roles", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="old-space", payload={}, summary={}),
        upload or Outcome(article_id=ref.id, stage="upload", source="", fingerprint="old-upload",
                          summary={"base_study_id": "bs-1", "analyses": 2}),
    ]
    catalog.record([row for row in rows if row.stage not in without])


@pytest.mark.parametrize("extractions", [
    "downloaded files missing on disk: 1",
    "TimeoutError: the extractor timed out",
    "extractor returned nothing",
    "text file gone",
    "payload blob gone",
    "one source found no text, another failed",
])
def test_a_transient_failure_upstream_only_blocks_and_takes_nothing_back(env, tmp_path, monkeypatch, extractions):
    """A failed re-run of an article's only source, or its text file or blob gone, says
    something about the machine, not the article: the passages and everything made from
    them stay, so nothing reaches neurostore."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="46"))
    gone = tmp_path / "gone" / "article.txt"
    found = {"tables": 0, "has_text": True, "text_sha256": "abc"}
    rows = {
        "text file gone": [Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                                   payload={"full_text_path": str(gone)}, summary=found)],
        "payload blob gone": [Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                                      payload={"full_text_path": str(path)}, summary=found)],
        "one source found no text, another failed": [
            Outcome(article_id=ref.id, stage="extract", source="ace", fingerprint="ex-1",
                    payload={"full_text_path": None}, summary={"tables": 0, "has_text": False}),
            Outcome.failure(ref.id, "extract", "pubget", "TimeoutError", fingerprint="ex-2")],
    }.get(extractions) or [Outcome.failure(ref.id, "extract", "pubget", extractions, fingerprint="ex-1")]
    catalog.record(rows + [
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="old-passages",
                payload={"full_text_path": str(path), "passages": []}, summary={"passages": 1}),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={}, summary={}),
    ])
    ctx = Context(settings, catalog)
    if extractions == "payload blob gone":
        monkeypatch.setattr(ctx, "payload", lambda artifact: None)
    plan, outcomes = _run(PassagesStage(settings), ctx, catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    assert catalog.artifacts([ref.id], "passages")[ref.id][""].status is Status.OK


def test_a_relative_text_path_is_read_from_beside_the_catalog_whatever_the_cwd(env, tmp_path, monkeypatch):
    """Extractions run with a relative cache_root recorded `.cache/extract/...`; from another
    cwd those texts must still be found, not taken back."""
    from ingestion_workflow.pipeline.stages.passages import read_text

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="47"))
    text_file = tmp_path / ".cache" / "extract" / "article.txt"  # tmp_path holds the catalog, k/
    text_file.parent.mkdir(parents=True)
    text_file.write_text(EXTRACTED, encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    extraction = catalog.artifacts([ref.id], "extract")[ref.id]["pubget"]
    payload = dict(catalog.payload(extraction), full_text_path=".cache/extract/article.txt")
    catalog.record([Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                            payload=payload, summary=extraction.summary)])
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    ctx = Context(settings, catalog)
    plan, (made,) = _run(PassagesStage(settings), ctx, catalog, ref)
    assert made.status is Status.OK and made.summary["passages"] > 0
    assert made.payload["full_text_path"] == str(text_file)
    assert read_text(ctx, {"full_text_path": ".cache/extract/article.txt",
                           "text_sha256": made.payload["text_sha256"]}) == EXTRACTED


def test_the_take_back_runs_through_a_stage_that_last_failed_for_another_reason(env, monkeypatch):
    """prose last failed on a timeout while resolve still holds what an older prose made:
    both are taken back, so nothing downstream counts the old text fresh."""
    from ingestion_workflow.pipeline.stage import NO_TEXT

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="48"))
    _uploaded_chain(catalog, ref)
    catalog.record([Outcome.failure(ref.id, "prose", "", "timeout", fingerprint="newer-prose")])
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error) == (stage.name, Status.FAILED, NO_TEXT)


def _sync_then_upload(settings, ctx, catalog, ref, *, pond=None):
    """sync, then upload, with an ingester that fails the test if anything is sent."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.pipeline.stages.upload import UploadStage

    settings = settings.model_copy(update={"ns_pond_root": pond or Path(settings.data_root) / "pond"})
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, ref)
    upload = UploadStage(settings)
    upload.ingester = lambda: pytest.fail("a take-back was sent to neurostore")
    _, (uploaded,) = _run(upload, ctx, catalog, ref)
    again, outcomes = _run(upload, ctx, catalog, ref)
    assert outcomes == []  # recorded once
    return synced, uploaded, again


@pytest.mark.parametrize("roles", ["taken back", "failed", "missing"])
def test_roles_that_are_not_ok_beside_an_ok_space_only_block_the_sync(env, monkeypatch, roles):
    """Roles taken back while space has not run since, or failed, or never run: sync waits
    for them, so no parse is written for upload to send, and nothing is taken back."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="51"))
    rows = [
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="old-space", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="upload", source="", fingerprint="old-upload",
                summary={"base_study_id": "bs-1", "analyses": 2}),
    ]
    if roles == "taken back":
        rows.append(Outcome.failure(ref.id, "roles", "", NO_TEXT, fingerprint=NO_TEXT))
    elif roles == "failed":
        rows.append(Outcome.failure(ref.id, "roles", "", "no role model", fingerprint="r-1"))
    catalog.record(rows)
    plan, outcomes = _run(SyncStage(settings), Context(settings, catalog), catalog, ref)
    assert (plan.blocked, plan.pending, outcomes) == (1, [], [])


def test_roles_read_each_passage_from_the_text_its_spans_index(env, tmp_path):
    """The passages payload stores spans, not text: the roles classifier is given the
    passage, heading and neighbours as the text prints them, and a text that changed
    under the spans fails the article."""
    from ingestion_workflow.pipeline.stages.roles import _read_passages

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="52"))
    _record_upstream(catalog, ref, path)
    text_file = tmp_path / "article.txt"
    text_file.write_text(TEXT, encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    ctx = Context(settings, catalog)
    _, (found,) = _run(PassagesStage(settings), ctx, catalog, ref)
    (read,) = _read_passages(ctx, found.payload)
    assert read["text"].startswith("Patients showed") and read["heading"] == "Results"
    assert read["text"] in TEXT
    assert _read_passages(ctx, None) == []
    text_file.write_text(TEXT.replace("greater", "stronger"), encoding="utf-8")
    with pytest.raises(LookupError):
        _read_passages(ctx, found.payload)


def test_the_take_back_runs_through_a_stage_that_never_made_anything(env, monkeypatch):
    """Roles never ran (no model was configured), but the space and upload made before roles
    existed hold the old text's analyses: roles records the take-back, so space, sync and
    upload take back theirs, and upload records that the study needs a retraction."""
    from ingestion_workflow.pipeline.upstream import RETRACTION_NEEDED

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="53"))
    _uploaded_chain(catalog, ref, without=("roles",))
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error) == (stage.name, Status.FAILED, NO_TEXT)
    again, outcomes = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    assert (again.blocked, outcomes) == (1, [])  # recorded once
    synced, uploaded, _ = _sync_then_upload(settings, ctx, catalog, ref)
    assert synced.error == NO_TEXT
    assert (uploaded.error, uploaded.summary["base_study_id"]) == (
        f"{RETRACTION_NEEDED}: {NO_TEXT}", "bs-1")


def test_a_take_back_with_no_upload_of_its_own_asks_nothing_of_neurostore(env, monkeypatch):
    """The article was never uploaded by this pipeline, though neurostore knows it: the
    take-back is recorded and nothing is sent."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="54", neurostore="bs-9"))
    _uploaded_chain(catalog, ref, without=("upload",))
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        _run(stage, ctx, catalog, ref)
    _, taken, _ = _sync_then_upload(settings, ctx, catalog, ref)
    assert (taken.status, taken.error, taken.fingerprint) == (Status.FAILED, NO_TEXT, NO_TEXT)
    assert "retraction_needed" not in taken.summary


def test_an_upload_that_recorded_no_base_study_is_taken_back_by_the_article_s_neurostore_id(
        env, monkeypatch, tmp_path):
    """Sync finds the corpus copy by the article's neurostore id and moves it aside."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="55", neurostore="bs-9"))
    _uploaded_chain(catalog, ref, upload=Outcome(article_id=ref.id, stage="upload", source="",
                                                 fingerprint="old-upload", summary={"analyses": 2}))
    pond = tmp_path / "pond"
    (pond / "bs-9").mkdir(parents=True)
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        _run(stage, ctx, catalog, ref)
    synced, _, _ = _sync_then_upload(settings, ctx, catalog, ref, pond=pond)
    assert synced.summary["base_study_id"] == "bs-9" and not (pond / "bs-9").exists()


@pytest.mark.parametrize("failed, error", [
    ("passages", "timeout"),
    ("passages", "unreadable: UnicodeDecodeError"),
    ("prose", "TimeoutError: the model timed out"),
    ("prose", "1 of 3 passages failed"),
    ("resolve", "KeyError: 'tables'"),
    ("roles", "no role for its sets: the text role model is missing"),
    ("space", "ConnectionError: the tunnel dropped"),
])
def test_a_stage_that_failed_for_another_reason_only_blocks_the_next(env, monkeypatch, failed, error):
    """A timed-out or broken run in the middle of the chain says nothing about the article's
    text: the stage after it waits, and nothing made from the old text is taken back."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="56"))
    _uploaded_chain(catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint="old-passages",
                            payload={"passages": []}, summary={"passages": 1}),
                    Outcome.failure(ref.id, failed, "", error, fingerprint=f"newer-{failed}")])
    after = {"passages": ProseStage(settings), "prose": ResolveStage(settings),
             "resolve": _roles(settings, monkeypatch), "roles": SpaceStage(settings),
             "space": SyncStage(settings)}[failed]
    plan, outcomes = _run(after, Context(settings, catalog), catalog, ref)
    assert (plan.blocked, plan.pending, outcomes) == (1, [], [])


@pytest.mark.parametrize("space, synced", [("taken back", True), ("ok", False)])
def test_only_a_study_retracted_for_its_text_leaves_the_corpus_without_roles(env, space, synced):
    """An article with no roles row waits for its roles like any other; one whose analyses
    were taken back with its text has none to wait for, and sync takes it out of the corpus."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="57", neurostore="bs-1"))
    catalog.record([Outcome.failure(ref.id, "space", "", NO_TEXT, fingerprint=NO_TEXT)
                    if space == "taken back" else
                    Outcome(article_id=ref.id, stage="space", source="", fingerprint="sp-1",
                            summary={"tables": 1})])
    ctx = Context(settings, catalog)
    sync = SyncStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                                    catalog.artifacts([ref.id], "space"))
    assert (len(sync.pending), sync.blocked) == ((1, 0) if synced else (0, 1))


def test_roles_waits_for_prose_to_read_the_passages_it_would_index(env, monkeypatch):
    """Passages ran again and prose has not: the prose sets' indices point into the old list,
    so roles waits, and plans again with the pair once prose reads the new one."""
    from ingestion_workflow.pipeline.stages.roles import PROSE_READ_OTHER_PASSAGES

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="58"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    roles = _roles(settings, monkeypatch)
    _, (first,) = _run(roles, ctx, catalog, ref)
    assert first.status is Status.OK
    found = catalog.artifacts([ref.id], "passages")[ref.id][""]
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint="newer-passages",
                            payload=catalog.payload(found), summary=found.summary)])
    plan, outcomes = _run(roles, ctx, catalog, ref)
    assert (outcomes, plan.reasons) == ([], {PROSE_READ_OTHER_PASSAGES: 1})
    prose = ProseStage(settings)
    prose.client = lambda: _Reader()
    _run(prose, ctx, catalog, ref)
    _run(ResolveStage(settings), ctx, catalog, ref)
    _, (again,) = _run(roles, ctx, catalog, ref)
    assert again.status is Status.OK and again.fingerprint != first.fingerprint


def _roles_after_a_chain(env, monkeypatch, pmid):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid=pmid))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    roles = _roles(settings, monkeypatch)
    _, (first,) = _run(roles, ctx, catalog, ref)
    assert first.status is Status.OK
    return settings, catalog, ref, ctx, roles, first


def test_roles_waits_for_resolve_to_read_the_prose_it_classifies(env, monkeypatch):
    """Prose re-read new passages and now holds no point (w2ni664ashw2): resolve takes back the
    row made from the earlier reading, and roles takes back its own, never writing on indices
    into the old list."""
    from ingestion_workflow.pipeline.stage import NO_ANALYSES, NO_TEXT

    settings, catalog, ref, ctx, roles, first = _roles_after_a_chain(env, monkeypatch, "61")
    found = catalog.artifacts([ref.id], "passages")[ref.id][""]
    # The passage now starts a character later: its text changed, so prose reads it again.
    moved = catalog.payload(found)
    moved["passages"][0]["span"][0] += 1
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint="newer-passages",
                            payload=moved, summary=found.summary)])

    class NoPoint:
        def read(self, passage, *, title="", abstract=""):
            return {"space": None, "analyses": []}

    prose = ProseStage(settings)
    prose.client = lambda: NoPoint()
    _run(prose, ctx, catalog, ref)
    _, (resolved,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert (resolved.status, resolved.error, resolved.fingerprint) == (
        Status.FAILED, NO_ANALYSES, NO_TEXT)
    _, (roled,) = _run(roles, ctx, catalog, ref)
    assert (roled.status, roled.error, roled.fingerprint) == (Status.FAILED, NO_ANALYSES, NO_TEXT)


def test_resolve_records_the_prose_it_was_made_from(env, monkeypatch):
    settings, catalog, ref, ctx, roles, first = _roles_after_a_chain(env, monkeypatch, "62")
    resolved = catalog.artifacts([ref.id], "resolve")[ref.id][""]
    assert resolved.summary["prose_read"] == catalog.artifacts([ref.id], "prose")[ref.id][""].fingerprint


def test_roles_plans_again_when_the_spans_change_under_the_same_passages(env, monkeypatch):
    """A passages code change that kept its fingerprint moved the spans prose's indices point
    into; resolve and prose are as they were, and only roles' own fingerprint can notice."""
    settings, catalog, ref, ctx, roles, first = _roles_after_a_chain(env, monkeypatch, "63")
    found = catalog.artifacts([ref.id], "passages")[ref.id][""]
    plan, _ = _run(roles, ctx, catalog, ref)
    assert plan.fresh == 1
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint=found.fingerprint,
                            payload=catalog.payload(found),
                            summary={**found.summary, "spans": "moved-spans"})])
    plan = roles.plan(ctx, [ref], catalog.artifacts([ref.id], "roles"), catalog.artifacts([ref.id], "resolve"))
    assert (plan.fresh, len(plan.pending)) == (0, 1)
    assert plan.pending[0].fingerprint != first.fingerprint


def test_the_spans_digest_follows_the_spans_and_the_text():
    from ingestion_workflow.pipeline.stages.passages import spans_digest

    one = [{"span": [0, 5], "before": None, "after": None, "heading": None}]
    other = [{"span": [0, 6], "before": None, "after": None, "heading": None}]
    assert len({spans_digest("t", one), spans_digest("t", other), spans_digest("u", one)}) == 3


def test_a_passage_with_a_hit_outside_the_legends_is_not_from_a_legend(env, tmp_path, monkeypatch):
    """The marker needs every hit in a legend. Passages are read span by span, so no real text
    gives one passage hits on both sides; this one is made to."""
    from types import SimpleNamespace

    from ingestion_workflow.extractors.figure_captions import append_legends
    from ingestion_workflow.pipeline.stages import passages

    text, spans = append_legends(TEXT, [(["F2"], "Figure 2. Insula (x = -34, y = 16, z = -6).")])
    path = tmp_path / "mixed.txt"
    path.write_text(text, encoding="utf-8")
    legend_at = spans[0]["span"][0]

    def mixed(_text, a, b):
        return [SimpleNamespace(hits=[SimpleNamespace(span=(legend_at, legend_at + 1)),
                                      SimpleNamespace(span=(0, 1))], from_legend=None)]

    monkeypatch.setattr(passages, "_shifted", mixed)
    _, found, _, _ = passages.find_passages({"text_path": str(path), "figure_captions": spans})
    assert found and not any(p.from_legend for p in found)


def test_passages_over_the_cap_are_recorded_with_their_reason(env, tmp_path, monkeypatch):
    from ingestion_workflow.pipeline.stages import passages

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="59"))
    filler = " ".join(["The blocks of trials were presented in random order."] * 30)
    second = "The right insula responded more (x = 40, y = 12, z = 2; t = 3.9).\n"
    text_file = tmp_path / "article.txt"
    text_file.write_text(f"{TEXT}\n{filler}\n\n{second}", encoding="utf-8")
    _record_extraction(catalog, ref, text_file)
    ctx = Context(settings, catalog)
    _, (whole,) = _run(PassagesStage(settings), ctx, catalog, ref)
    assert whole.summary["omitted"] == 0 and whole.payload["omitted"] == []
    kept = whole.summary["passages"]
    assert kept == 2
    monkeypatch.setattr(passages, "MAX_PASSAGES", kept - 1)
    _, (cut,) = _run(PassagesStage(settings), Context(settings, catalog, refresh=["passages"]), catalog, ref)
    assert (cut.summary["passages"], cut.summary["omitted"]) == (kept - 1, 1)
    (left_out,) = cut.payload["omitted"]
    assert left_out["span"] == whole.payload["passages"][-1]["span"]
    assert left_out["reason"] == f"over the {kept - 1}-passage cap"


def test_sync_hands_the_parse_the_passages_left_out_over_the_cap(env):
    from collections import defaultdict

    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.sync import _parse_inputs

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="60"))
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint="p-1", summary={},
                            payload={"passages": [{"span": [0, 1]}],
                                     "omitted": [{"span": [2, 3], "reason": "over the 40-passage cap"}]})])
    found = defaultdict(dict, passages=catalog.artifacts([ref.id], "passages")[ref.id])
    work = Work(ref=ref, source="", fingerprint="s-1", upstream=None)
    inputs = _parse_inputs(Context(settings, catalog), work, "bs-1", {}, found, "pubget")
    assert (inputs.passages_kept, inputs.passages_omitted) == (1, 1)


class _NothingFound(_Reader):
    """The passage names a contrast and reports no peak for it."""

    def read(self, passage, *, title="", abstract=""):
        return {"space": "MNI", "analyses": [{"name": "controls > patients", "measure": None, "points": []}]}


def test_a_prose_only_article_with_only_empty_contrasts_is_resolved(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="71"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _, (read,) = _read_prose(ctx, catalog, ref, _NothingFound())
    assert read.summary["coordinates"] == 0 and read.summary["analyses"] == 1
    plan, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert len(plan.pending) == 1 and plan.skipped == 0
    assert [(a["name"], a["coordinates"]) for a in merged.payload["prose"]["analyses"]] == [
        ("controls > patients", [])]


def test_a_new_clean_rereads_the_stored_answers_not_the_model(env, monkeypatch):
    from ingestion_workflow.pipeline.stages import prose as prose_stage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="72"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _, (first,) = _read_prose(ctx, catalog, ref, _Reader())
    assert first.payload["passages"][0]["answer"]["analyses"][0]["name"] == "patients > controls"
    monkeypatch.setattr(prose_stage, "CLEAN_VERSION", "another clean")
    plan, (again,) = _read_prose(ctx, catalog, ref, _Reader(fail=True))
    assert len(plan.pending) == 1 and again.status is Status.OK
    assert again.fingerprint != first.fingerprint
    assert again.summary["read_fingerprint"] == first.summary["read_fingerprint"]
    assert again.payload["passages"] == first.payload["passages"]


def test_a_new_prompt_reads_with_the_model_again(env, monkeypatch):
    from ingestion_workflow.pipeline.stages import prose as prose_stage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="73"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    monkeypatch.setattr(prose_stage, "PROSE_PROMPT_VERSION", "another prompt")
    _, (again,) = _read_prose(ctx, catalog, ref, _Reader(fail=True))
    assert again.status is not Status.OK


class _V2Reader:
    """Answers as nu-prose-v2 does: each hit under two contrasts, twice in the first."""

    def read(self, passage, *, title="", abstract=""):
        points = [{"x": h.x, "y": h.y, "z": h.z, "statistic": "T", "value": 4.1,
                   "cluster_size": None} for h in passage.hits]
        return {"space": None, "analyses": [
            {"name": "patients > controls", "measure": None, "points": points * 2},
            {"name": "patients > controls, covarying for IQ", "measure": None, "points": points}]}


def test_resolve_makes_a_repeat_within_a_set_one_point(env):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="31"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _, (read,) = _read_prose(ctx, catalog, ref, _V2Reader())
    assert read.status is Status.OK and read.summary["coordinates"] == 3
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    one, other = merged.payload["prose"]["analyses"]
    assert [(c["x"], c["y"], c["z"]) for c in one["coordinates"]] == [(-22.0, -4.0, -18.0)]
    assert one["metadata"]["dropped"] == [
        {"x": -22.0, "y": -4.0, "z": -18.0, "reason": "repeats a point of this analysis"}]
    assert "other_statistics" not in one["coordinates"][0]   # the repeat gave the same statistic
    # The repeat across contrasts is not a repeat: each keeps the point.
    assert [(c["x"], c["y"], c["z"]) for c in other["coordinates"]] == [(-22.0, -4.0, -18.0)]
    assert (merged.summary["repeated"], merged.summary["merged_statistics"]) == (1, 0)


def test_a_place_given_with_three_statistics_is_one_point_carrying_all_three():
    """nmb-112, as nu-prose-v2 answered it: each peak once each as t, Z and p."""
    from ingestion_workflow.clients.prose_coordinates import clean_answer

    case = json.loads((Path(__file__).parents[1] / "data" / "prose" / "nmb_112.json")
                      .read_text(encoding="utf-8"))
    answer = clean_answer(case["answer"], case["text"])
    prose = {"space": "MNI", "passages": [{"heading": None, "space": "MNI", **answer}]}
    out, summary = resolve({}, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    places = [(-12.0, -2.0, -16.0), (-60.0, -42.0, 4.0), (64.0, -30.0, -10.0)]
    assert [(c["x"], c["y"], c["z"], c["statistic_type"], c["statistic_value"])
            for c in analysis["coordinates"]] == [
        (*places[0], "T", 6.24), (*places[1], "T", 4.09), (*places[2], "T", 3.71)]
    assert [c["other_statistics"] for c in analysis["coordinates"]] == [
        [{"statistic_type": k, "statistic_value": v} for k, v in zip("ZP", values)]
        for values in [(4.17, 0.001), (3.22, 0.001), (3.01, 0.001)]]
    assert len(analysis["metadata"]["dropped"]) == 6
    counts = [summary[k] for k in ("prose_points", "repeated", "merged_statistics")]
    assert counts == [3, 6, 3]


def test_a_place_repeated_across_sets_stays_in_each_after_resolve():
    """nmb-164: one Talairach peak reported for three contrasts."""
    names = ["Affect x Group interaction", "libertarians > determinists", "main effect of Affect"]
    prose = {"passages": [_passage(*[(n, [(50, -50, 15, "result")]) for n in names])]}
    out, summary = resolve({}, prose, "slug")
    assert [(a["name"], [(c["x"], c["y"], c["z"]) for c in a["coordinates"]])
            for a in out["prose"]["analyses"]] == [(n, [(50.0, -50.0, 15.0)]) for n in names]
    assert (summary["repeated"], summary["merged_statistics"]) == (0, 0)


def _given(*points):
    """One passage, one set, its points given as (x, y, z, statistic, value)."""
    return {"passages": [{"text": "...", "heading": None, "space": "MNI", "error": None,
                          "analyses": [{"name": "patients > controls", "measure": None, "points": [
                              {"x": x, "y": y, "z": z, "statistic": k, "value": v,
                               "cluster_size": None} for x, y, z, k, v in points]}]}]}


def test_points_of_a_set_a_millimetre_apart_or_at_another_z_stay_two():
    places = [(10, 20, 30), (10, 20, -30), (-22.6, 40.4, 8), (-22.4, 40.6, 8)]
    out, summary = resolve({}, _given(*[(*xyz, "T", 4.1) for xyz in places]), "slug")
    (analysis,) = out["prose"]["analyses"]
    assert [(c["x"], c["y"], c["z"]) for c in analysis["coordinates"]] == [
        tuple(float(v) for v in xyz) for xyz in places]
    assert "dropped" not in analysis["metadata"] and summary["repeated"] == 0


def test_a_place_given_as_t_and_z_is_one_point_carrying_both():
    out, summary = resolve({}, _given((-12, -2, -16, "T", 6.24), (-12, -2, -16, "Z", 4.17)), "slug")
    (coordinate,) = out["prose"]["analyses"][0]["coordinates"]
    assert (coordinate["statistic_type"], coordinate["statistic_value"]) == ("T", 6.24)
    assert coordinate["other_statistics"] == [{"statistic_type": "Z", "statistic_value": 4.17}]
    assert (summary["repeated"], summary["merged_statistics"]) == (1, 1)


def test_a_repeat_with_no_number_adds_nothing():
    out, summary = resolve({}, _given((-12, -2, -16, "T", 6.24), (-12, -2, -16, "P", None)),
                           "slug")
    (coordinate,) = out["prose"]["analyses"][0]["coordinates"]
    assert "other_statistics" not in coordinate
    assert (summary["repeated"], summary["merged_statistics"]) == (1, 0)


@pytest.mark.parametrize("given, sign", [
    ([("T", 6.24), ("Z", 4.17), ("P", 0.001)], "positive"),
    ([("T", -4.2), ("P", 0.001)], "negative"),
    ([("P", 0.001), ("T", -4.2), ("Z", -3.1)], "negative"),
])
def test_the_sign_of_a_merged_place_is_read_from_every_statistic_not_the_last(given, sign):
    from ingestion_workflow.models.analysis import Coordinate

    out, _ = resolve({}, _given(*[(-12, -2, -16, k, v) for k, v in given]), "slug")
    (stored,) = out["prose"]["analyses"][0]["coordinates"]
    assert Coordinate.from_dict(stored).sign == sign


def test_every_statistic_of_a_merged_place_reaches_its_parsed_point():
    """Given p first, then t and Z: the parse's point holds all three, and it is positive."""
    from types import SimpleNamespace

    from ingestion_workflow.models.analysis import Coordinate
    from ingestion_workflow.services.paper_parse import _point

    out, _ = resolve({}, _given((-12, -2, -16, "P", 0.001), (-12, -2, -16, "T", 6.24),
                                (-12, -2, -16, "Z", 4.17)), "slug")
    (stored,) = out["prose"]["analyses"][0]["coordinates"]
    coordinate = Coordinate.from_dict(stored)
    point = _point(coordinate, SimpleNamespace(coordinate_space=None), found=None)
    assert [(v.kind, v.value) for v in point.values] == [("p", 0.001), ("t", 6.24), ("z", 4.17)]
    assert coordinate.sign == stored["sign"] == "positive"


def test_a_nu_prose_v2_reading_reaches_space_with_roles_from_the_roles_stage(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="32"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _V2Reader())
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.status is Status.OK and merged.summary["prose_points"] == 2
    assert [a["name"] for a in merged.payload["prose"]["analyses"]] == [
        "patients > controls", "patients > controls, covarying for IQ"]
    _, (roled,) = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    assert roled.status is Status.OK and roled.summary["sets_by_origin"] == {"text": 2}
    _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
    roles = [a["metadata"]["set_role"] for a in spaced.payload["prose"]["analyses"]]
    assert [(r["role"], r["role_origin"]) for r in roles] == [("result", "text")] * 2


TWO_PASSAGES = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = -22, y = -4, z = -18; t = 4.1).\n\n"
    + "Behaviour did not differ between the groups on any measure we collected. " * 12 + "\n\n"
    "Controls showed greater activation than patients in the precuneus (x = 6, y = -60, z = 40; "
    "t = 3.9).\n"
)


def _two_passages(env, pmid):
    """An article whose passages were found, and the prose stage that reads them."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid=pmid))
    _record_upstream(catalog, ref, path, text=TWO_PASSAGES)
    ctx = Context(settings, catalog)
    _run(PassagesStage(settings), ctx, catalog, ref)
    found = catalog.payload(catalog.artifacts([ref.id], "passages")[ref.id][""])
    assert len(found["passages"]) == 2
    return settings, catalog, ref, ctx, found


def _earlier_prose(catalog, ref, texts, fingerprint="earlier-prose"):
    """A prose result as nu-prose's first corpus run stored it, before passages were spans:
    the text and the analyses cleaned from its answer, as that answer gave them; no answer
    stored, no producer."""
    passages = [{"text": text, "heading": "Results", "space": "MNI", "error": None, "analyses": [
        {"name": name, "measure": None, "points": [
            {"x": x, "y": y, "z": z, "statistic": "T", "value": v, "cluster_size": None,
             "role": "result"}] * 2}]}
        for text, (name, (x, y, z, v)) in texts]
    catalog.record([Outcome(article_id=ref.id, stage="prose", source="", fingerprint=fingerprint,
                            payload={"source": "pubget", "read": "methods+results",
                                     "space": "MNI", "passages": passages},
                            summary={"passages": len(passages)})])


def _texts(catalog, ref, found):
    from ingestion_workflow.pipeline.stages.passages import passage_from

    text = (catalog.artifacts([ref.id], "extract")[ref.id]["pubget"])
    text = open(catalog.payload(text)["full_text_path"], encoding="utf-8").read()
    return [passage_from(p, text).text for p in found["passages"]]


def _prose(settings, reader):
    stage = ProseStage(settings)
    stage.client = lambda: reader
    return stage


class _Counting(_Reader):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def read(self, passage, *, title="", abstract=""):
        self.calls += 1
        return super().read(passage, title=title, abstract=abstract)


def test_an_earlier_reading_of_the_same_passage_text_is_reused_at_its_new_span(env):
    """The corpus's prose was read before passages were spans: the same text is not read again."""
    settings, catalog, ref, ctx, found = _two_passages(env, "81")
    first, second = _texts(catalog, ref, found)
    _earlier_prose(catalog, ref, [(first, ("patients > controls", (-22, -4, -18, 4.1))),
                                 (second, ("controls > patients", (6, -60, 40, 3.9)))])
    _, (read,) = _run(_prose(settings, _Reader(fail=True)), ctx, catalog, ref)
    assert read.status is Status.OK
    assert read.summary["reused"] == [0, 1]
    assert read.summary["read_again"] == {}
    assert read.summary["prompt_versions"] == {"2026-10-06.n5": 2}
    # re-anchored: the new spans, the earlier names and points
    assert [p["span"] for p in read.payload["passages"]] == [p["span"] for p in found["passages"]]
    assert [[(a["name"], [(q["x"], q["y"], q["z"]) for q in a["points"]])
             for a in p["analyses"]] for p in read.payload["passages"]] == [
        [("patients > controls", [(-22.0, -4.0, -18.0)] * 2)],
        [("controls > patients", [(6.0, -60.0, 40.0)] * 2)]]   # resolve makes each repeat one
    # credited to nu-prose under the prompt it read with then
    assert {(p["producer"]["model"], p["producer"]["prompt_version"])
            for p in read.payload["passages"]} == {("nu-prose", "2026-10-06.n5")}


def test_a_passage_whose_text_changed_is_read_again_and_counted(env):
    settings, catalog, ref, ctx, found = _two_passages(env, "82")
    first, second = _texts(catalog, ref, found)
    _earlier_prose(catalog, ref, [(first, ("patients > controls", (-22, -4, -18, 4.1))),
                                 (second.replace("3.9", "3.8"), ("controls > patients",
                                                                 (6, -60, 40, 3.8)))])
    reader = _Counting()
    _, (read,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 1
    assert read.summary["reused"] == [0]
    assert read.summary["read_again"] == {"passage text changed": 1}
    assert read.summary["prompt_versions"] == {"2026-10-06.n5": 1, "2026-10-11.nu-prose-v2": 1}
    # the changed passage holds the model's new answer, not the old one's 3.8
    values = [q["value"] for a in read.payload["passages"][1]["analyses"] for q in a["points"]]
    assert values == [4.1]


def test_each_passage_read_is_counted_with_its_reason(env):
    settings, catalog, ref, ctx, found = _two_passages(env, "83")
    reader = _Counting()
    _, (read,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 2 and read.summary["read_again"] == {"no earlier result": 2}
    assert read.summary["reused"] == []
    producer = read.payload["passages"][0]["producer"]
    assert (producer["prompt_version"], producer["model"], producer["stage"]) == (
        "2026-10-11.nu-prose-v2", "nu-prose-v2", "prose")
    assert read.summary["text_normalization"] == "nfkc-minus-space.1"
    # what each answer was read from: the passage's own text first
    from ingestion_workflow.services.offsets import sha256

    assert [p["inputs"][0] for p in read.payload["passages"]] == [
        {"artifact_kind": "passage_text", "fingerprint": sha256(p["text"]),
         "producer_version": None} for p in read.payload["passages"]]
    # read again under new passages of the same text: reused, the payload unchanged
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="", fingerprint="newer",
                            payload=found, summary={"passages": 2})])
    reader = _Counting()
    _, (again,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 0 and again.summary["reused"] == [0, 1]
    assert again.payload["passages"] == read.payload["passages"]
    # a new title and abstract: read again
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-2",
                            payload={"title": "T2", "abstract": "A2"}, summary={})])
    reader = _Counting()
    _, (third,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 2 and third.summary["read_again"] == {"title and abstract changed": 2}


@pytest.mark.parametrize("model, prompt_version, reason", [
    ("nu-prose", "2026-10-06.n5", None),
    ("nu-prose", "2026-09-30.n4", "earlier model not interchangeable"),
    ("nu-prose-v2", "2026-10-06.n5", "earlier prompt not interchangeable"),
    ("another-prose-model", "2026-10-11.nu-prose-v2", "earlier model not interchangeable"),
])
def test_only_nu_prose_s_first_answers_and_the_reader_s_own_are_reused(
        env, model, prompt_version, reason):
    settings, catalog, ref, ctx, found = _two_passages(env, "84")
    _, (read,) = _run(_prose(settings, _Reader()), ctx, catalog, ref)
    blob = json.loads(json.dumps(read.payload))
    for p in blob["passages"]:
        p["producer"] = {**p["producer"], "model": model, "prompt_version": prompt_version}
    catalog.record([Outcome(article_id=ref.id, stage="prose", source="", fingerprint="earlier",
                            payload=blob, summary=read.summary)])
    reader = _Counting()
    _, (again,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert again.status is Status.OK
    if reason is None:
        assert reader.calls == 0 and again.summary["prompt_versions"] == {prompt_version: 2}
    else:
        assert reader.calls == 2 and again.summary["read_again"] == {reason: 2}


def test_nu_prose_s_first_answers_are_read_again_under_a_later_prompt(env, monkeypatch):
    from ingestion_workflow.pipeline.stages import prose

    settings, catalog, ref, ctx, found = _two_passages(env, "86")
    first, second = _texts(catalog, ref, found)
    _earlier_prose(catalog, ref, [(first, ("patients > controls", (-22, -4, -18, 4.1))),
                                 (second, ("controls > patients", (6, -60, 40, 3.9)))])
    monkeypatch.setattr(prose, "PROSE_PROMPT_VERSION", "2026-11-01.nu-prose-v3")
    reader = _Counting()
    _, (read,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 2
    assert read.summary["read_again"] == {"earlier model not interchangeable": 2}


def test_results_cleaned_and_resolved_before_nu_prose_v2_are_made_again(env):
    """Stored answers are cleaned again without the model, and resolves made before
    statistics were merged are made again: both versions moved on from main's."""
    from ingestion_workflow.catalog import fingerprint
    from ingestion_workflow.pipeline.stages.resolve import SAME_PEAK_MM, SAME_VALUE

    settings, catalog, ref, ctx, found = _two_passages(env, "99")
    passages = catalog.artifacts([ref.id], "passages")[ref.id][""]
    metadata = catalog.artifacts([ref.id], "metadata")[ref.id][""]
    stage = ProseStage(settings)
    read = stage.read_fingerprint(passages, metadata)
    assert stage.fingerprint_for(passages, metadata) != fingerprint(
        "prose clean", "named-empty-kept", upstream=read)
    prose = Outcome(article_id=ref.id, stage="prose", source="", fingerprint="p", summary={})
    catalog.record([prose])
    prose = catalog.artifacts([ref.id], "prose")[ref.id][""]
    assert ResolveStage(settings).fingerprint_for(prose, None) != fingerprint(
        "resolve", 9, SAME_PEAK_MM, SAME_VALUE, "no tables", upstream=prose.fingerprint)


def test_the_normalized_text_is_what_a_re_extraction_changes_and_a_reader_does_not():
    from ingestion_workflow.services.prose_text import normalized

    assert normalized("(x\u00a0=\u2009\u221222,\n y = \u20134; \ufb01ndings\u207b)") == (
        "(x = -22, y = -4; findings-)")
    assert normalized("( x = -22)") != normalized("(x = -22)")
    assert normalized("T = 5") != normalized("t = 5")


def test_a_passage_equal_once_normalized_takes_the_earlier_answer(env):
    """The old text has a unicode minus and a no-break space where the new one has neither."""
    settings, catalog, ref, ctx, found = _two_passages(env, "86")
    first, second = _texts(catalog, ref, found)
    old = first.replace("-22", "\u221222").replace(" ", "\u00a0", 1)
    assert old != first
    _earlier_prose(catalog, ref, [(old, ("patients > controls", (-22, -4, -18, 4.1))),
                                 (second, ("controls > patients", (6, -60, 40, 3.9)))])
    _, (read,) = _run(_prose(settings, _Reader(fail=True)), ctx, catalog, ref)
    assert read.status is Status.OK
    assert read.summary["reused"] == [0, 1]
    assert read.payload["passages"][0]["text"] == first   # re-anchored to the new text


@pytest.mark.parametrize("change", [
    pytest.param(lambda t: t.replace("(x", "( x"), id="parenthesis spacing"),
    pytest.param(lambda t: t[t.index(" ") + 1:], id="boundaries moved"),
])
def test_a_passage_whose_text_differs_beyond_normalization_is_read_again(env, change):
    settings, catalog, ref, ctx, found = _two_passages(env, "87")
    first, second = _texts(catalog, ref, found)
    assert change(first) != first
    _earlier_prose(catalog, ref, [(change(first), ("patients > controls", (-22, -4, -18, 4.1))),
                                 (second, ("controls > patients", (6, -60, 40, 3.9)))])
    reader = _Counting()
    _, (read,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert reader.calls == 1
    assert read.summary["reused"] == [1]
    assert read.summary["read_again"] == {"passage text changed": 1}


@pytest.mark.parametrize("earlier", ["nu-prose's first answers", "nu-prose-v2"])
def test_another_reader_reads_every_passage_again(env, earlier):
    settings, catalog, ref, ctx, found = _two_passages(env, "88")
    if earlier == "nu-prose-v2":
        _run(_prose(settings, _Reader()), ctx, catalog, ref)
    else:
        first, second = _texts(catalog, ref, found)
        _earlier_prose(catalog, ref, [(first, ("patients > controls", (-22, -4, -18, 4.1))),
                                     (second, ("controls > patients", (6, -60, 40, 3.9)))])
    other = settings.model_copy(update={"prose_model": "another-prose-model"})
    reader = _Counting()
    _, (third,) = _run(_prose(other, reader), ctx, catalog, ref)
    assert reader.calls == 2
    assert third.summary["read_again"] == {"earlier model not interchangeable": 2}
    assert {p["producer"]["model"] for p in third.payload["passages"]} == {"another-prose-model"}


def test_of_two_earlier_copies_of_a_text_the_first_is_taken(env):
    settings, catalog, ref, ctx, found = _two_passages(env, "89")
    first, second = _texts(catalog, ref, found)
    _earlier_prose(catalog, ref, [(first, ("first copy", (-22, -4, -18, 4.1))),
                                 (first, ("second copy", (-22, -4, -18, 4.1))),
                                 (second, ("controls > patients", (6, -60, 40, 3.9)))])
    _, (read,) = _run(_prose(settings, _Reader(fail=True)), ctx, catalog, ref)
    assert read.payload["passages"][0]["analyses"][0]["name"] == "first copy"


@pytest.mark.parametrize("content,finish_reason", [
    pytest.param(None, "stop", id="no content"),
    pytest.param("  ", "stop", id="whitespace"),
    pytest.param('{"space": "MNI", "analyses": []}', "length", id="cut off at the token limit"),
])
def test_an_answer_of_nothing_fails_the_article_rather_than_reading_as_no_coordinates(
        env, content, finish_reason):
    from types import SimpleNamespace

    from ingestion_workflow.clients.prose_coordinates import ProseCoordinateClient

    settings, catalog, ref, ctx, found = _two_passages(env, "100")
    reader = ProseCoordinateClient(settings)
    choice = SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish_reason)
    reader.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
        create=lambda **kw: SimpleNamespace(choices=[choice]))))
    _, (read,) = _run(_prose(settings, reader), ctx, catalog, ref)
    assert (read.status, read.error) == (Status.FAILED, "2 of 2 passages failed")


def _read_before(env):
    """Three articles: one read OK whose title has changed since, one never read, and one
    read OK whose passages were found again unchanged, which takes its stored answers."""
    settings, catalog, path = env
    ctx = Context(settings, catalog)
    read, unread, same = (catalog.register(Identifier(pmid=p)) for p in ("110", "111", "114"))
    for ref in (read, unread, same):
        _record_upstream(catalog, ref, path, text=TWO_PASSAGES)
        _run(PassagesStage(settings), ctx, catalog, ref)
    for ref in (read, same):
        _, (first,) = _run(_prose(settings, _Reader()), ctx, catalog, ref)
        assert first.status is Status.OK
    catalog.record([Outcome(article_id=read.id, stage="metadata", source="", fingerprint="me-2",
                            payload={"title": "T2", "abstract": "A2"}, summary={})])
    found = catalog.payload(catalog.artifacts([same.id], "passages")[same.id][""])
    catalog.record([Outcome(article_id=same.id, stage="passages", source="", fingerprint="newer",
                            payload=found, summary={"passages": 2})])
    return settings, catalog, ctx, read, unread, same


def _execute(stage, ctx, catalog, refs):
    ids = [ref.id for ref in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, stage.requires))
    assert len(plan.pending) == len(refs)
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return outcomes


def test_an_unreachable_server_stops_prose_and_leaves_every_article_as_it_was(env, caplog):
    from ingestion_workflow.tests.clients.test_prose_client import _served, _server

    settings, catalog, ctx, read, unread, same = _read_before(env)
    before = catalog.artifacts([read.id, same.id], "prose")
    stored = {i: catalog.payload(before[i][""]) for i in (read.id, same.id)}
    server = _server(up=False)
    stage = _prose(settings, _served(settings, server))
    with caplog.at_level("ERROR"):
        assert _execute(stage, ctx, catalog, [read, unread]) == []
        asked = len(server.calls)
        # a batch after it, one that needs no call: nothing written
        assert _execute(stage, ctx, catalog, [same]) == []
    assert asked and len(server.calls) == asked
    after = catalog.artifacts([read.id, unread.id, same.id], "prose")
    assert unread.id not in after
    for i in (read.id, same.id):
        assert after[i][""].ok and after[i][""].fingerprint == before[i][""].fingerprint
        assert catalog.payload(after[i][""]) == stored[i]
        assert all(p["answer"] is not None for p in stored[i]["passages"])
    stops = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert len(stops) == 2 and all("prose stopped: nu-prose-v2 at" in m and "did not answer" in m
                                   for m in stops)
    assert [m.rsplit("; ", 1)[1] for m in stops] == [
        "2 article(s) left pending", "1 article(s) left pending"]


def test_a_server_up_that_fails_on_one_article_s_input_fails_that_article_only(env):
    from ingestion_workflow.tests.clients.test_prose_client import _served, _server

    settings, catalog, path = env
    ctx = Context(settings, catalog)
    good, bad = catalog.register(Identifier(pmid="112")), catalog.register(Identifier(pmid="113"))
    _record_upstream(catalog, good, path)
    _record_upstream(catalog, bad, path, text=TWO_PASSAGES)
    for ref in (good, bad):
        _run(PassagesStage(settings), ctx, catalog, ref)
    server = _server(fails=lambda content: "y = -60, z = 40" in content)
    stage = _prose(settings, _served(settings, server))
    outcomes = {o.article_id: o for o in _execute(stage, ctx, catalog, [good, bad])}
    assert outcomes[good.id].status is Status.OK
    assert (outcomes[bad.id].status, outcomes[bad.id].error) == (
        Status.FAILED, "1 of 2 passages failed")


def test_a_stopped_prose_stage_writes_no_take_back_either(env):
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stage import NO_TEXT
    from ingestion_workflow.tests.clients.test_prose_client import _served, _server

    settings, catalog, ctx, read, unread, same = _read_before(env)
    taken = catalog.register(Identifier(pmid="115"))
    take_back = Work(ref=taken, source="", fingerprint=NO_TEXT, upstream=None)
    ids = [read.id, unread.id]
    plan = ProseStage(settings).plan(ctx, [read, unread], catalog.artifacts(ids, "prose"),
                                     catalog.artifacts(ids, "passages"))
    stage = _prose(settings, _served(settings, _server(up=False)))
    # the stopping batch, then a later one that holds only the take-back
    assert list(stage.execute(ctx, [take_back, *plan.pending])) == []
    assert list(stage.execute(ctx, [take_back])) == []


def _empty_re_read(catalog, ref, *, analyses="no coordinate tables", prose_analyses=1, without=()):
    """w2ni664ashw2's shape: everything made from an earlier prose reading, then a re-read that
    found nothing. `analyses` is the tables' state: SKIPPED for no coordinate table, an
    Outcome, or None for no analyses run."""
    _uploaded_chain(catalog, ref, without=("passages", *without))
    rows = [Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="old-resolve",
                    payload={}, summary={"prose_analyses": prose_analyses}),
            Outcome(article_id=ref.id, stage="prose", source="", fingerprint="re-read",
                    payload={"passages": [{"analyses": [], "omitted": []}]},
                    summary={"passages": 1, "coordinates": 0, "analyses": 0})]
    if analyses == "no coordinate tables":
        rows.append(Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-0",
                            status=Status.SKIPPED,
                            summary={"tables": 0, "reason": "no coordinate tables"}))
    elif analyses is not None:
        rows.append(analyses)
    catalog.record(rows)


def test_an_empty_re_read_takes_back_everything_made_from_the_earlier_reading(env, monkeypatch):
    """w2ni664ashw2: the role-free model read the passage again and found no coordinate, and
    the article has no table. The resolve, roles, space and upload made from the earlier
    reading's one analysis are taken back, upload records that the study needs a retraction,
    and roles is not left waiting on a resolve made from other prose."""
    from ingestion_workflow.pipeline.upstream import RETRACTION_NEEDED

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="90"))
    _empty_re_read(catalog, ref)
    ctx = Context(settings, catalog)
    for stage in (ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error, taken.fingerprint) == (
            stage.name, Status.FAILED, NO_ANALYSES, NO_TEXT)
    # recorded once: resolve now skips the article, roles waits on nothing
    again, outcomes = _run(ResolveStage(settings), ctx, catalog, ref)
    assert (again.skipped, outcomes) == (1, [])
    again, outcomes = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    assert (again.blocked, again.reasons, outcomes) == (1, {}, [])
    synced, uploaded, _ = _sync_then_upload(settings, ctx, catalog, ref)
    assert (synced.error, uploaded.error) == (NO_ANALYSES, f"{RETRACTION_NEEDED}: {NO_ANALYSES}")


def test_an_article_never_resolved_with_nothing_to_resolve_is_skipped_not_taken_back(env):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="91"))
    catalog.record([Outcome(article_id=ref.id, stage="prose", source="", fingerprint="empty",
                            payload={"passages": []}, summary={"passages": 0, "coordinates": 0})])
    plan, outcomes = _run(ResolveStage(settings), Context(settings, catalog), catalog, ref)
    assert (plan.skipped, plan.pending, outcomes) == (1, [], [])


def test_a_re_read_that_finds_points_again_rebuilds_a_taken_back_resolve(env):
    from ingestion_workflow.pipeline.stage import NO_ANALYSES, NO_TEXT

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="92"))
    catalog.record([Outcome.failure(ref.id, "resolve", "", NO_ANALYSES, fingerprint=NO_TEXT),
                    Outcome(article_id=ref.id, stage="prose", source="", fingerprint="found",
                            payload={"space": "MNI",
                                     "passages": [_stated("insula", 38, 20, -4, "T", 4.1)]},
                            summary={"passages": 1, "coordinates": 1, "analyses": 1})])
    _, (resolved,) = _run(ResolveStage(settings), Context(settings, catalog), catalog, ref)
    assert resolved.status is Status.OK and resolved.summary["prose_points"] == 1


def _analyses(ref, status=Status.OK, error=None, **summary):
    if status is Status.FAILED:
        return Outcome.failure(ref.id, "analyses", "", error, fingerprint="an-1")
    return Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                   status=status, payload={}, summary=summary)


@pytest.mark.parametrize("prose_analyses", [1, 0], ids=["made from prose", "tables only"])
@pytest.mark.parametrize("tables", [
    pytest.param(lambda ref: _analyses(ref, Status.FAILED, "APIConnectionError: Connection error."),
                 id="failed tables run"),
    pytest.param(lambda ref: _analyses(ref, Status.FAILED, "TimeoutError: the request timed out"),
                 id="timeout"),
    pytest.param(lambda ref: _analyses(ref, Status.FAILED, "the extraction triage judged is gone"),
                 id="extraction gone"),
    pytest.param(lambda ref: _analyses(ref, tables=0, unread={"T1": "raw content missing"}),
                 id="raw content missing"),
    pytest.param(lambda ref: None, id="no analyses or triage run"),
])
def test_tables_not_known_to_be_empty_only_block(env, monkeypatch, tables, prose_analyses):
    """A failed, partial or missing tables run says nothing about the article: with empty
    prose, nothing is taken back and nothing is retracted; resolve blocks, with its reason."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="93"))
    _empty_re_read(catalog, ref, analyses=tables(ref), prose_analyses=prose_analyses)
    ctx = Context(settings, catalog)
    plan, outcomes = _run(ResolveStage(settings), ctx, catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    assert [r.split(":")[0] for r in plan.reasons] == ["tables not read"]
    assert catalog.artifacts([ref.id], "resolve")[ref.id][""].status is Status.OK
    plan, outcomes = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    assert not any(o.error for o in outcomes)


def test_tables_read_whole_and_empty_are_known_to_hold_nothing(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="94"))
    _empty_re_read(catalog, ref, analyses=_analyses(
        ref, tables=0, unread={"T2": "repeats another table's numbers"}))
    _, (taken,) = _run(ResolveStage(settings), Context(settings, catalog), catalog, ref)
    assert (taken.status, taken.error) == (Status.FAILED, NO_ANALYSES)


def test_a_resolve_made_from_the_tables_alone_is_not_taken_back(env):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="95"))
    _empty_re_read(catalog, ref, prose_analyses=0)
    plan, outcomes = _run(ResolveStage(settings), Context(settings, catalog), catalog, ref)
    assert (plan.skipped, outcomes) == (1, [])


def test_an_empty_re_read_never_uploaded_is_recorded_taken_back_with_its_reason(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="97"))
    _empty_re_read(catalog, ref, without=("upload",))
    ctx = Context(settings, catalog)
    for stage in (ResolveStage(settings), _roles(settings, monkeypatch), SpaceStage(settings)):
        _run(stage, ctx, catalog, ref)

    _, done, _ = _sync_then_upload(settings, ctx, catalog, ref)
    assert (done.status, done.error, done.fingerprint) == (Status.FAILED, NO_ANALYSES, NO_TEXT)


def test_the_run_report_counts_an_empty_re_read_as_taken_back(env, monkeypatch):
    from ingestion_workflow.pipeline.scheduler import run_stages

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="98"))
    _empty_re_read(catalog, ref)
    report = run_stages(Context(settings, catalog), [ResolveStage(settings)], [ref])
    assert (report.stages["resolve"].failed, report.stages["resolve"].taken_back) == (1, 1)
