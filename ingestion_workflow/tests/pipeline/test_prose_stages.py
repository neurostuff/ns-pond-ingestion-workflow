"""`prose` reads coordinates from the text; `resolve` merges them with the tables'."""

from __future__ import annotations

import pytest

from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.prose import ProseStage
from ingestion_workflow.pipeline.stages.resolve import ResolveStage, resolve
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
            {"x": x, "y": y, "z": z, "statistic": "T", "value": 4.1, "cluster_size": None, "role": role}
            for x, y, z, role in points]}
        for name, points in analyses]}


def test_prose_and_resolve_run_after_analyses_and_before_space():
    order = list(STAGE_ORDER)
    assert order.index("analyses") < order.index("prose") < order.index("resolve") < order.index("space")


def test_the_prose_stages_are_left_out_unless_switched_on(tmp_path):
    off = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    assert not {s.name for s in build(None, off)} & set(PROSE_STAGES)
    with pytest.raises(ValueError, match="prose_model"):
        build(["prose"], off)
    on = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose")
    assert set(PROSE_STAGES) <= {s.name for s in build(None, on)}


def test_space_reads_resolve_only_when_prose_is_on(tmp_path):
    off = SpaceStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))
    on = SpaceStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose"))
    assert (off.requires, on.requires) == ("analyses", "resolve")
    assert SpaceStage.requires == "analyses"   # the class keeps the default


def test_a_restated_table_peak_is_not_uploaded_twice():
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("patients > controls", [(-22, -4, -17, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert out == tables
    assert summary["restated"] == 1 and summary["prose_analyses"] == 0


def test_a_correlation_computed_at_a_table_peak_is_its_own_analysis():
    """Of 277 labelled prose results on a table peak, 85 were a different
    analysis there; most were correlations and conjunctions."""
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("negative correlation with craving", [(-22, -4, -18, "result")]),
                                   ("patients > controls", [(-22, -4, -18, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert [a["name"] for a in out["prose"]["analyses"]] == ["negative correlation with craving"]
    assert summary["restated"] == 1 and summary["at_table_peaks"] == 1


def test_only_results_are_kept_and_the_rest_are_counted():
    prose = {"passages": [_passage(
        ("amygdala seed", [(-22, -4, -18, "seed")]),
        ("Smith et al. (2010)", [(30, 20, 10, "prior_study")]),
        ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    names = [a["name"] for a in out["prose"]["analyses"]]
    assert names == ["faces > houses"]
    assert summary["not_results"] == {"seed": 1, "prior_study": 1}


def test_one_peak_reported_for_two_contrasts_stays_under_both():
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result")]),
                                   ("main effect of load", [(40, -50, -20, "result")]),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    analyses = out["prose"]["analyses"]
    assert [a["name"] for a in analyses] == ["faces > houses", "main effect of load"]
    assert [len(a["coordinates"]) for a in analyses] == [1, 1]
    assert summary["prose_points"] == 2


def test_an_unstated_space_is_left_for_the_space_stage():
    out, _ = resolve({}, {"passages": [_passage(("a > b", [(1, 2, 3, "result")]), space=None)]}, "s")
    assert out["prose"]["coordinate_space"] == "OTHER"


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
                        catalog_root=tmp_path / "k", prose_model="nu-prose", llm_api_key="x")
    path = tmp_path / "article.xml"
    path.write_text(ARTICLE)
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, path


def _record_upstream(catalog, ref, path, *, tables=None):
    rows = [
        Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                payload={"files": [{"file_path": str(path), "file_type": "xml"}]}, summary={}),
        Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-1",
                payload={"title": "T", "abstract": "A"}, summary={}),
    ]
    if tables:
        rows.append(Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload=tables, summary={"tables": len(tables)}))
    catalog.record(rows)


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


class _Reader:
    """Stands in for the model: reports every coordinate the passage holds as a result."""

    def __init__(self, fail=False):
        self.fail = fail

    def extract(self, passage, *, title="", abstract=""):
        if self.fail:
            raise RuntimeError("server down")
        return {"space": None, "analyses": [{"name": "patients > controls", "measure": None, "points": [
            {"x": h.x, "y": h.y, "z": h.z, "statistic": "T", "value": 4.1, "cluster_size": None,
             "role": "result"} for h in passage.hits]}]}


def test_prose_reads_the_download_s_methods_and_results_only(env, monkeypatch):
    """Not the tables, not the discussion's cited peak -- and no extraction needed."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="1"))
    _record_upstream(catalog, ref, path)
    prose = ProseStage(settings)
    monkeypatch.setattr(prose, "client", lambda: _Reader())
    _, (read,) = _run(prose, Context(settings, catalog), catalog, ref)
    assert read.status is Status.OK and read.summary["read"] == "methods+results"
    points = [(q["x"], q["y"], q["z"]) for p in read.payload["passages"] for a in p["analyses"] for q in a["points"]]
    assert points == [(-22.0, -4.0, -18.0)]
    assert read.payload["passages"][0]["heading"] == "Results"
    assert read.payload["space"] == "MNI"   # read from the Methods


def test_a_download_with_no_coordinate_is_filtered_before_it_is_parsed(env, monkeypatch, tmp_path):
    settings, catalog, _ = env
    plain = tmp_path / "plain.xml"
    plain.write_text("<article><body><sec><title>Results</title><p>Accuracy was 85% (70, 85, 92).</p></sec></body></article>")
    ref = catalog.register(Identifier(pmid="5"))
    _record_upstream(catalog, ref, plain)
    from ingestion_workflow.services import prose_text

    monkeypatch.setattr(prose_text, "read_download", lambda *a: (_ for _ in ()).throw(AssertionError("parsed")))
    prose = ProseStage(settings)
    _, (read,) = _run(prose, Context(settings, catalog), catalog, ref)
    assert read.summary == {"source": "pubget", "read": "filtered", "passages": 0, "coordinates": 0, "results": 0}


def test_a_prose_only_article_reaches_space_with_its_space_read(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="2"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    prose = ProseStage(settings)
    monkeypatch.setattr(prose, "client", lambda: _Reader())
    _run(prose, ctx, catalog, ref)
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.summary["prose_points"] == 1 and merged.summary["basis"] == ""
    _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
    assert spaced.payload["prose"]["analyses"][0]["coordinates"][0]["x"] == -22.0
    assert spaced.payload["prose"]["coordinate_space"] == "MNI"


def test_switching_prose_on_leaves_a_tables_only_article_fresh(env, monkeypatch):
    """Resolve names the analyses it passed through, so space -- and upload
    after it -- keep the fingerprints they had before prose existed."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="3"))
    _record_upstream(catalog, ref, path, tables={"t1": _table([(-22, -4, -18)])})
    ctx = Context(settings, catalog)
    before = SpaceStage(Settings(data_root=settings.data_root, cache_root=settings.cache_root))
    expected = before.fingerprint_for(catalog.artifact(ref.id, "analyses", ""))
    prose = ProseStage(settings)
    monkeypatch.setattr(prose, "client", lambda: _Reader())
    _run(prose, ctx, catalog, ref)
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.summary["restated"] == 1
    assert merged.summary["basis"] == "an-1"
    assert SpaceStage(settings).fingerprint_for(catalog.artifact(ref.id, "resolve", "")) == expected


def test_an_article_with_a_failed_passage_is_retried_whole(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="4"))
    _record_upstream(catalog, ref, path)
    prose = ProseStage(settings)
    monkeypatch.setattr(prose, "client", lambda: _Reader(fail=True))
    _, (outcome,) = _run(prose, Context(settings, catalog), catalog, ref)
    assert outcome.status is Status.FAILED


def test_resolve_runs_again_when_the_tables_arrive(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="6"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    prose = ProseStage(settings)
    monkeypatch.setattr(prose, "client", lambda: _Reader())
    _run(prose, ctx, catalog, ref)
    _run(ResolveStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-2",
                            payload={"t1": _table([(-22, -4, -18)])}, summary={"tables": 1})])
    plan, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert len(plan.pending) == 1 and merged.summary["restated"] == 1
