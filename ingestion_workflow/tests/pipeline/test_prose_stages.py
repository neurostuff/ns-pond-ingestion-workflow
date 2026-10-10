"""`prose` reads coordinates from the text; `resolve` merges them with the tables'."""

from __future__ import annotations

import pytest

from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.passages import PassagesStage
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


def test_space_reads_resolve_only_when_prose_is_on(tmp_path):
    off = SpaceStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))
    on = SpaceStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose"))
    assert (off.requires, on.requires) == ("analyses", "resolve")
    assert SpaceStage.requires == "analyses"   # the class keeps the default


def test_a_restated_table_peak_is_recorded_but_not_uploaded_twice():
    from ingestion_workflow.services.coordinate_flags import leaves_with_its_role

    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("patients > controls", [(-22, -4, -17, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert analysis["metadata"]["restatement"] is True and len(analysis["coordinates"]) == 1
    assert not leaves_with_its_role(analysis["metadata"])
    assert summary["restated"] == 1 and summary["restatements"] == 1 and summary["prose_points"] == 0


def test_an_analysis_restating_a_table_under_another_name_is_recorded_not_uploaded():
    """"NHSDD > HSDD" in a Results sentence, its four peaks those of table 3."""
    from ingestion_workflow.services.coordinate_flags import leaves_with_its_role

    peaks = [(-6, 24, 48), (40, 18, -4), (-38, 20, -2), (4, -60, 30)]
    tables = {"t3": _table(peaks)}
    tables["t3"]["analyses"][0]["name"] = "NHSDD > HSDD"
    prose = {"passages": [_passage(("erotic > non-erotic: NHSDD > HSDD",
                                    [(x, y, z, "result") for x, y, z in peaks]))]}
    out, summary = resolve(tables, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert analysis["metadata"]["restatement"] is True
    assert [p["restates"] for p in analysis["metadata"]["restated_points"]] == [
        [{"table": "t3", "analysis": "NHSDD > HSDD"}]] * 4
    assert not leaves_with_its_role(analysis["metadata"])
    assert summary["restatements"] == 1 and summary["restated"] == 4


def test_an_analysis_with_new_points_keeps_them_and_records_the_restated_ones():
    """A correlation reported at one table peak and at a peak the table lacks."""
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("negative correlation with craving",
                                    [(-22, -4, -18, "result"), (36, 20, 2, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    (analysis,) = out["prose"]["analyses"]
    assert [(c["x"], c["y"], c["z"]) for c in analysis["coordinates"]] == [(36.0, 20.0, 2.0)]
    assert "restatement" not in analysis["metadata"]
    assert analysis["metadata"]["restated_points"] == [{
        "x": -22.0, "y": -4.0, "z": -18.0,
        "restates": [{"table": "t1", "analysis": "patients > controls"}]}]
    assert summary["restated"] == 1 and summary["restatements"] == 0 and summary["prose_points"] == 1


def test_every_role_is_kept_one_role_per_analysis():
    prose = {"passages": [_passage(
        ("amygdala seed", [(-22, -4, -18, "seed")]),
        ("insula ROI", [(36, 20, 2, "roi")]),
        ("left DLPFC TMS target", [(-40, 30, 30, "target")]),
        ("Smith et al. (2010)", [(30, 20, 10, "prior_study")]),
        ("slice shown", [(0, 0, 10, "figure")]),
        ("faces > houses", [(40, -50, -20, "result"), (12, 10, 8, "seed")]))]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["role"], len(a["coordinates"]))
           for a in out["prose"]["analyses"]]
    assert got == [("amygdala seed", "seed", 1), ("insula ROI", "roi", 1),
                   ("left DLPFC TMS target", "target", 1),
                   ("Smith et al. (2010)", "prior_study", 1),
                   ("slice shown", "figure", 1), ("faces > houses", "result", 1),
                   ("faces > houses", "seed", 1)]
    # A seed is the set's role; no point carries a seed flag of its own.
    assert not any("is_seed" in c for a in out["prose"]["analyses"] for c in a["coordinates"])
    assert summary["kept"] == {"seed": 2, "roi": 1, "target": 1, "result": 1,
                               "prior_study": 1, "figure": 1}


def test_a_display_location_reaches_the_output_but_not_pondie_or_neurostore():
    from ingestion_workflow.models import AnalysisCollection
    from ingestion_workflow.services.coordinate_flags import leaves_with_its_role

    prose = {"passages": [_passage(("slice shown", [(0, -52, 10, "figure")]),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    collection = AnalysisCollection.from_dict(out["prose"])
    assert [(a.name, a.metadata["role"]) for a in collection.analyses] == [
        ("slice shown", "figure"), ("faces > houses", "result")]
    leaving = [a.name for a in collection.analyses if leaves_with_its_role(a.metadata)]
    assert leaving == ["faces > houses"]
    # The display location does not decide the collection's space.
    assert out["prose"]["coordinate_space"] == "MNI"


def test_one_peak_reported_for_two_contrasts_stays_under_both():
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result")]),
                                   ("main effect of load", [(40, -50, -20, "result")]),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    analyses = out["prose"]["analyses"]
    assert [a["name"] for a in analyses] == ["faces > houses", "main effect of load"]
    assert [len(a["coordinates"]) for a in analyses] == [1, 1]
    assert summary["prose_points"] == 2


def test_two_unnamed_analyses_in_two_passages_stay_two():
    prose = {"passages": [
        _passage((None, [(-38, 22, -6, "result"), (42, 18, -4, "result")])),
        _passage(("", [(4, 52, 18, "result")])),
    ]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], len(a["coordinates"]))
           for a in out["prose"]["analyses"]]
    assert got == [("unnamed prose analysis", [0], 2), ("unnamed prose analysis", [1], 1)]
    assert summary["prose_analyses"] == 2


def test_two_unnamed_analyses_in_one_passage_stay_two_in_the_passages_order():
    prose = {"passages": [_passage((None, [(4, 52, 18, "result")]),
                                   (None, [(-38, 22, -6, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["ordinal"], a["coordinates"][0]["x"])
           for a in out["prose"]["analyses"]]
    assert got == [("unnamed prose analysis", 0, 4.0), ("unnamed prose analysis 2", 1, -38.0)]


def test_main_effect_in_two_experiments_with_no_shared_peak_is_two_analyses():
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


def test_an_analysis_named_again_in_another_passage_at_a_shared_peak_is_one():
    """The striatum ROI from Results, cited again in the Discussion "(peak -18, 11, -2)"."""
    prose = {"passages": [
        _passage(("striatum ROI", [(-18, 11, -2, "roi"), (18, 12, -2, "roi")])),
        _passage(("Placebo > Sham", [(30, 2, -20, "result")])),
        _passage(("Striatum  ROI", [(-18.4, 11, -2, "roi")])),
    ]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], len(a["coordinates"]))
           for a in out["prose"]["analyses"]]
    assert got == [("striatum ROI", [0, 2], 2), ("Placebo > Sham", [1], 1)]
    assert summary["prose_analyses"] == 2


def test_different_names_at_a_shared_peak_in_two_passages_stay_two():
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result")])),
                          _passage(("main effect of load", [(40, -50, -20, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    assert [a["metadata"]["passages"] for a in out["prose"]["analyses"]] == [[0], [1]]


def test_a_restated_peak_is_recorded_with_the_table_analysis_it_restates():
    tables = {"t1": _table([(30, -60, 12), (-22, -4, -18)])}
    tables["t1"]["analyses"][0]["name"] = "Patients > Controls"
    prose = {"passages": [_passage(("PATIENTS  >  controls",
                                    [(30.4, -60, 12, "result"), (-22, -4, -18, "result"),
                                     (8, 8, 8, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert [len(a["coordinates"]) for a in out["prose"]["analyses"]] == [1]
    assert summary["restated_points"] == [{
        "passages": [0], "analysis": "PATIENTS  >  controls", "role": "result", "points": 2,
        "restates": [{"table": "t1", "analysis": "Patients > Controls"}], "restatement": False}]


def test_a_named_contrast_the_passage_reports_no_peak_for_is_kept():
    prose = {"passages": [_passage(("Placebo > Sham", []),
                                   ("faces > houses", [(40, -50, -20, "result")]))]}
    out, summary = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["role"], len(a["coordinates"]))
           for a in out["prose"]["analyses"]]
    assert got == [("Placebo > Sham", "result", 0), ("faces > houses", "result", 1)]
    assert summary["prose_analyses"] == 2


def test_the_space_is_voted_by_the_uploaded_roles_only():
    """Two prior studies' Talairach peaks do not outvote the paper's MNI result."""
    prose = {"passages": [_passage(("Smith et al. (2010)", [(30, 20, 10, "prior_study"),
                                                            (-30, 20, 10, "prior_study")]), space="TAL"),
                          _passage(("faces > houses", [(40, -50, -20, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    assert out["prose"]["coordinate_space"] == "MNI"


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


def _download(ref, path):
    """A download payload as the download stage writes it."""
    from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType

    return DownloadResult(identifier=ref.identifier, source=DownloadSource.PUBGET, success=True, files=[
        DownloadedFile(file_path=path, file_type=FileType.XML, content_type="application/xml",
                       source=DownloadSource.PUBGET)]).to_dict()


def _record_upstream(catalog, ref, path, *, tables=None):
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
    """Stands in for the model: reports every coordinate the passage holds as a result."""

    def __init__(self, fail=False):
        self.fail = fail

    def read(self, passage, *, title="", abstract=""):
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
    _, (read,) = _read_prose(Context(settings, catalog), catalog, ref, _Reader())
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
    ctx = Context(settings, catalog)
    _, (found,) = _run(PassagesStage(settings), ctx, catalog, ref)
    assert found.summary == {"source": "pubget", "read": "filtered", "passages": 0, "hits": 0}
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
    _read_prose(ctx, catalog, ref, _Reader())
    _, (merged,) = _run(ResolveStage(settings), ctx, catalog, ref)
    assert merged.summary["restated"] == 1
    assert merged.summary["basis"] == "an-1"
    assert SpaceStage(settings).fingerprint_for(catalog.artifact(ref.id, "resolve", "")) == expected


def test_an_article_with_a_failed_passage_is_retried_whole(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="4"))
    _record_upstream(catalog, ref, path)
    _, (outcome,) = _read_prose(Context(settings, catalog), catalog, ref, _Reader(fail=True))
    assert outcome.status is Status.FAILED


def test_an_article_found_only_through_its_prose_is_fetched_metadata(env, monkeypatch, tmp_path):
    """No extraction, so metadata had no way to reach it; its passages do."""
    from ingestion_workflow.models.metadata import ArticleMetadata
    from ingestion_workflow.pipeline.stages.metadata import MetadataStage

    settings, catalog, path = env
    plain = tmp_path / "plain.xml"
    plain.write_text("<article><body><sec><title>Results</title><p>No coordinates.</p></sec></body></article>")
    found, empty = catalog.register(Identifier(pmid="7")), catalog.register(Identifier(pmid="8"))
    catalog.record([Outcome(article_id=r.id, stage="download", source="pubget", fingerprint="dl-1",
                            payload={"files": [{"file_path": str(f), "file_type": "xml"}]}, summary={})
                    for r, f in ((found, path), (empty, plain))])
    ctx = Context(settings, catalog)
    for ref in (found, empty):
        _run(PassagesStage(settings), ctx, catalog, ref)
    meta = MetadataStage(settings)
    asked = []
    monkeypatch.setattr(MetadataStage, "service", property(lambda self: self))
    monkeypatch.setattr(meta, "enrich_metadata", lambda contents: asked.extend(c.identifier.pmid for c in contents)
                        or {c.slug: ArticleMetadata(title="T") for c in contents}, raising=False)
    plan = meta.plan(ctx, [found, empty], catalog.artifacts([found.id, empty.id], "metadata"),
                     catalog.artifacts([found.id, empty.id], "extract"))
    assert [w.article_id for w in plan.pending] == [found.id] and plan.blocked == 1
    (outcome,) = meta.execute(ctx, plan.pending)
    assert outcome.status is Status.OK and asked == ["7"]


def test_prose_reads_again_once_the_title_and_abstract_arrive(env):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="9"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget", fingerprint="dl-1",
                            payload={"files": [{"file_path": str(path), "file_type": "xml"}]}, summary={})])
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


def test_passages_keeps_the_text_it_read_for_an_article_with_a_passage(env, tmp_path):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="10"))
    _record_upstream(catalog, ref, path)
    _, (found,) = _run(PassagesStage(settings), Context(settings, catalog), catalog, ref)
    text = open(found.payload["full_text_path"]).read()
    assert "greater activation than controls" in text and "# Results" in text


def test_an_article_extract_could_not_read_is_synced_from_its_prose(env, monkeypatch, tmp_path):
    """124 of the first sync's failures were pages ACE could not identify, whose
    prose the generic reader had read: its download, its text, its analyses."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.services.nspond_schema import read_record

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="11"))
    _record_upstream(catalog, ref, path)
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS11", "study_id": "S11"})])
    sync = SyncStage(settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    record = read_record(settings.ns_pond_root, "BS11")
    processed = record.processed["pubget"]
    assert "greater activation than controls" in processed.text
    assert [a["table_id"] for a in record.stage1["analyses"]] == ["prose"]
    assert (settings.ns_pond_root / "pmids.tsv").read_text().split("\t")[:2] == ["11", "BS11"]


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


class _NothingFound(_Reader):
    """The passage names a contrast and reports no peak for it."""

    def read(self, passage, *, title="", abstract=""):
        return {"space": "MNI", "analyses": [{"name": "controls > patients", "measure": None, "points": []}]}


def test_a_prose_only_article_with_only_empty_contrasts_is_resolved(env, monkeypatch):
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="11"))
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
    ref = catalog.register(Identifier(pmid="12"))
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
