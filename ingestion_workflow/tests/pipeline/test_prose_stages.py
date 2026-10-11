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


def test_roles_reads_resolve_only_when_prose_is_on_and_space_always_reads_roles(tmp_path):
    off = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    on = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model="nu-prose")
    assert (RolesStage(off).requires, RolesStage(on).requires) == ("analyses", "resolve")
    assert SpaceStage(off).requires == SpaceStage(on).requires == "roles"


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
    for analysis in out["prose"]["analyses"]:
        assert analysis["metadata"] == {"source": "prose", "passages": [0]}
    # No point carries a role or seed flag of its own.
    assert not any({"role", "is_seed"} & set(c) for a in out["prose"]["analyses"] for c in a["coordinates"])
    assert summary["prose_points"] == 5 and "dropped" not in summary


def test_a_ppi_seed_at_a_table_peak_is_its_own_set():
    """The analysis is computed at the peak; the roles stage reads it as the seed it is."""
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("PPI with amygdala seed", [(-22, -4, -18, "seed")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert [a["name"] for a in out["prose"]["analyses"]] == ["PPI with amygdala seed"]
    assert summary["at_table_peaks"] == 1


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


def test_unnamed_analyses_are_each_their_own_set_in_the_model_s_order():
    """An unnamed peak quoted in the Introduction never joins an unnamed result."""
    prose = {"passages": [_passage((None, [(10, 10, 10, "prior_study")])),
                          _passage((None, [(40, -50, -20, "result")]), (None, [(1, 2, 3, "result")]))]}
    out, _ = resolve({}, prose, "slug")
    got = [(a["name"], a["metadata"]["passages"], a["coordinates"][0]["x"])
           for a in out["prose"]["analyses"]]
    assert got == [("unnamed prose analysis", [0], 10.0), ("unnamed prose analysis", [1], 40.0),
                   ("unnamed prose analysis", [1], 1.0)]


def test_a_point_dropped_from_a_set_is_recorded_with_its_reason():
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("faces > houses", [(40, -50, -20, "result"), (40.2, -50, -20, "result"),
                                                       (-22, -4, -18, "result")]))]}
    out, summary = resolve(tables, prose, "slug")
    [analysis] = out["prose"]["analyses"]
    assert len(analysis["coordinates"]) == 1
    assert analysis["metadata"]["dropped"] == [
        {"x": 40.2, "y": -50.0, "z": -20.0, "reason": "repeats a point of this analysis"},
        {"x": -22.0, "y": -4.0, "z": -18.0, "reason": "restates a table peak"},
    ]
    assert (summary["repeated"], summary["restated"]) == (1, 1)


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


class _Results:
    """A role classifier that calls every set a result."""

    source = "results@1"

    def predict(self, texts):
        from ingestion_workflow.services.set_roles import COORDINATE_ROLES, Prediction

        return [Prediction(0.99, {r: float(r == "result") for r in COORDINATE_ROLES}, {}, 0.0)
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
        def extract(self, passage, *, title="", abstract=""):
            seen.append(title)
            return super().extract(passage, title=title, abstract=abstract)

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


def _synced_names(env, tmp_path, monkeypatch, summary):
    """Sync one article whose analyses artifact carries `summary`; the analysis names it wrote."""
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, path = env
    settings = settings.model_copy(update={"ns_pond_root": tmp_path / "pond"})
    ref = catalog.register(Identifier(pmid="12"))
    pts = [{"x": 1.0, "y": 2.0, "z": 3.0, "statistic_value": v, "statistic_type": "T"}
           for v in (4.0,)]
    neg = [{**pts[0], "statistic_value": -4.0}]
    tables = {"t1": {"slug": "s::t1", "analyses": [
        {"name": "Load", "table_id": "t1", "coordinates": pts},
        {"name": "Load (negative)", "table_id": "t1", "coordinates": neg},
    ]}}
    _record_upstream(catalog, ref, path)
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload=tables, summary=summary)])
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(_roles(settings, monkeypatch), ctx, catalog, ref)
    _run(SpaceStage(settings), ctx, catalog, ref)
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
    _, (synced,) = _run(sync, ctx, catalog, ref)
    assert synced.status is Status.OK, synced.error
    return [(x.name, x.metadata.get("split")) for x in seen["t1"].analyses]


def test_a_printed_negative_name_is_not_a_split_in_a_native_payload(env, tmp_path, monkeypatch):
    names = _synced_names(env, tmp_path, monkeypatch, {"tables": 1, "split_declared": True})
    assert names == [("Load", None), ("Load (negative)", None)]


def test_a_legacy_payload_pair_is_still_paired(env, tmp_path, monkeypatch):
    names = _synced_names(env, tmp_path, monkeypatch, {"tables": 1})
    assert [n for n, _ in names] == ["Load", "Load (negative)"]
    assert [s["half"] for _, s in names] == ["original", "inverse"]
