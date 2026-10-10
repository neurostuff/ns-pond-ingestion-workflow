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


def test_results_and_the_regions_defined_to_get_them_are_kept_one_role_per_analysis():
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
                   ("left DLPFC TMS target", "target", 1), ("faces > houses", "result", 1),
                   ("faces > houses", "seed", 1)]
    # A seed is the set's role; no point carries a seed flag of its own.
    assert not any("is_seed" in c for a in out["prose"]["analyses"] for c in a["coordinates"])
    assert summary["kept"] == {"seed": 2, "roi": 1, "target": 1, "result": 1}
    assert summary["dropped"] == {"prior_study": 1, "figure": 1}


def test_a_seed_at_a_table_peak_is_the_tables_result_reused():
    tables = {"t1": _table([(-22, -4, -18)])}
    prose = {"passages": [_passage(("PPI with amygdala seed", [(-22, -4, -18, "seed")]))]}
    out, summary = resolve(tables, prose, "slug")
    assert out == tables and summary["restated"] == 1


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
    assert found.summary == {"source": "pubget", "read": "filtered", "passages": 0, "hits": 0}
    # and prose records the empty result without a call
    prose = ProseStage(settings)
    prose.client = lambda: (_ for _ in ()).throw(AssertionError("called"))
    _, (read,) = _run(prose, ctx, catalog, ref)
    assert read.status is Status.OK and read.summary["kept"] == 0


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
    _run(SpaceStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-1",
                            summary={"base_study_id": "BS11", "study_id": "S11"})])
    sync = SyncStage(settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    record = read_record(settings.ns_pond_root, "BS11")
    passages = catalog.payload(catalog.artifact(ref.id, "passages", ""))
    assert record.processed["pubget"].text == open(passages["full_text_path"], encoding="utf-8").read()
    assert [a["table_id"] for a in record.stage1["analyses"]] == ["prose"]
    from pyarty import read_bundle
    from study_schema.layouts import PaperParse, check_paper

    parse = read_bundle(PaperParse, settings.ns_pond_root / "BS11" / "parse")
    assert check_paper(parse, settings.ns_pond_root / "BS11") == []
    assert [a.origin for a in parse.coordinate_parse.analyses] == ["text"]


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


def test_an_uploaded_article_whose_passages_are_taken_back_leaves_neurostore_and_the_corpus(
        env, monkeypatch):
    """Old prose with kept points, and the resolve, space, upload and sync made from it: once
    passages are taken back, each stage takes back its own, upload retracts the study's
    pipeline analyses, and sync takes the article out of the corpus."""
    from ingestion_workflow.pipeline.stage import NO_TEXT
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.pipeline.stages.upload import UploadStage

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="44"))
    catalog.record([
        Outcome.failure(ref.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={},
                summary={"kept": 2}),
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="old-resolve",
                payload={}, summary={"tables": 1}),
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="old-space", payload={},
                summary={"tables": 1}),
        Outcome(article_id=ref.id, stage="upload", source="", fingerprint="old-upload",
                summary={"base_study_id": "bs-1", "analyses": 2}),
        Outcome(article_id=ref.id, stage="sync", source="", fingerprint="old-sync", summary={}),
    ])
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error) == (stage.name, Status.FAILED, NO_TEXT)
    resolved = ResolveStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "resolve"),
                                           catalog.artifacts([ref.id], "prose"))
    assert (resolved.fresh, resolved.pending, resolved.blocked) == (0, [], 1)

    retracted = []

    def retract(self, targets, **_):
        for work, base_study_id in targets:
            retracted.append(base_study_id)
            yield Outcome(article_id=work.article_id, stage="upload", source="",
                          fingerprint=work.fingerprint,
                          summary={"base_study_id": base_study_id, "retracted": "emptied"})

    monkeypatch.setattr(UploadStage, "_retract", retract)
    monkeypatch.setattr(settings, "upload_source", "nuextract-test")
    upload = UploadStage(settings)
    _run(upload, ctx, catalog, ref)
    assert retracted == ["bs-1"]
    again, outcomes = _run(upload, ctx, catalog, ref)  # retracted once, not on every run
    assert (again.blocked, outcomes) == (1, [])
    sync = SyncStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                                    catalog.artifacts([ref.id], "upload"))
    assert [w.upstream.summary["retracted"] for w in sync.pending] == ["emptied"]


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


def _uploaded_chain(catalog, ref, *, upload=None):
    """passages taken back, with the prose, resolve, space and upload made from the old text."""
    from ingestion_workflow.pipeline.stage import NO_TEXT

    catalog.record([
        Outcome.failure(ref.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint="old-prose", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="old-resolve", payload={}, summary={}),
        Outcome(article_id=ref.id, stage="space", source="", fingerprint="old-space", payload={}, summary={}),
        upload or Outcome(article_id=ref.id, stage="upload", source="", fingerprint="old-upload",
                          summary={"base_study_id": "bs-1", "analyses": 2}),
    ])


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


def test_the_take_back_runs_through_a_stage_that_last_failed_for_another_reason(env):
    """prose last failed on a timeout while resolve still holds what an older prose made:
    both are taken back, so nothing downstream counts the old text fresh."""
    from ingestion_workflow.pipeline.stage import NO_TEXT

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="48"))
    _uploaded_chain(catalog, ref)
    catalog.record([Outcome.failure(ref.id, "prose", "", "timeout", fingerprint="newer-prose")])
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), SpaceStage(settings)):
        plan, (taken,) = _run(stage, ctx, catalog, ref)
        assert (stage.name, taken.status, taken.error) == (stage.name, Status.FAILED, NO_TEXT)


class _Tunnel:
    def __init__(self, *_, fails=False):
        self.fails = fails

    def __enter__(self):
        if self.fails:
            raise ConnectionError("the tunnel dropped")
        return self

    def __exit__(self, *exc):
        return False


def _upload_through(monkeypatch, settings, *, fails=False, retracted=None):
    """UploadStage with neurostore stood in for: the tunnel fails, or each retraction comes back
    as `retracted` returns."""
    from ingestion_workflow.pipeline.stages.upload import UploadStage
    from ingestion_workflow.services import db, upload

    monkeypatch.setattr(db, "SSHTunnel", lambda *a, **k: _Tunnel(fails=fails))
    monkeypatch.setattr(db, "SessionFactory", lambda *a, **k: None)
    monkeypatch.setattr(upload.UploadService, "retract",
                        lambda self, targets, **kw: [retracted(slug, bsid, kw) for slug, bsid in targets])
    monkeypatch.setattr(settings, "upload_source", "nuextract-test")
    return UploadStage(settings)


def test_a_failed_retraction_keeps_its_base_study_and_is_tried_again(env, monkeypatch):
    from ingestion_workflow.pipeline.stage import NO_TEXT
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.services.upload import RetractOutcome

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="49"))
    _uploaded_chain(catalog, ref)
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), SpaceStage(settings)):
        _run(stage, ctx, catalog, ref)
    _, (failed,) = _run(_upload_through(monkeypatch, settings, fails=True), ctx, catalog, ref)
    assert (failed.status, failed.fingerprint, failed.summary) == (Status.FAILED, NO_TEXT, {"base_study_id": "bs-1"})
    sync = SyncStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                                    catalog.artifacts([ref.id], "upload"))
    assert sync.pending == []  # the article stays in the corpus until neurostore confirms

    asked = []

    def deleted(slug, bsid, kw):
        asked.append((bsid, kw))
        return RetractOutcome(slug=slug, base_study_id=bsid, study_id="s-1", action="deleted", success=True)

    upload = _upload_through(monkeypatch, settings, retracted=deleted)
    _, (done,) = _run(upload, ctx, catalog, ref)
    assert asked == [("bs-1", {"hold_studyset_members": True})]
    assert (done.status, done.summary["base_study_id"], done.summary["retracted"]) == (Status.OK, "bs-1", "deleted")
    again, outcomes = _run(upload, ctx, catalog, ref)
    assert (again.blocked, outcomes) == (1, [])


def test_a_study_a_studyset_holds_is_held_for_review_not_retracted(env, monkeypatch):
    from ingestion_workflow.pipeline.scheduler import StageReport
    from ingestion_workflow.pipeline.stage import HELD_FOR_REVIEW
    from ingestion_workflow.pipeline.stages.sync import SyncStage
    from ingestion_workflow.services.upload import RetractOutcome

    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid="50"))
    _uploaded_chain(catalog, ref)
    ctx = Context(settings, catalog)
    for stage in (ProseStage(settings), ResolveStage(settings), SpaceStage(settings)):
        _run(stage, ctx, catalog, ref)
    upload = _upload_through(monkeypatch, settings, retracted=lambda slug, bsid, kw: RetractOutcome(
        slug=slug, base_study_id=bsid, study_id="s-1", action="held", studysets=["ss1"], success=True))
    _, (held,) = _run(upload, ctx, catalog, ref)
    assert held.status is Status.FAILED and held.error.startswith(HELD_FOR_REVIEW)
    assert held.summary["base_study_id"] == "bs-1" and held.summary["studysets"] == ["ss1"]
    sync = SyncStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                                    catalog.artifacts([ref.id], "upload"))
    assert sync.pending == []
    again = upload.plan(ctx, [ref], catalog.artifacts([ref.id], "upload"), catalog.artifacts([ref.id], "space"))
    assert len(again.pending) == 1  # looked at again, so it goes once no studyset holds it
    assert "1 held for review" in StageReport(stage="upload", failed=1, held=1).line()
