"""The review of #221: what an extraction appends is fingerprinted by what was read; a failed
extraction stays failed; a stored extraction is appended to as the catalog recorded it; one
file's failure is noted, not the article's; supplements are read as the body is."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from ingestion_workflow.catalog import Outcome, Status
from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import ExtractStage, SupplementsStage
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.passages import find_passages
from ingestion_workflow.services import supplements as sp
from ingestion_workflow.tests.pipeline.test_supplements_stage import (  # noqa: F401
    _article, _download, _extraction, _plan, env)


def _item(tmp_path, name, body, caption, sha="a"):
    out = tmp_path / ("read-" + name)
    (out / "tables").mkdir(parents=True, exist_ok=True)
    (out / "text.txt").write_text(body)
    csv = out / "tables" / "t.csv"
    csv.write_text("Region,x,y,z\nL insula,-34,16,-6\n")
    return sp.Supplement("pmc", f"https://pmc/{name}", name, path=str(out / name), sha256=sha * 64,
                         text_path=str(out / "text.txt"), read_by="docling",
                         tables=[ExtractedTable(table_id="table-1", raw_content_path=csv,
                                                caption=caption).to_dict()])


def _supplements(catalog, ref, items, fp="supp-fp", **summary):
    catalog.record([Outcome(ref.id, "supplements", "", Status.OK, fp,
                            payload={"supplements": [i.to_dict() for i in items]},
                            summary={"listed": len(items), "read": sum(i.read for i in items),
                                     **({"read_digest": sp.read_digest(items)} if sp.read_digest(items) else {}),
                                     **summary})])


def _appended_extraction(catalog, tmp_path, ref, download, items):
    content = sp.append(_extraction(tmp_path, ref), items, tmp_path / "joined")
    digest = sp.read_digest(items)
    catalog.record([Outcome(ref.id, "extract", "pubget", Status.OK,
                            extraction_fingerprint("pubget", download, digest), payload=content.to_dict(),
                            summary={"supplements_fingerprint": digest,
                                     "base_fingerprint": extraction_fingerprint("pubget", download)})])


# -- R1: what was read is the fingerprint ------------------------------------------------


def test_a_supplement_read_since_makes_the_extraction_stale_though_the_routes_are_the_same(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    first = [_item(tmp_path, "s1.docx", "Table S1 peaks A", "Table S1")]
    _supplements(catalog, ref, first)
    _appended_extraction(catalog, tmp_path, ref, download, first)
    assert _plan(ExtractStage(settings), ctx, catalog, [ref]).fresh == 1

    # the supplements stage asked again (same routes, same fingerprint) and read a second file
    _supplements(catalog, ref, first + [_item(tmp_path, "s2.xlsx", "Table S2 peaks B", "Table S2", sha="b")])

    assert len(_plan(ExtractStage(settings), ctx, catalog, [ref]).pending) == 1


def test_a_file_whose_bytes_changed_makes_the_extraction_stale(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    first = [_item(tmp_path, "s1.docx", "Table S1 peaks A", "Table S1", sha="a")]
    _supplements(catalog, ref, first)
    _appended_extraction(catalog, tmp_path, ref, download, first)

    _supplements(catalog, ref, [_item(tmp_path, "s1.docx", "Table S1 peaks A2", "Table S1", sha="c")])

    assert len(_plan(ExtractStage(settings), ctx, catalog, [ref]).pending) == 1


def test_asking_again_and_reading_the_same_leaves_the_extraction_fresh(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    first = [_item(tmp_path, "s1.docx", "Table S1 peaks A", "Table S1")]
    _supplements(catalog, ref, first)
    _appended_extraction(catalog, tmp_path, ref, download, first)

    # a new SUPPLEMENTS_VERSION changes the stage's own fingerprint, not what was read
    _supplements(catalog, ref, first, fp="supp-fp-v2")

    assert _plan(ExtractStage(settings), ctx, catalog, [ref]).fresh == 1


# -- R2: a failed extraction stays failed ------------------------------------------------


def test_an_extraction_that_failed_is_not_made_ok_by_its_supplements(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    items = [_item(tmp_path, "s1.docx", "Table S1 peaks", "Table S1")]
    stage = ExtractStage(settings)
    stage._appending = {"a1": (sp.read_digest(items), items)}
    failed = ExtractedContent(slug="a1", source=DownloadSource.PUBGET, identifier=ref.identifier,
                              full_text_path=None, tables=[], error_message="no body in the XML")
    work = Work(ref=ref, source="pubget", upstream=download,
                fingerprint=extraction_fingerprint("pubget", download, sp.read_digest(items)))

    outcome = stage._outcome(work, "pubget", failed)

    assert (outcome.status, outcome.error) == (Status.FAILED, "no body in the XML")


def test_an_extraction_with_tables_and_no_text_keeps_its_tables_without_the_supplements(tmp_path):
    content = ExtractedContent(slug="s", source=DownloadSource.PDF, full_text_path=None,
                               tables=[ExtractedTable(table_id="t1", raw_content_path=tmp_path / "t1.csv")])

    out = sp.append(content, [_item(tmp_path, "s1.docx", "Table S1", "Table S1")], tmp_path / "joined")

    assert (out.full_text_path, out.supplements, [t.table_id for t in out.tables]) == (None, [], ["t1"])


# -- R3: a stored extraction, as the catalog recorded it --------------------------------


def _relative_extraction(catalog, settings, ref, download):
    base = Path(settings.catalog_root).resolve().parent / ".cache" / "extract" / "a1.txt"
    base.parent.mkdir(parents=True, exist_ok=True)
    base.write_text("## Methods\n\nTwenty-four patients.\n")
    content = ExtractedContent(slug="a1", source=DownloadSource.PUBGET, identifier=ref.identifier,
                               full_text_path=Path(".cache/extract/a1.txt"), tables=[])
    catalog.record([Outcome(ref.id, "extract", "pubget", Status.OK, extraction_fingerprint("pubget", download),
                            payload=content.to_dict(), summary={})])
    return base


def test_a_relative_recorded_text_is_appended_to_from_any_working_directory(env, tmp_path, monkeypatch):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    _relative_extraction(catalog, settings, ref, download)
    _supplements(catalog, ref, [_item(tmp_path, "s1.docx", "Table S1 peaks", "Table S1")])
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    stage = ExtractStage(settings)
    stage.extractor = lambda s: (_ for _ in ()).throw(AssertionError("the download was read again"))

    [outcome] = list(stage.execute(ctx, _plan(stage, ctx, catalog, [ref]).pending))

    text = Path(outcome.payload["full_text_path"]).read_text()
    assert outcome.status is Status.OK
    assert text.startswith("## Methods\n\nTwenty-four patients.") and "Table S1 peaks" in text


def test_a_recorded_text_that_is_gone_fails_the_article(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    _relative_extraction(catalog, settings, ref, download).unlink()
    _supplements(catalog, ref, [_item(tmp_path, "s1.docx", "Table S1 peaks", "Table S1")])
    stage = ExtractStage(settings)

    [outcome] = list(stage.execute(ctx, _plan(stage, ctx, catalog, [ref]).pending))

    assert outcome.status is not Status.OK and "not there" in outcome.error


def test_a_download_that_changed_is_read_again_not_appended_to(env, tmp_path):
    """The re-append is for supplements alone: the extraction must be its download's."""
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    old = Outcome(ref.id, "extract", "pubget", Status.OK, "an older download's extraction",
                  payload=_extraction(tmp_path, ref).to_dict(), summary={})
    catalog.record([old])
    _supplements(catalog, ref, [_item(tmp_path, "s1.docx", "Table S1 peaks", "Table S1")])
    stage = ExtractStage(settings)
    read = []

    class Extractor:
        def extract(self, downloads):
            read.extend(downloads)
            return [_extraction(tmp_path, ref)]

    stage.extractor = lambda source: Extractor()
    list(stage.execute(ctx, _plan(stage, ctx, catalog, [ref]).pending))

    assert len(read) == 1


# -- one file's failure is the file's -----------------------------------------------------


class _Session:
    """Answers each URL from a table; the rest 404."""

    def __init__(self, answers):
        self.answers = answers

    def get(self, url, **kwargs):
        status, body = self.answers.get(url, (404, b""))
        return _Response(url, status, body)


class _Response:
    def __init__(self, url, status, body):
        self.url, self.status_code, self._body = url, status, body
        self.headers = {"Content-Type": "application/octet-stream"}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, size):
        yield self._body


def test_a_busy_host_leaves_the_article_standing_with_its_other_files_and_due_a_retry(env, tmp_path, monkeypatch):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    _download(catalog, tmp_path, ref, "pubget")
    stage = SupplementsStage(settings)
    good = sp.Supplement("pmc", "https://pmc/good.txt", "good.txt")
    busy = sp.Supplement("pmc", "https://pmc/busy.txt", "busy.txt")
    monkeypatch.setattr(stage, "_list", lambda *args: [good, busy])
    monkeypatch.setattr("requests.Session", lambda: _Session({
        "https://pmc/good.txt": (200, b"Supplementary results: x = -34, y = 16, z = -6."),
        "https://pmc/busy.txt": (503, b"slow down")}))

    [outcome] = list(stage.execute(ctx, _plan(stage, ctx, catalog, [ref]).pending))
    catalog.record([outcome])

    assert outcome.status is Status.OK
    assert outcome.summary["read"] == 1 and outcome.summary["read_digest"]
    assert outcome.summary["transient"] == ["busy.txt"]
    artifact = catalog.artifact(ref.id, "supplements", "")
    assert ctx.is_fresh(artifact, artifact.fingerprint)  # until the backoff has passed
    later = Context(settings, catalog, retry_after=timedelta(0))
    assert later.due_again(artifact) and not later.is_fresh(artifact, artifact.fingerprint)


def test_pdfs_no_engine_could_read_are_noted_for_the_retry_not_failed(env, tmp_path, monkeypatch):
    settings, _, _ = env

    class Broken:
        def __init__(self, settings):
            pass

        def extract(self, downloads):
            from ingestion_workflow.clients.model_server import ModelUnavailable

            raise ModelUnavailable("MinerU: no GPU has room")

    monkeypatch.setattr("ingestion_workflow.extractors.pdf_extractor.PdfExtractor", Broken)
    path = tmp_path / "s1.pdf"
    path.write_bytes(b"%PDF-1.4")
    item = sp.Supplement("pmc", "u", "s1.pdf", path=str(path))

    SupplementsStage(settings)._read_pdfs(tmp_path / "root", [("a1", 1, item)])

    assert sp.is_transient(item) and "no GPU has room" in item.reason


# -- passages read a supplement as the body ----------------------------------------------


def test_a_supplement_s_introduction_and_references_are_not_read_for_prose(tmp_path):
    body = "## Methods\n\nWe scanned patients.\n\n## Figure legends\n\nFig. 1. Maps."
    head = "\n\n## Supplementary material\n\n### Supplementary file 1 (s1.docx)\n\n"
    supplement = ("Introduction\n\nEarlier work found the insula (x = -30, y = 20, z = 4; Smith 2001).\n\n"
                  "Results\n\nActivation peaked in the left insula (x = -34, y = 16, z = -6; t = 5.2).\n\n"
                  "References\n\nSmith (2001) reported x = -30, y = 20, z = 4.\n")
    text = body + head + supplement
    path = tmp_path / "joined.txt"
    path.write_text(text)
    start = len(body + head)

    _, found, _, _ = find_passages({"text_path": str(path), "figure_captions": [],
                                    "supplements": [[start, len(text)]]})

    hits = {(h.x, h.y, h.z) for p in found for h in p.hits}
    assert (-34.0, 16.0, -6.0) in hits and (-30.0, 20.0, 4.0) not in hits



def test_a_package_with_more_than_a_thousand_files_is_listed_whole():
    from ingestion_workflow.pipeline.stages.supplements import _bucket_keys

    pages = {None: ("<Key>PMC1.1/a.pdf</Key><IsTruncated>true</IsTruncated>"
                    "<NextContinuationToken>t2</NextContinuationToken>"),
             "t2": "<Key>PMC1.1/b.docx</Key><IsTruncated>false</IsTruncated>"}

    class Paged:
        def get(self, url, params=None, **kwargs):
            answer = _Response(url, 200, b"")
            answer.text = pages[params.get("continuation-token")]
            answer.raise_for_status = lambda: None
            return answer

    assert _bucket_keys(Paged(), "PMC1.") == ["PMC1.1/a.pdf", "PMC1.1/b.docx"]


def test_a_file_fetched_twice_under_two_names_is_read_once():
    from ingestion_workflow.pipeline.stages.supplements import _dedupe

    pmc = sp.Supplement("pmc", "u1", "1-s2.0-S1-mmc1.docx", sha256="a" * 64, path="p1")
    elsevier = sp.Supplement("elsevier", "u2", "mmc1.docx", sha256="a" * 64, path="p2")
    other = sp.Supplement("elsevier", "u3", "mmc2.docx", sha256="b" * 64, path="p3")

    _dedupe([pmc, elsevier, other])

    assert (pmc.reason, elsevier.reason, other.reason) == (None, "same file as 1-s2.0-S1-mmc1.docx", None)


def crash_on_doc(task):
    """A reader that kills its process on one file, as a segfaulting converter does."""
    import os

    from ingestion_workflow.pipeline.stages.supplements import _read_one

    payload, _ = task
    if payload["name"] == "crashes.xlsx":
        os._exit(11)
    return _read_one(task)


def test_a_reader_that_dies_fails_its_file_alone(env, tmp_path):
    from openpyxl import Workbook

    settings, _, _ = env
    settings = settings.model_copy(update={"max_workers": 2})
    jobs = []
    for article_id, name in (("a1", "good.xlsx"), ("a1", "crashes.xlsx"), ("a2", "also-good.xlsx")):
        book = Workbook()
        book.active.append(["Region", "x", "y", "z"])
        book.active.append(["L insula", -34, 16, -6])
        path = tmp_path / name
        book.save(path)
        jobs.append((article_id, len(jobs) + 1, sp.Supplement("pmc", "u", name, path=str(path))))

    SupplementsStage(settings)._read_rest(tmp_path / "root", jobs, worker=crash_on_doc)

    assert [(s.name, s.read) for _, _, s in jobs] == [
        ("good.xlsx", True), ("crashes.xlsx", False), ("also-good.xlsx", True)]
    assert jobs[1][2].reason.startswith("unreadable: Stopped")


def test_a_file_that_cannot_be_written_is_noted_for_a_retry_not_the_batch_s_failure(env, tmp_path, monkeypatch):
    settings, catalog, ctx = env
    refs = [_article(catalog, f"a{n}", pmcid=f"PMC{n}") for n in (1, 2)]
    for ref in refs:
        _download(catalog, tmp_path, ref, "pubget")
    stage = SupplementsStage(settings)
    def listed(*args):  # the stage's `_list`, whichever arguments it takes
        work = next(a for a in args if isinstance(a, Work))
        return [sp.Supplement("pmc", f"https://pmc/{work.article_id}.txt", f"{work.article_id}.txt")]

    monkeypatch.setattr(stage, "_list", listed)
    monkeypatch.setattr("requests.Session", lambda: _Session({
        "https://pmc/a1.txt": (200, b"Supplementary results."), "https://pmc/a2.txt": (200, b"More results.")}))
    real = sp.fetch

    def disk_full(item, dest, *args, **kwargs):
        if item.name == "a1.txt":
            raise OSError(28, "No space left on device")
        return real(item, dest, *args, **kwargs)

    monkeypatch.setattr(sp, "fetch", disk_full)

    outcomes = {o.article_id: o for o in stage.execute(ctx, _plan(stage, ctx, catalog, refs).pending)}

    assert [o.status for o in outcomes.values()] == [Status.OK, Status.OK]
    assert outcomes["a1"].summary["transient"] == ["a1.txt"] and outcomes["a2"].summary["read"] == 1
