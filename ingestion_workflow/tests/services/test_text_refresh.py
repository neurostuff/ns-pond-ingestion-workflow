from pathlib import Path

from lxml import etree

from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.services import text_refresh as R
from ingestion_workflow.tests.services.test_citations import JATS


def _article(tmp_path: Path) -> Path:
    xml = tmp_path / "pmcid_1" / "article.xml"
    xml.parent.mkdir()
    xml.write_text(JATS)
    return xml


def test_a_changed_text_is_rewritten_and_an_unchanged_one_left_alone(tmp_path):
    xml = _article(tmp_path)
    text = tmp_path / "article.txt"
    text.write_text("an older rendering", encoding="utf-8")
    job = R.Job("a1", "pubget", str(text), str(xml))

    first = R.rebuild(job, write=True)
    second = R.rebuild(job, write=True)

    assert first.status == "rewritten" and second.status == "unchanged"
    assert text.read_text(encoding="utf-8") == article_text(etree.parse(str(xml)), xml.parent)
    assert first.old_sha256 == R.sha256("an older rendering")


def test_a_dry_run_writes_nothing(tmp_path):
    xml = _article(tmp_path)
    text = tmp_path / "article.txt"
    text.write_text("an older rendering", encoding="utf-8")

    result = R.rebuild(R.Job("a1", "pubget", str(text), str(xml)), write=False)

    assert result.status == "would_rewrite"
    assert text.read_text(encoding="utf-8") == "an older rendering"


def test_an_unreadable_download_keeps_its_text(tmp_path):
    xml = tmp_path / "pmcid_2" / "article.xml"
    xml.parent.mkdir()
    xml.write_text("<article><body>")  # truncated
    text = tmp_path / "article.txt"
    text.write_text("kept", encoding="utf-8")

    result = R.rebuild(R.Job("a2", "pubget", str(text), str(xml)), write=True)

    assert result.status.startswith("failed") and text.read_text(encoding="utf-8") == "kept"


def test_only_a_byte_identical_corpus_copy_is_replaced(tmp_path):
    rebuilt = tmp_path / "article.txt"
    rebuilt.write_text("new text", encoding="utf-8")
    corpus = tmp_path / "corpus"
    copy = corpus / "rec1" / "processed" / "pubget" / "text.txt"
    other = corpus / "rec2" / "processed" / "pubget" / "text.txt"
    for path, content in ((copy, "old text"), (other, "old text, edited")):
        path.parent.mkdir(parents=True)
        path.write_text(content, encoding="utf-8")

    done = list(R.refresh_corpus(corpus, {R.sha256("old text"): str(rebuilt)}, ["pubget"], write=True))

    assert [p for p, _ in done] == [copy]
    assert copy.read_text(encoding="utf-8") == "new text"
    assert other.read_text(encoding="utf-8") == "old text, edited"


def test_a_carriage_return_the_builder_keeps_is_compared_and_copied_as_is(tmp_path):
    """read_text would turn it into a newline: the text would never compare equal to its rebuild."""
    rebuilt = tmp_path / "article.txt"
    rebuilt.write_bytes("Input:\r x\n".encode("utf-8"))
    corpus = tmp_path / "corpus"
    copy = corpus / "rec1" / "processed" / "pubget" / "text.txt"
    copy.parent.mkdir(parents=True)
    copy.write_bytes(b"old")

    list(R.refresh_corpus(corpus, {R.sha256("old"): str(rebuilt)}, ["pubget"], write=True))

    assert copy.read_bytes() == "Input:\r x\n".encode("utf-8")


def test_a_rewrite_carries_the_passages_onto_the_new_text(tmp_path):
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier
    from ingestion_workflow.pipeline.stages.passages import PassagesStage
    from ingestion_workflow.services.offsets import diff

    old = "## Results\nThe peak (x = -22, y = -4, z = -18) was here.\n"
    new = "## Results\nA superscript² first.\nThe peak (x = -22, y = -4, z = -18) was here.\n"
    text = tmp_path / "article.txt"
    text.write_text(new, encoding="utf-8")
    a = old.index("The peak")
    hit = old.index("x = -22")
    passages = {"text_from": "extract", "full_text_path": str(text), "text_sha256": R.sha256(old),
                "passages": [{"span": [a, len(old) - 1], "before": None, "after": None, "heading": [3, 10],
                              "hits": [{"pattern": "labelled", "x": -22, "y": -4, "z": -18,
                                        "span": [hit, hit + len("x = -22, y = -4, z = -18")]}]}]}
    citations = {"text_sha256": R.sha256(old),
                 "citations": [{"text_span": {"start_char": a + 4, "end_char": a + 8, "text": "peak"}}]}
    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Identifier(pmid="1")).id
        catalog.record([
            Outcome(article_id=ref_id, stage="extract", source="pubget", fingerprint="ex-1",
                    payload={"full_text_path": str(text)}, summary={"has_text": True, "text_sha256": R.sha256(old)}),
            Outcome(article_id=ref_id, stage="passages", source="", fingerprint="pa-1", payload=passages,
                    summary={"passages": 1}),
            Outcome(article_id=ref_id, stage="sync", source="", fingerprint="sy-1", payload={}, summary={}),
            Outcome(article_id=ref_id, stage="references", source="pubget", fingerprint="re-1",
                    payload=citations, summary={"citations": 1}),
        ])
        result = R.Result(ref_id, "pubget", "rewritten", str(text), R.sha256(old), R.sha256(new),
                          [list(e) for e in diff(old, new).edits])
        rows, what = R.carried(catalog, result)
        catalog.record(rows)
        got = {a.stage: a for a in catalog.artifacts_for_article(ref_id)}
        payload = catalog.payload(got["passages"])

    assert what == "remapped"
    (p,) = payload["passages"]
    assert new[p["span"][0]:p["span"][1]] == old[a:len(old) - 1]
    assert new[slice(*p["hits"][0]["span"])] == "x = -22, y = -4, z = -18"
    assert new[slice(*p["heading"])] == "Results"
    assert payload["text_sha256"] == R.sha256(new) == got["extract"].summary["text_sha256"]
    assert got["passages"].fingerprint == PassagesStage.fingerprint_for(None, got["extract"], R.sha256(new))
    assert got["sync"].fingerprint == R.STALE
    # The citations' offsets index the old text: the references stage reads the new one again.
    assert got["references"].fingerprint == R.STALE and got["references"].status.value == "ok"

    # A span the map cannot carry leaves the passages on the old text, for the stage to read again.
    s = p["span"][0]
    lost = R.Result(ref_id, "pubget", "rewritten", str(text), R.sha256(new), R.sha256("x"),
                    [[s + 1, s + 20, s + 1, s + 3]])
    with Catalog.open(tmp_path / "k") as catalog:
        assert R.carried(catalog, lost)[1] == "stale"


def test_an_unchanged_text_has_its_hash_recorded_once(tmp_path):
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier

    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Identifier(pmid="2")).id
        catalog.record([Outcome(article_id=ref_id, stage="extract", source="pubget", fingerprint="ex-1",
                                payload={"full_text_path": "t"}, summary={"has_text": True})])
        result = R.Result(ref_id, "pubget", "unchanged", "t", R.sha256("same"), R.sha256("same"))
        rows, what = R.carried(catalog, result)
        catalog.record(rows)
        (extraction,) = catalog.artifacts([ref_id], "extract")[ref_id].values()
        assert (what, extraction.summary["text_sha256"], extraction.fingerprint) == (
            "none", R.sha256("same"), "ex-1")
        assert R.carried(catalog, result)[0] == []


def _rewritten_with_passages(tmp_path, old, new, n=1):
    """A catalog holding `n` articles whose text went from `old` to `new`, with passages on `old`."""
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier
    from ingestion_workflow.services.offsets import diff
    from ingestion_workflow.services.prose_passages import passages

    found = passages(old)
    payload = lambda path: {"text_from": "extract", "full_text_path": path, "text_sha256": R.sha256(old),
                            "passages": [{"span": list(p.span), "before": None, "after": None, "heading": None,
                                          "hits": [{"x": h.x, "y": h.y, "z": h.z, "span": list(h.span)}
                                                   for h in p.hits]} for p in found]}
    results = []
    with Catalog.open(tmp_path / "k") as catalog:
        for i in range(n):
            ref_id = catalog.register(Identifier(pmid=str(100 + i))).id
            path = str(tmp_path / f"{i}.txt")
            catalog.record([
                Outcome(article_id=ref_id, stage="extract", source="pubget", fingerprint="ex-1",
                        payload={"full_text_path": path}, summary={"text_sha256": R.sha256(old)}),
                Outcome(article_id=ref_id, stage="passages", source="", fingerprint="pa-1",
                        payload=payload(path), summary={}),
            ])
            results.append(R.Result(ref_id, "pubget", "rewritten", path, R.sha256(old), R.sha256(new),
                                    [list(e) for e in diff(old, new).edits]))
    return results


def test_a_hit_whose_characters_an_edit_changed_is_read_again_not_moved(tmp_path):
    from ingestion_workflow.catalog import Catalog

    old = "## Results\nThe peak (x = -22, y = -4, z = -18) was in the amygdala.\n"
    # a superscript kept inside the triplet: the old x, y, z are no longer what it prints
    new = old.replace("y = -4", "y = -4\u00b9")
    (result,) = _rewritten_with_passages(tmp_path, old, new)
    with Catalog.open(tmp_path / "k") as catalog:
        rows, what = R.carried(catalog, result)
    assert what == "stale" and {r.stage for r in rows} == {"extract"}


def test_carry_all_reads_and_records_in_batches(tmp_path):
    from ingestion_workflow.catalog import Catalog

    old = "## Results\nThe peak (x = -22, y = -4, z = -18) was in the amygdala.\n"
    new = "## Results\nA note\u00b2 first.\n" + old[len("## Results\n"):]
    results = _rewritten_with_passages(tmp_path, old, new, n=5)
    with Catalog.open(tmp_path / "k") as catalog:
        reads = []
        artifacts = catalog.artifacts
        catalog.artifacts = lambda ids, stage: reads.append(len(ids)) or artifacts(ids, stage)
        counts = R.carry_all(catalog, results, write=True, batch=2)
        catalog.artifacts = artifacts
        moved = catalog.artifacts([r.article_id for r in results], "passages")
        payloads = [catalog.payload(moved[r.article_id][""]) for r in results]
    assert counts == {"remapped": 5}
    assert reads == [2] * 4 + [2] * 4 + [1] * 4  # one query per stage per batch
    for payload in payloads:
        assert new[slice(*payload["passages"][0]["hits"][0]["span"])] == "x = -22, y = -4, z = -18"


CAPTIONED = """<article><front><article-meta><title-group><article-title>T</article-title></title-group>
</article-meta></front><body>
<sec><title>Results</title><p>Patients showed greater activation in the left amygdala
(x = -22, y = -4, z = -18; t = 4.1) than controls (Smith, 2001).</p></sec></body>
<floats-group><fig id="F1"><label>Figure 1</label><caption><p>Insula activation
(x = -34, y = 16, z = -6; p &lt; 0.05).</p></caption></fig></floats-group></article>"""


def test_captions_added_to_a_text_carry_its_passages_and_citations_and_read_it_again(tmp_path):
    """The rebuilt text gains its figure legends: the passages and citations move onto it,
    the extraction records the captions' spans, and the passages are marked to be read again,
    since only a read finds the caption's coordinate."""
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.extractors.figure_captions import HEADING
    from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions
    from ingestion_workflow.models.ids import Identifier
    from ingestion_workflow.services.prose_passages import passages

    xml = tmp_path / "pmcid_9" / "article.xml"
    xml.parent.mkdir()
    xml.write_text(CAPTIONED)
    new, captions = article_text_and_captions(etree.parse(str(xml)), xml.parent)
    old = new[: new.index(HEADING)].rstrip().removesuffix("##").rstrip()  # what pubget wrote before
    assert "Insula" not in old and new.startswith(old)
    text = tmp_path / "article.txt"
    text.write_text(old, encoding="utf-8")

    found = passages(old)
    cite = old.index("Smith, 2001")
    sentence = (old.index("Patients"), old.index("2001).") + 6)
    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Identifier(pmid="9")).id
        catalog.record([
            Outcome(article_id=ref_id, stage="extract", source="pubget", fingerprint="ex-1",
                    payload={"full_text_path": str(text)}, summary={"text_sha256": R.sha256(old)}),
            Outcome(article_id=ref_id, stage="passages", source="", fingerprint="pa-1", summary={},
                    payload={"text_from": "extract", "full_text_path": str(text), "text_sha256": R.sha256(old),
                             "passages": [{"span": list(p.span), "before": None, "after": None, "heading": None,
                                           "hits": [{"x": h.x, "y": h.y, "z": h.z, "span": list(h.span)}
                                                    for h in p.hits]} for p in found]}),
            Outcome(article_id=ref_id, stage="references", source="pubget", fingerprint="re-1", summary={},
                    payload={"citations": [{"text_span": {"start_char": cite, "end_char": cite + 11,
                                                          "text": "Smith, 2001"},
                                            "sentence": {"start_char": sentence[0], "end_char": sentence[1]},
                                            "references": ["r1"]}]}),
        ])
        result = R.rebuild(R.Job(ref_id, "pubget", str(text), str(xml)), write=True)
        assert result.status == "rewritten" and result.captions_hold_coordinates
        assert result.figure_captions == captions
        assert R.carry_all(catalog, [result], write=True) == {"captions": 1}

        rebuilt = text.read_text(encoding="utf-8")
        assert rebuilt == new
        extraction = catalog.artifacts([ref_id], "extract")[ref_id]["pubget"]
        assert catalog.payload(extraction)["figure_captions"] == captions
        assert extraction.summary["text_sha256"] == R.sha256(new)
        stored = catalog.artifacts([ref_id], "passages")[ref_id][""]
        assert stored.fingerprint == R.STALE  # read again: the caption's coordinate is new
        moved = catalog.payload(stored)
        assert moved["text_sha256"] == R.sha256(new)
        for before, after in zip(found, moved["passages"]):
            assert rebuilt[slice(*after["span"])] == old[slice(*before.span)]
            assert [rebuilt[slice(*h["span"])] for h in after["hits"]] == [old[slice(*h.span)] for h in before.hits]
        references = catalog.artifacts([ref_id], "references")[ref_id]["pubget"]
        assert references.fingerprint == R.STALE
        (citation,) = catalog.payload(references)["citations"]
        assert rebuilt[citation["text_span"]["start_char"]:citation["text_span"]["end_char"]] == "Smith, 2001"
        assert rebuilt[citation["sentence"]["start_char"]:citation["sentence"]["end_char"]] == old[slice(*sentence)]
        # the passages stage, reading the text again, finds the caption where it was written
        from ingestion_workflow.pipeline.stages.passages import find_passages

        _, again, _, _ = find_passages({"text_path": str(text), "figure_captions": captions})
        assert [(p.hits[0].x, p.from_legend) for p in again] == [(-22.0, False), (-34.0, True)]


def test_jobs_find_each_source_s_article_file():
    files = [{"file_path": "/d/meta/metadata.json", "file_type": "json"},
             {"file_path": "/d/content.xml", "file_type": "xml"}, {"file_path": "/d/page.html", "file_type": "html"},
             {"file_path": "/d/pmcid_1/article.xml", "file_type": "xml"}]
    assert R._article_file("elsevier", files) == "/d/content.xml"
    assert R._article_file("ace", files) == "/d/page.html"
    assert R._article_file("pmc", files) == "/d/pmcid_1/article.xml"
    assert R._article_file("pdf", files) is None


INLINE_FIG = """<article><front><article-meta><title-group><article-title>T</article-title></title-group>
</article-meta></front><body><sec><title>Results</title><p>Patients showed more (Smith, 2001).</p>
<fig id="F1"><label>Figure 1</label><caption><p>Insula map.</p></caption></fig>
<p>Controls did not (Jones, 2003).</p><p>Done.</p></sec></body></article>"""


def test_a_body_figure_taken_out_moves_the_citations_after_it(tmp_path):
    """The old text printed the caption in the body; the new one prints it in the legends,
    so a citation after it moves back, and a sentence that held it is cleared."""
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions
    from ingestion_workflow.models.ids import Identifier

    xml = tmp_path / "pmcid_8" / "article.xml"
    xml.parent.mkdir()
    xml.write_text(INLINE_FIG)
    new, _ = article_text_and_captions(etree.parse(str(xml)), xml.parent)
    from ingestion_workflow.extractors.figure_captions import HEADING

    old = new[: new.index(HEADING)].rstrip().removesuffix("##").rstrip().replace("Controls", "Figure 1 Insula map.\n\nControls")
    text = tmp_path / "article.txt"
    text.write_text(old, encoding="utf-8")

    def citation(marker, sentence_from):
        start = old.index(marker)
        return {"text_span": {"start_char": start, "end_char": start + len(marker), "text": marker},
                "sentence": {"start_char": old.index(sentence_from), "end_char": start + len(marker) + 2}}

    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Identifier(pmid="8")).id
        catalog.record([
            Outcome(article_id=ref_id, stage="extract", source="pubget", fingerprint="ex-1",
                    payload={"full_text_path": str(text)}, summary={"text_sha256": R.sha256(old)}),
            Outcome(article_id=ref_id, stage="references", source="pubget", fingerprint="re-1", summary={},
                    payload={"citations": [citation("Jones, 2003", "Controls"),
                                           citation("Jones, 2003", "Figure 1"),  # its sentence held the caption
                                           citation("Insula", "Figure 1")]}),  # inside the caption
        ])
        result = R.rebuild(R.Job(ref_id, "pubget", str(text), str(xml)), write=True)
        R.carry_all(catalog, [result], write=True)
        rebuilt = text.read_text(encoding="utf-8")
        assert rebuilt == new and old.index("Jones") != new.index("Jones")
        moved = catalog.payload(catalog.artifacts([ref_id], "references")[ref_id]["pubget"])["citations"]
        assert [rebuilt[c["text_span"]["start_char"]:c["text_span"]["end_char"]] for c in moved] == ["Jones, 2003"] * 2
        assert rebuilt[moved[0]["sentence"]["start_char"]:moved[0]["sentence"]["end_char"]] == "Controls did not (Jones, 2003)."
        assert moved[1]["sentence"] is None


def test_an_ace_rebuild_gives_ace_the_extraction_s_pmid(tmp_path, monkeypatch):
    """Without it ACE returns no article for a page that does not print its PMID
    (14 of 61 ScienceDirect, OUP and MDPI pages on beast)."""
    from ingestion_workflow.extractors import ace_extractor

    seen = []
    monkeypatch.setattr(ace_extractor, "article_text_and_captions",
                        lambda html, pmid, table_dir: seen.append(pmid) or (None, "text", []))
    page = tmp_path / "1.html"
    page.write_text("<html></html>")
    R.build("ace", page, tmp_path / "article.txt", "14597300")
    assert seen == ["14597300"]
