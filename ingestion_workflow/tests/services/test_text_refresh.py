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
