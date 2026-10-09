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
