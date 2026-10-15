"""Passages reads the legends by the extraction's caption spans, so they are an input of it."""

from __future__ import annotations

from ingestion_workflow.catalog import ArticleRef, Artifact
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource, ExtractedContent, Identifier
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import ExtractStage
from ingestion_workflow.pipeline.stages.passages import PassagesStage


def extraction_with(tmp_path, captions) -> Artifact:
    """The extraction artifact the extract stage records, for one text and these caption spans."""
    text = tmp_path / "article.txt"
    text.write_text("Body.\n\nFigure legends\n\nFig. 1 x = -34, y = 16, z = -6.", encoding="utf-8")
    content = ExtractedContent(
        slug="s",
        source=DownloadSource.ELSEVIER,
        identifier=Identifier(pmid="1"),
        full_text_path=text,
        tables=[],
        has_coordinates=False,
        figure_captions=captions,
    )
    stage = ExtractStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))
    work = Work(ref=ArticleRef(id="a1", identifier=Identifier(pmid="1")), source="elsevier", fingerprint="fp")
    outcome = stage._outcome(work, "elsevier", content)
    return Artifact(article_id="a1", stage="extract", source="elsevier", fingerprint="fp", summary=outcome.summary)


def test_a_change_in_the_caption_spans_alone_changes_the_passages_fingerprint(tmp_path):
    none = extraction_with(tmp_path, [])
    one = extraction_with(tmp_path, [{"ids": ["f1"], "span": [22, 55]}])
    again = extraction_with(tmp_path, [{"ids": ["f1"], "span": [22, 55]}])
    assert none.summary["text_sha256"] == one.summary["text_sha256"]
    sha = none.summary["text_sha256"]
    assert PassagesStage.fingerprint_for(none, sha) != PassagesStage.fingerprint_for(one, sha)
    assert PassagesStage.fingerprint_for(one, sha) == PassagesStage.fingerprint_for(again, sha)


def test_an_extraction_recording_no_caption_digest_is_not_read_as_having_none(tmp_path):
    recorded = extraction_with(tmp_path, [])
    older = Artifact(article_id="a1", stage="extract", source="elsevier", fingerprint="fp",
                     summary={"text_sha256": recorded.summary["text_sha256"]})
    sha = recorded.summary["text_sha256"]
    assert PassagesStage.fingerprint_for(older, sha) != PassagesStage.fingerprint_for(recorded, sha)
