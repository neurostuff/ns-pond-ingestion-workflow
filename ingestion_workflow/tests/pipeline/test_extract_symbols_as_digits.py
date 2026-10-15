"""A PDF whose text layer maps math symbols to digits holds nothing.

Article 2fbrux6ypmwb (PMID 27004598, HBM, Wiley PDF): its ACE text reads
`x=-22, y=56, z=42, Z=3.48`, its PDF text `x5222, y556, z542, Z53.48`, and the
figure legend `222, 56, 42` for `-22, 56, 42`. Ranked above ACE, the PDF text
replaced the good one.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pdf_extractor import symbols_as_digits
from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable, Identifier
from ingestion_workflow.models.analysis import Coordinate
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stage import NO_TEXT
from ingestion_workflow.pipeline.stages import ExtractStage, PassagesStage, TriageStage
from ingestion_workflow.pipeline.stages.extract import SYMBOLS_AS_DIGITS, extraction_fingerprint
from ingestion_workflow.services.funnel_report import build_funnel, open_readonly

#: The PDF text of 2fbrux6ypmwb's results and legend, as Docling read it.
MISREAD = (
    "Activation was thresholded at P 5 0.001 uncorrected with a cluster size5 20 voxels. "
    "The left hippocampus responded more to novel scenes (peak voxel MNI coordinate: "
    "x5222, y556, z542, Z53.48) and the right one to familiar scenes (x5244, y518, z532, "
    "Z53.12). Figure 2. Peaks at 222, 56, 42; 244, 18, 32 (P 5 0.002)."
)
#: The same passage as ACE read it from the publisher's HTML.
TYPED = (
    "Activation was thresholded at P = 0.001 uncorrected with a cluster size = 20 voxels. "
    "The left hippocampus responded more to novel scenes (peak voxel MNI coordinate: "
    "x = -22, y = 56, z = 42, Z = 3.48) and the right one to familiar scenes "
    "(x = -44, y = 18, z = 32, Z = 3.12). Figure 2. Peaks at -22, 56, 42; -44, 18, 32 "
    "(P < 0.002)."
)


def test_the_misread_layer_is_recognised():
    assert symbols_as_digits(MISREAD) == {"misread": 5, "typed": 0}


def test_a_text_with_its_symbols_typed_is_not():
    assert symbols_as_digits(TYPED) is None


def test_one_or_two_coincidences_are_not_a_misread_layer():
    """A cluster of "size 5 12" voxels and an axis labelled "x 5 210, y 5" happen."""
    assert symbols_as_digits("clusters of size 5 12 voxels at (x 5 210, y 5 mm)") is None
    assert symbols_as_digits("clusters of size 5 12 voxels") is None


def test_a_text_with_more_typed_p_values_than_misread_forms_is_not():
    """Three coincidences in a paper that types its p-values the rest of the time."""
    coincidences = "size 5 12, size 5 14 and size 5 16 voxels"
    typed = " ".join(f"P = 0.0{i}" for i in range(1, 5))
    assert symbols_as_digits(f"{coincidences}. {typed}") is None
    assert symbols_as_digits(f"{coincidences}. {typed[:16]}") is not None


# -- the stages ------------------------------------------------------------------


@pytest.fixture()
def env(tmp_path):
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a gate")
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        coordinate_gate_path=str(gate),
        download_sources=["wiley", "pdf", "ace"],
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, tmp_path


def _downloaded(catalog, ref, *sources):
    catalog.record([
        Outcome(article_id=ref.id, stage="download", source=source, fingerprint=f"dl-{source}",
                payload={"files": []}, summary={"files": 1})
        for source in sources
    ])
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="meta",
                            payload={})])


def _extracted(settings, catalog, ref, source, download_source, text, tmp_path):
    """Record what the extract stage makes of `text` and one table of peaks read from it."""
    path = tmp_path / f"{ref.id}-{source}.txt"
    path.write_text(text, encoding="utf-8")
    download = catalog.artifact(ref.id, "download", download_source)
    work = Work(ref=ref, source=source, upstream=download,
                fingerprint=extraction_fingerprint(download_source, download))
    content = ExtractedContent(
        slug=ref.id, source=DownloadSource(source), full_text_path=path,
        tables=[ExtractedTable(table_id="t1", raw_content_path=None,
                               coordinates=[Coordinate(x=222.0, y=56.0, z=42.0)])],
        figure_captions=[{"ids": ["2"], "span": [0, 10]}],
    )
    outcome = ExtractStage(settings)._outcome(work, source, content)
    catalog.record([outcome])
    return outcome


def _plan(stage, settings, catalog, ref, upstream_stage):
    ids = [ref.id]
    return stage.plan(Context(settings, catalog), [ref], catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, upstream_stage))


def test_a_misread_pdf_holds_no_text_and_no_tables_and_says_why(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley")

    outcome = _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)

    assert outcome.status is Status.OK
    assert outcome.summary == {
        "tables": 0, "tables_with_coordinates": 0, "has_text": False,
        "reason": SYMBOLS_AS_DIGITS, "symbols_as_digits": {"misread": 5, "typed": 0},
        "tables_read": 1}
    content = ExtractedContent.from_dict(outcome.payload)
    assert (content.full_text_path, content.tables, content.figure_captions) == (None, [], [])


def test_a_pdf_with_its_symbols_typed_keeps_its_text_and_tables(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="1"))
    _downloaded(catalog, ref, "pdf")

    outcome = _extracted(settings, catalog, ref, "pdf", "pdf", TYPED, tmp_path)

    assert (outcome.summary["has_text"], outcome.summary["tables"]) == (True, 1)
    assert "reason" not in outcome.summary


def test_only_a_pdf_text_layer_is_checked(env):
    """The misreading is a PDF text layer's; ACE reads the publisher's HTML."""
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="2"))
    _downloaded(catalog, ref, "ace")

    outcome = _extracted(settings, catalog, ref, "ace", "ace", MISREAD, tmp_path)

    assert (outcome.summary["has_text"], outcome.summary["tables"]) == (True, 1)


def test_the_source_below_a_misread_pdf_is_extracted_and_then_the_article_is_fresh(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley", "ace")
    _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)

    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")
    assert [(w.source, w.upstream.source) for w in plan.pending] == [("ace", "ace")]

    _extracted(settings, catalog, ref, "ace", "ace", TYPED, tmp_path)
    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")
    assert (plan.pending, plan.fresh) == ([], 1)


def test_a_misread_pdf_alone_is_fresh_not_retried(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley")
    _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)

    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")

    assert (plan.pending, plan.fresh, plan.permanent) == ([], 1, 0)


def test_triage_judges_the_ace_text_beside_a_misread_pdf(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley", "ace")
    _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)
    _extracted(settings, catalog, ref, "ace", "ace", TYPED, tmp_path)

    (work,) = _plan(TriageStage(settings), settings, catalog, ref, "metadata").pending
    assert work.upstream.source == "ace"
    (work,) = _plan(PassagesStage(settings), settings, catalog, ref, "extract").pending
    assert work.upstream.source == "ace"


def test_what_was_made_from_a_misread_pdf_alone_is_taken_back(env):
    """Passages read from the garbled text before it was recognised are taken back, and
    triage judges the extraction again, which now holds no table."""
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley")
    _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)
    catalog.record([
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="from-misread-text",
                payload={"passages": []}, summary={"passages": 3, "source": "pdf"}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="from-misread-tables",
                payload={"source": "pdf", "tables": []}, summary={"source": "pdf"}),
    ])

    passages = PassagesStage(settings)
    plan = _plan(passages, settings, catalog, ref, "extract")
    (taken,) = passages.execute(Context(settings, catalog), plan.pending)
    assert (taken.status, taken.error) == (Status.FAILED, NO_TEXT)

    (work,) = _plan(TriageStage(settings), settings, catalog, ref, "metadata").pending
    assert work.upstream.source == "pdf"
    assert ExtractedContent.from_dict(catalog.payload(work.upstream)).tables == []


def test_the_funnel_gives_the_reason_on_the_pdf_extraction(env):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"))
    _downloaded(catalog, ref, "wiley")
    _extracted(settings, catalog, ref, "pdf", "wiley", MISREAD, tmp_path)

    funnel = build_funnel(open_readonly(settings.catalog_root))

    assert ("extract", "pdf", "ok", SYMBOLS_AS_DIGITS) in {k[:4] for k in funnel.sources}
