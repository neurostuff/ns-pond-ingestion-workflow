"""A PDF whose text layer maps math symbols to digits holds nothing.

Article 2fbrux6ypmwb (PMID 27004598, HBM, Wiley PDF): its ACE text reads
`x=-22, y=56, z=42, Z=3.48`, its PDF text `x5222, y556, z542, Z53.48`, and the
figure legend `222, 56, 42` for `-22, 56, 42`. Ranked above ACE, the PDF text
replaced the good one. The excerpts below are from the stored texts of the
articles named.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.docling_convert import normalize_text_tokens
from ingestion_workflow.extractors.pdf_extractor import symbols_as_digits
from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable, Identifier
from ingestion_workflow.models.analysis import Coordinate
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stage import NO_TEXT
from ingestion_workflow.pipeline.stages import ExtractStage, PassagesStage, TriageStage
from ingestion_workflow.pipeline.stages.extract import SYMBOLS_AS_DIGITS, extraction_fingerprint
from ingestion_workflow.services.funnel_report import build_funnel, open_readonly
from ingestion_workflow.pipeline.stage import metadata_digests

#: `=` as 5, its minus as 2: P values, coordinates, a matrix size.
HBM_2FBRUX6YPMWB = (
    "between the N-back task and the DDT (r 5 20.004, P 5 0.979). "
    "Pcorrected50.047; peak voxel MNI coordinate: x5222, y556, z542, Z53.48, Puncorrected "
    "Pcorrected50.019; peak voxel MNI coordinate: x5266, y5212, z5214, Z53.63, Puncorrected "
    "flip angle578; FOV5256 3 256 mm; and matrix size5256 3 256."
)
#: `=` as 5 beside a `<` the font kept, which outnumbers it.
HBM_JWPMJ3GZ7EMV = (
    "| P 50.77 |\n| P 50.19 |\n| 0.74 | P < 0.0001 | P 5 0.90 |\n"
    "is significant (P<0.05) and when all its coefficients are significant using "
    "t-statistic (P<0.05). This is assessed by the selected hybrid model (P<0.001)"
)
#: `=` as 5 in APA p-values, with no leading zero.
JINS_H6Y37JZOLVND = (
    "using an individual voxel probability threshold of p 5 .001, indicated that "
    "resulted in an overall significance of p 5 .048. There was a significant effect of "
    "group (F(1,21) 5 7.17, p 5 .014)"
)
#: The minus as a NUL, `=` intact.
W3VRP6ZACAN4 = (
    "in the pre-SMA (x = \x004, y = 6, z = 52). Activations were found in the right cuneus "
    "(x = 12, y = \x0086, z = 34), the inferior temporal gyrus (x = \x0052, y = \x0062, "
    "z = \x002). [F(1, 23) = 2.46, p = .13] [F(3, 69) = 5.87, p = .003]"
)
#: Table cells, as Docling read them: `| \x0056 |` is -56.
VDS7LXQISVPJ_CELLS = "| L IFG | \x0044 | 18 | \x0056 |"
JJS7G7BVLOBB_CELL = "<th>Left anterior MFG (BA 10)</th><td>\x0030</td><td>50</td>"
#: Both: `=` as 5, and the minus as a NUL that the repair restores.
PSYCHOPHYSIOLOGY_SUKDWLE3ICVJ = (
    "during phobic than nonphobic stimulus presentation (x528, y5 \x008, z5 \x0013, "
    "z53.28, p5.01; Figure 2A). related to arousal level (right hemisphere: x514, "
    "y5 \x0031, z5 \x007, z53.31, p5.1, 17 voxels"
)
#: A sound text layer: `=` and `<` typed, coordinates with their minus signs.
CLEAN_CD6FEP6C7MXV = (
    "with coordinates in a similarly central location (x = -24, y = 0, z = -16). By "
    "contrast, in response (F = 3.74, degrees of freedom [df] = 1,14, P = 0.07) between "
    "condition elicited by supraliminal neutral (t = 4.28, df = 14, P = 0.001), "
    "in hypothesized regions of interest (threshold, P < 0.05, SVC) | 2, 34, \t4 | 38 |"
)


def _read(text):
    """What the pdf extractor keeps of a text layer's text, and what it sets aside."""
    repaired = normalize_text_tokens(text)
    return repaired, symbols_as_digits(repaired)


@pytest.mark.parametrize("raw, repaired", [
    ("in the pre-SMA (x = \x004, y = 6, z = 52)", "in the pre-SMA (x = -4, y = 6, z = 52)"),
    (VDS7LXQISVPJ_CELLS, "| L IFG | -44 | 18 | -56 |"),
    (JJS7G7BVLOBB_CELL, "<th>Left anterior MFG (BA 10)</th><td>-30</td><td>50</td>"),
    # 7qdjcoevplk5: -80 C, its degree sign still misread
    ("stored until sectioning at \x00801C.", "stored until sectioning at -801C."),
])
def test_a_nul_before_a_digit_is_read_as_the_minus_it_was(raw, repaired):
    assert normalize_text_tokens(raw) == repaired


@pytest.mark.parametrize("raw, read", [
    # nz6cotdhpgtx, ccv4fmkvh6q2: with a space it was a binary minus or an approximation,
    # read as a minus like the table cells' "| \x00 9 |"; Postgres stores no NUL
    ("orthogonal to (n \x00 1) lead vectors", "orthogonal to (n -1) lead vectors"),
    ("(AV-1451: median \x00 3 months)", "(AV-1451: median -3 months)"),
    # xd5on45nm3no: not before a digit
    ("TRP\x00 channels", "TRP- channels"),
])
def test_a_nul_anywhere_else_is_read_as_a_minus_too(raw, read):
    assert normalize_text_tokens(raw) == read


def test_a_sound_text_is_left():
    assert normalize_text_tokens(CLEAN_CD6FEP6C7MXV) == CLEAN_CD6FEP6C7MXV


def test_a_text_whose_only_misreading_is_the_nul_minus_is_repaired_not_set_aside():
    repaired, misread = _read(W3VRP6ZACAN4)
    assert misread is None
    assert "(x = -4, y = 6, z = 52)" in repaired and "z = -2)" in repaired


def test_a_text_that_still_reads_equals_as_5_after_the_repair_is_set_aside():
    repaired, misread = _read(PSYCHOPHYSIOLOGY_SUKDWLE3ICVJ)
    assert "(x528, y5 -8, z5 -13," in repaired
    assert misread == {"misread": 6, "typed": 0}


@pytest.mark.parametrize("text, misread", [
    (HBM_2FBRUX6YPMWB, 4), (HBM_JWPMJ3GZ7EMV, 3), (JINS_H6Y37JZOLVND, 3),
], ids=["2fbrux6ypmwb", "jwpmj3gz7emv", "h6y37jzolvnd"])
def test_the_misread_layers_found_in_the_corpus_are_recognised(text, misread):
    assert symbols_as_digits(text) == {"misread": misread, "typed": 0}


@pytest.mark.parametrize("form", [
    "P 5 0.05", "p50.77", "p 5 .001", "x5222, y5", "z5214, x5", "y5 -8, z5", "size5 20",
])
def test_each_misread_form_counts(form):
    assert symbols_as_digits("; ".join([form] * 3)) is not None


@pytest.mark.parametrize("near_miss", [
    "ap 5 0.05", "p 5 1.5", "size 5 1 ", "x5222 and y5", "x = 22, y = 5", "max 5 0.2",
])
def test_what_only_looks_like_a_misread_form_does_not(near_miss):
    assert symbols_as_digits("; ".join([near_miss] * 3)) is None


def test_a_sound_text_layer_is_not_misread():
    assert symbols_as_digits(CLEAN_CD6FEP6C7MXV) is None


def test_one_or_two_coincidences_are_not_a_misread_layer():
    assert symbols_as_digits("clusters of size 5 12 voxels at (x 5 210, y 5 mm)") is None


@pytest.mark.parametrize("misread, typed", [
    ("P50.019, P50.203, P 5 0.979", "P = 0.07, P = 0.001, P = 0.04"),
    ("p 5 .001, p 5 .048, p 5 .014", "p = .13, p = .003, p = .034"),
])
def test_equals_misread_no_more_often_than_it_is_typed_is_not_misread(misread, typed):
    assert symbols_as_digits(f"{misread}. {typed}") is None
    first = misread.split(", ")[0]
    assert symbols_as_digits(f"{misread}, {first}. {typed}") is not None


# -- the extractor -----------------------------------------------------------------


def _pdf(path: Path, lines, table=None) -> Path:
    """A one-page PDF whose text layer holds `lines`, and `table` (rows of cells) in a
    ruled grid below them, as a publisher's would."""
    ops = "".join(f"BT /F1 11 Tf 56 {760 - 16 * i} Td ({line}) Tj ET\n"
                  for i, line in enumerate(lines))
    for r, row in enumerate(table or []):
        top = 640 - 22 * r
        for c, cell in enumerate(row):
            left = 56 + 110 * c
            ops += (f"{left} {top - 22} 110 22 re S\n"
                    f"BT /F1 10 Tf {left + 6} {top - 15} Td ({cell}) Tj ET\n")
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        "/Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(ops)} >>\nstream\n{ops}endstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out, offsets = "%PDF-1.4\n", []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n"
    out += "".join(f"{offset:010d} 00000 n \n" for offset in offsets)
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    path.write_bytes(out.encode("latin-1"))
    return path


def _read_pdf(tmp_path, lines, table=None):
    from ingestion_workflow.extractors.pdf_extractor import _extract_pdf_article
    from ingestion_workflow.models import DownloadedFile, DownloadResult, FileType

    pdf = _pdf(tmp_path / "article.pdf", lines, table)
    identifier = Identifier(pmid="27004598")
    download = DownloadResult(identifier, DownloadSource.WILEY, True, files=[
        DownloadedFile(file_path=pdf, file_type=FileType.PDF, content_type="application/pdf",
                       source=DownloadSource.WILEY)])
    return _extract_pdf_article(download, tmp_path / "extractions")


MISREAD_LINES = [
    "Activation was thresholded at P 5 0.001 with a cluster size5 20 voxels.",
    "Peak voxel MNI coordinate: x5222, y556, z542, Z53.48 (P 5 0.002).",
]
TABLE = [["Region", "x", "y", "z"], ["L IFG", "-44", "18", "-56"],
         ["R IFG", "46", "20", "4"]]


def test_the_pdf_extractor_sets_a_misread_text_aside(tmp_path):
    content = _read_pdf(tmp_path, MISREAD_LINES, TABLE)

    assert content.symbols_as_digits == {"misread": 4, "typed": 0, "tables_set_aside": 1}
    assert (content.full_text_path, content.tables, content.figure_captions) == (None, [], [])


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
                            payload={}, summary={"digests": metadata_digests({})})])


def _table():
    return ExtractedTable(table_id="t1", raw_content_path=None,
                          coordinates=[Coordinate(x=222.0, y=56.0, z=42.0)])


def _work(catalog, ref, source, download_source):
    download = catalog.artifact(ref.id, "download", download_source)
    return Work(ref=ref, source=source, upstream=download,
                fingerprint=extraction_fingerprint(download_source, download))


def _set_aside(settings, catalog, ref, download_source="wiley"):
    """Record what the extract stage makes of the pdf extractor's set-aside content."""
    content = ExtractedContent(
        slug=ref.id, source=DownloadSource.PDF,
        symbols_as_digits={"misread": 7, "typed": 0, "tables_set_aside": 2})
    outcome = ExtractStage(settings)._outcome(
        _work(catalog, ref, "pdf", download_source), "pdf", content)
    catalog.record([outcome])
    return outcome


def _extracted(settings, catalog, ref, source, text, tmp_path, tables=1):
    path = tmp_path / f"{ref.id}-{source}.txt"
    path.write_text(text, encoding="utf-8")
    content = ExtractedContent(slug=ref.id, source=DownloadSource(source), full_text_path=path,
                               tables=[_table() for _ in range(tables)])
    outcome = ExtractStage(settings)._outcome(_work(catalog, ref, source, source), source,
                                              content)
    catalog.record([outcome])
    return outcome


def _plan(stage, settings, catalog, ref, upstream_stage):
    ids = [ref.id]
    return stage.plan(Context(settings, catalog), [ref], catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, upstream_stage))


def test_set_aside_content_is_recorded_as_holding_nothing_and_why(env, evidence):
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, "wiley")

    outcome = _set_aside(settings, catalog, ref)

    assert outcome.status is Status.OK
    assert outcome.summary == {
        "tables": 0, "tables_with_coordinates": 0, "has_text": False,
        "symbols_as_digits": {"misread": 7, "typed": 0, "tables_set_aside": 2}}
    content = ExtractedContent.from_dict(outcome.payload)
    assert (content.full_text_path, content.tables) == (None, [])
    assert content.symbols_as_digits["tables_set_aside"] == 2


def test_a_pdf_read_into_tables_and_no_text_is_recorded_as_before(env, evidence):
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="3"), evidence)
    _downloaded(catalog, ref, "pdf")
    content = ExtractedContent(slug=ref.id, source=DownloadSource.PDF, tables=[_table()])

    outcome = ExtractStage(settings)._outcome(_work(catalog, ref, "pdf", "pdf"), "pdf", content)

    assert (outcome.status, outcome.summary["tables"], outcome.summary["has_text"]) == (
        Status.OK, 1, False)
    assert "symbols_as_digits" not in outcome.summary


def test_the_source_below_a_set_aside_pdf_is_extracted_and_then_the_article_is_fresh(
        env, evidence):
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, "wiley", "ace")
    _set_aside(settings, catalog, ref)

    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")
    assert [(w.source, w.upstream.source) for w in plan.pending] == [("ace", "ace")]

    _extracted(settings, catalog, ref, "ace", CLEAN_CD6FEP6C7MXV, tmp_path)
    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")
    assert (plan.pending, plan.fresh) == ([], 1)


@pytest.mark.parametrize("ace_failed", [False, True])
def test_a_set_aside_pdf_with_nothing_else_due_is_fresh_not_retried(env, ace_failed, evidence):
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, *(["wiley", "ace"] if ace_failed else ["wiley"]))
    if ace_failed:
        catalog.record([Outcome.failure(ref.id, "extract", "ace", "nope", permanent=True)])
    _set_aside(settings, catalog, ref)

    plan = _plan(ExtractStage(settings), settings, catalog, ref, "download")

    assert (plan.pending, plan.fresh, plan.permanent) == ([], 1, 0)


@pytest.mark.parametrize("ace_tables", [1, 0])
def test_triage_and_passages_read_the_ace_text_beside_a_set_aside_pdf(env, ace_tables, evidence):
    """Also when ACE kept no table, as for a text-only ACE beside a PDF whose two tables
    were set aside."""
    settings, catalog, tmp_path = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, "wiley", "ace")
    _set_aside(settings, catalog, ref)
    _extracted(settings, catalog, ref, "ace", CLEAN_CD6FEP6C7MXV, tmp_path, tables=ace_tables)

    (work,) = _plan(TriageStage(settings), settings, catalog, ref, "metadata").pending
    assert work.upstream.source == "ace"
    (work,) = _plan(PassagesStage(settings), settings, catalog, ref, "extract").pending
    assert work.upstream.source == "ace"


def _made_from_the_misread_text(catalog, ref):
    catalog.record([
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="from-misread-text",
                payload={"passages": []}, summary={"passages": 3, "source": "pdf"}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="from-misread-tables",
                payload={"source": "pdf", "tables": []}, summary={"source": "pdf"}),
    ])


def test_what_was_made_from_a_set_aside_pdf_alone_is_taken_back(env, evidence):
    """Passages read from the misread text before it was set aside are taken back, and
    triage judges the extraction again, which now holds no table."""
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, "wiley")
    _made_from_the_misread_text(catalog, ref)
    _set_aside(settings, catalog, ref)

    passages = PassagesStage(settings)
    plan = _plan(passages, settings, catalog, ref, "extract")
    (taken,) = passages.execute(Context(settings, catalog), plan.pending)
    assert (taken.status, taken.error) == (Status.FAILED, NO_TEXT)

    (work,) = _plan(TriageStage(settings), settings, catalog, ref, "metadata").pending
    assert work.upstream.source == "pdf"
    assert ExtractedContent.from_dict(catalog.payload(work.upstream)).tables == []


def test_the_funnel_names_the_set_aside_text_as_why_an_article_has_none(env, evidence):
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="27004598"), evidence)
    _downloaded(catalog, ref, "wiley")
    _made_from_the_misread_text(catalog, ref)
    _set_aside(settings, catalog, ref)
    passages = PassagesStage(settings)
    catalog.record(list(passages.execute(
        Context(settings, catalog), _plan(passages, settings, catalog, ref, "extract").pending)))
    other = catalog.register(Identifier(pmid="4"), evidence)
    _downloaded(catalog, other, "pdf")
    catalog.record([
        Outcome(article_id=other.id, stage="extract", source="pdf", fingerprint="x",
                payload={}, summary={"tables": 0, "has_text": False}),
        Outcome.failure(other.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
    ])
    # an extraction that notes some other reason is not one set aside
    noted = catalog.register(Identifier(pmid="8"), evidence)
    _downloaded(catalog, noted, "pdf")
    catalog.record([
        Outcome(article_id=noted.id, stage="extract", source="pdf", fingerprint="x",
                payload={}, summary={"tables": 0, "has_text": False, "reason": "a note"}),
        Outcome.failure(noted.id, "passages", "", NO_TEXT, fingerprint=NO_TEXT),
    ])

    funnel = build_funnel(open_readonly(settings.catalog_root))

    assert ("extract", "pdf", "ok", SYMBOLS_AS_DIGITS) in {k[:4] for k in funnel.sources}
    reasons = {aid: key[2] for key, (_, sample) in funnel.cells.items()
               if key[0] == "passages" for aid, _ in sample}
    assert reasons == {ref.id: SYMBOLS_AS_DIGITS, other.id: NO_TEXT, noted.id: NO_TEXT}
    assert ("extract", "pdf", "ok", "") in {k[:4] for k in funnel.sources}


# -- the manifest of stored misread texts -----------------------------------------

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "symbols_as_digits_manifest.py"


def _script():
    spec = importlib.util.spec_from_file_location("symbols_as_digits_manifest", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_manifest_lists_the_articles_whose_stored_pdf_text_would_now_read_differently(
        env, evidence):
    """Extractions stored before the extractor repaired a NUL minus or set a misread text
    aside: a misread text and one with a NUL minus are listed, the latter at a path
    recorded relative to the catalog's directory. A sound PDF, a misread ACE text, a
    failed pdf extraction and one already set aside are not."""
    settings, catalog, tmp_path = env
    names = ["misread", "nul minus", "sound", "ace", "failed", "set aside"]
    refs = {name: catalog.register(Identifier(pmid=str(20 + i), doi=f"10.1002/hbm.{20 + i}"),
            evidence)
            for i, name in enumerate(names)}
    for name, ref in refs.items():
        _downloaded(catalog, ref, "ace" if name == "ace" else "wiley")
    texts = tmp_path / "texts"
    texts.mkdir()
    for name, source, text, status in [
            ("misread", "pdf", HBM_2FBRUX6YPMWB, Status.OK),
            ("nul minus", "pdf", W3VRP6ZACAN4, Status.OK),
            ("sound", "pdf", CLEAN_CD6FEP6C7MXV, Status.OK),
            ("ace", "ace", HBM_2FBRUX6YPMWB, Status.OK),
            ("failed", "pdf", HBM_2FBRUX6YPMWB, Status.FAILED)]:
        (texts / f"{name}.txt").write_text(text, encoding="utf-8")
        catalog.record([Outcome(
            article_id=refs[name].id, stage="extract", source=source, status=status,
            fingerprint="v2", payload={"full_text_path": f"texts/{name}.txt"},
            summary={"tables": 1, "has_text": True})])
    _set_aside(settings, catalog, refs["set aside"])
    manifest = tmp_path / "misread.jsonl"

    assert _script().main([str(settings.catalog_root), str(manifest)]) == 0

    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert all(set(row) == {"neurostore", "pmid", "doi", "pmcid", "other_ids"} for row in rows)
    assert sorted(row["pmid"] for row in rows) == ["20", "21"]
