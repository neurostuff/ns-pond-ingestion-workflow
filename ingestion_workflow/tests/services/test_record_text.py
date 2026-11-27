"""The text a trimmed-1 record request carries: copied tables and reference lists left out,
nothing else. The shapes are the ones the synced corpus holds (2026-11-20): ACE writes a
table out several times; ACE and PDF texts keep their reference list, the figure legends
after it."""

from __future__ import annotations

from ingestion_workflow.services.offload.record_text import Trim, drop_repeats, record_text

TABLE = (
    "Table 1\n\nDemographic data and clinical characteristics.\n\n"
    "Characteristic .\tDMD group .\tHC group .\nAge (year)\t8.78\t9.24\nFull IQ a\t84.41\t105.24\n"
    "Verbal Comprehension Index a\t86.10\t100.29\n\n"
    "Abbreviations: DMD, Duchenne muscular dystrophy; HC, healthy control; "
    "IQ, intelligence quotient.\n\n"
    "aSignificant differences in between group comparation (P<0.05).\n\n"
)


def test_a_table_ace_wrote_out_again_is_kept_once():
    text = (
        "We enrolled 60 participants.\n\n"
        + TABLE
        + TABLE
        + TABLE
        + "The inclusion criteria were these."
    )

    out = drop_repeats(text, trim := Trim())

    assert out.count("Verbal Comprehension Index a\t86.10") == 1
    assert out.count("Abbreviations: DMD") == 1
    assert out.startswith("We enrolled 60 participants.") and out.endswith(
        "The inclusion criteria were these."
    )
    # the labels stay where they were: short, they may mean something in place
    assert out.count("Table 1") == 3 and trim.repeats == 8


def test_a_heading_a_lone_footnote_and_a_citation_broken_out_are_never_left_out():
    footnote = (
        "L, left; R, right; BA, Brodmann area; FDR-corrected p < 0.05, cluster size > 600 mm3."
    )
    text = (
        "### Experiment 1. Reversal learning\n\nMethods of experiment 1.\n\n"
        f"Table 2 rows\n\n{footnote}\n\nTable 3 rows\n\n{footnote}\n\n"
        "Activity rose (\n\nSmith et al., 2011\n\n;\n\nJones et al., 2012\n\n) and fell (\n\n"
        "Smith et al., 2011\n\n;\n\nJones et al., 2012\n\n).\n\n"
        "### Experiment 1. Reversal learning\n\nResults of experiment 1."
    )

    assert drop_repeats(text, Trim()) == text


def _paper(
    list_lines, after="Figure legends\n\nFig. 1. Activation for faces > houses at p < 0.001."
):
    body = (
        "The insula responded to faces (Smith et al., 2011).\n\n"
        "Acknowledgements\n\nWe thank the participants.\n\n"
    )
    return body + "References\n\n" + "\n\n".join(list_lines) + "\n\n" + after


ENTRIES = [
    f"Author{i} A, Other B ({1990 + i}). A study of thing {i}. J Neurosci {i}:{i}-{i + 9}."
    for i in range(12)
]


def test_a_reference_list_is_left_out_up_to_the_figure_legends():
    text = _paper(ENTRIES)

    out, trim = record_text(text)

    assert "Author3 A" not in out and len(trim.lists) == 1
    assert "References\n[The reference list (12 lines) is left out of this text.]" in out
    assert out.endswith("Figure legends\n\nFig. 1. Activation for faces > houses at p < 0.001.")
    assert "We thank the participants." in out and "Smith et al., 2011" in out


def test_a_list_runs_to_the_next_kept_heading_or_caption_whatever_follows():
    for after in (
        "Supporting information\n\nS1 Table. Peaks.",
        "Supplementary Material\n\nPlease find it below.",
        "Table 1. Demographics of the sample.",
        "Appendix A. Stimuli\n\nThe words were these.",
    ):
        out, _ = record_text(_paper(ENTRIES, after))
        assert out.endswith(after) and "Author3 A" not in out


def test_ace_s_link_labels_and_a_list_split_into_fields_are_a_list():
    labelled = [line for e in ENTRIES for line in (e, "Article", "PubMed", "Google Scholar")]
    fields = [
        line
        for i in range(30)
        for line in (
            f"Author{i} A,  Other B.",
            f"Author{i}",
            "A",
            "Other",
            "B",
            f"A long title of a paper about the thing numbered {i} in this list",
            f"Brain {1990 + i}",
        )
    ]
    for lines in (labelled, fields):
        out, trim = record_text(_paper(lines))
        assert len(trim.lists) == 1 and "Author3" not in out and out.endswith("p < 0.001.")


def test_what_is_not_a_list_stays_after_a_reference_heading():
    links = [
        "Related",
        "Information",
        "Recommended",
        "Involvement of brain structures in childhood epilepsy with centrotemporal spikes "
        "and more words",
    ]
    prose = [
        "The second experiment "
        + "tested the same participants on a harder version of the task. " * 5,
        "Its results " + "replicated the first, with a larger effect in the anterior insula. " * 5,
    ]
    for lines in (links, prose, ENTRIES[:3]):
        text = _paper(lines)
        assert record_text(text) == (text, Trim())


def test_two_paragraphs_of_prose_end_a_list():
    prose = (
        "Supplementary Discussion. " + "We considered whether the effect reflected arousal. " * 6
    )
    text = _paper(ENTRIES + [prose, prose.replace("arousal", "attention")], after="")

    out, trim = record_text(text)

    assert "Author3 A" not in out and prose in out and len(trim.lists) == 1


def test_pubget_elsevier_and_pmc_texts_hold_no_list_and_are_left_as_they_are():
    text = "## Methods\n\nWe scanned 24 patients.\n\n## Figure legends\n\nFig. 1. Activation."
    assert record_text(text) == (text, Trim())


# -- trimmed-2: fit_text and anchors_of ----------------------------------------------------

from ingestion_workflow.services.offload.record_text import anchors_of, fit_text  # noqa: E402


def words(s: str) -> int:
    """A stand-in token count: words."""
    return len(s.split())


def _book(n: int, target: int) -> str:
    """An abstract book's shape (the over-long papers of 2026-11-27: AAIC and EAN books): a
    heading and a short abstract per poster, the analysis in one of them."""
    parts = ["Proceedings of the meeting. Posters, Thursday."]
    for k in range(n):
        body = (f"Poster {k} reports a study of something else entirely, with its methods "
                "and results described in a few sentences of prose.")
        if k == target:
            body = ("We scanned 40 adults; the left insula responded to pain > touch "
                    "(MNI -38, 12, 4) at FWE p < 0.05.")
        parts.append(f"### P{k:03d}- A POSTER TITLE {k}\n\n{body}")
    return "\n\n".join(parts)


def test_a_text_within_the_budget_is_sent_whole():
    text = _book(5, 2)

    assert fit_text(text, [(0, 5)], budget=10_000, ntok=words)[0] == text


def test_an_abstract_book_keeps_the_abstract_its_analysis_is_in_and_its_heading():
    text = _book(300, 217)
    at = text.index("the left insula")

    out, fit = fit_text(text, [(at, at + 15)], budget=200, ntok=words)

    assert "### P217- A POSTER TITLE 217" in out and "(MNI -38, 12, 4)" in out
    assert out.startswith("Proceedings of the meeting.")
    gap_line = " characters of the text are left out here.]"
    assert words(out.replace(gap_line, "")) <= 200 + 4 * fit.gaps
    assert fit.anchored == 1 and fit.gaps >= 2 and gap_line in out
    # what is kept stays in the paper's order
    assert out.index("POSTER TITLE 217") > out.index("Proceedings")


def test_an_unbroken_block_is_cut_so_the_analysis_keeps_only_its_neighbourhood():
    """One of the 62 (wrqhsiuwrxmx) is a 715k-token block with no heading or blank line."""
    filler = "unrelated case history sentence. " * 3000
    text = filler + "The thalamus responded to the task at x=12, y=-18, z=6. " + filler
    at = text.index("The thalamus")

    out, _ = fit_text(text, [(at, at + 12)], budget=1500, ntok=words)

    assert "x=12, y=-18, z=6" in out and words(out) < 1600 and len(out) < len(text) // 10


def test_the_methods_and_results_come_before_the_rest_once_the_analyses_are_kept(monkeypatch):
    """The analysis sits at the start of the results, right after a supplement and before the
    discussion: nearest-first alone would take those; the methods, far off, come first."""
    from ingestion_workflow.services.offload import record_text as rt

    monkeypatch.setattr(rt, "FRONT_TOKENS", 10)
    sections, parts, pos = [], [], 0
    for kind, word in (("introduction", "background"), ("methods", "scanner"),
                       ("supplement", "appendix"), ("results", "activation"),
                       ("discussion", "speculation")):
        block = "\n\n".join(f"{word} paragraph {k} " + "filler " * 40 for k in range(10))
        parts.append(block)
        sections.append({"kind": kind, "start_char": pos, "end_char": pos + len(block)})
        pos += len(block) + 2
    text = "\n\n".join(parts)
    at = text.index("activation paragraph 0")

    out, _ = fit_text(text, [(at, at + 10)], budget=880, ntok=words, sections=sections)

    assert out.count("scanner paragraph") == 10 and out.count("activation paragraph") == 10
    assert "speculation paragraph" not in out and "appendix paragraph" not in out


def test_anchors_are_text_spans_table_spans_and_else_where_the_table_label_is_printed():
    text = "Intro.\n\nTable 3 Peaks of the contrast.\n\nrows\n\nTable S2 Supplementary peaks."
    entries = [{"key": "a", "text_spans": [{"start_char": 0, "end_char": 5}]},
               {"key": "b", "table_id": "t1"}, {"key": "c", "table_id": "t2"},
               {"key": "d", "table_id": "t3"}, {"key": "e", "table_id": "missing"}]
    tables = [{"table_id": "t1", "text_span": {"start_char": 8, "end_char": 40}},
              {"table_id": "t2", "label": "Table S2", "caption": None},
              {"table_id": "t3", "label": "None", "caption": "Peaks of the contrast."}]

    assert anchors_of(text, entries, tables) == [
        (0, 5), (8, 40), (text.index("Table S2"), text.index("Table S2") + 8),
        (text.index("Peaks of"), text.index("Peaks of") + 22)]


# -- trimmed-2: numeric supplementary data, lists of tables, ranges ------------------------

from ingestion_workflow.services.offload.record_text import (  # noqa: E402
    DATA,
    _row_in_text,
    carve,
    numeric_data_spans,
    outside,
)

SOURCE_DATA = "\n".join("\t".join(f"{(k * 7 + j) % 997}.{j}" for j in range(12))
                        for k in range(60))
PEAKS = "Region\tx\ty\tz\tT\nLeft insula\t-38\t12\t4\t5.2\nRight amygdala\t24\t-4\t-18\t4.7"


def _eLife(source_tables=("s1-sheet-01",), used=("s2-table-001",), header=""):
    """An eLife-shaped supplement (the over-long papers of 2026-11-27): a source-data xlsx
    whose sheet is all numbers, then a docx of peak tables an analysis came from."""
    body = "Introduction and methods of the paper. " * 20
    supp = ("## Supplementary material\n\n"
            "### Supplementary file 1 (elife-1-fig4-data1.xlsx): Figure 4—source data 1.\n\n"
            "Feuil1\n" + header + SOURCE_DATA + "\n\n"
            "### Supplementary file 2 (elife-1-supp2.docx): Supplementary file 2. Peaks.\n\n"
            + PEAKS + "\n")
    text = body + "\n\n" + supp
    a = len(body) + 2
    parsed = {"sections": [{"kind": "supplement", "start_char": a, "end_char": len(text)}],
              "tables": [{"table_id": "s1-sheet-01",
                          "caption": "Supplementary file 1 (elife-1-fig4-data1.xlsx): Feuil1"},
                         {"table_id": "s2-table-001",
                          "caption": "Supplementary file 2 (elife-1-supp2.docx)"},
                         {"table_id": "s3-sheet-01",
                          "caption": "Supplementary file 3 (elife-1-supp3.xlsx): Sheet1"}]}
    entries = [{"key": f"analysis#{t}", "table_id": t, "points": [{"row": 1}]} for t in used]
    return text, parsed, entries


def test_a_source_data_sheet_no_analysis_came_from_is_left_out_but_its_name_line_kept():
    text, parsed, entries = _eLife()

    (span,) = numeric_data_spans(text, parsed, entries)

    start, end, what = span
    assert what == DATA and SOURCE_DATA in text[start:end]
    assert text[:start].endswith("Feuil1\n") and "Supplementary file 2" in text[end:]


def test_a_sheet_an_analysis_came_from_or_naming_coordinates_is_kept():
    assert numeric_data_spans(*_eLife(used=("s1-sheet-01", "s2-table-001"))) == []
    assert numeric_data_spans(*_eLife(header="Region\tMNI x\tMNI y\tMNI z\n")) == []


def test_nothing_is_left_out_when_an_analysis_table_of_a_supplement_is_unmapped():
    """A sheet of a file the text holds no block for may be any of the data blocks."""
    assert numeric_data_spans(*_eLife(used=("s2-table-001", "s3-sheet-01"))) == []


def test_a_table_label_in_the_list_of_tables_is_not_where_the_table_is():
    """cqj5aoh2yonc, a thesis: all 30 anchors were its list of tables' entries."""
    text = ("Lista de tabelas\nTabela 14 – Regiões ESI e BOLD ........ 112\n\n"
            "Capítulo 5\n\nTabela 14 – Regiões ESI e BOLD\n"
            "1 E | Pólo temporal superior E | -39,-6,-10")
    entries = [{"key": "a", "table_id": "t14", "points": [{"row": 0}]}]
    tables = [{"table_id": "t14", "label": "Tabela 14 – Regiões ESI e BOLD"}]

    ((at, _),) = anchors_of(text, entries, tables)

    assert at == text.index("Capítulo 5") + len("Capítulo 5\n\n")


def test_a_table_without_span_or_label_is_found_by_a_row_its_points_came_from():
    text = "Methods.\n\nRegion\tx\ty\tz\nLeft insula\t-38\t12\t4\nRight amygdala\t24\t-4\t-18"
    table = {"rows": [{"row": 0, "cells": ["Region", "x", "y", "z"]},
                      {"row": 2, "cells": ["Right amygdala", "24", "-4", "-18"]}]}

    assert _row_in_text(text, table, {"points": [{"row": 2}]}) == (
        text.index("Right amygdala"), text.index("Right amygdala") + len("Right amygdala"))
    assert _row_in_text(text, table, {"points": [{"row": 5}]}) is None


def test_outside_and_carve_leave_the_kept_ranges_and_carved_paragraphs_in():
    assert outside([[10, 20], [30, 40]], 50) == [(0, 10, "the text"), (20, 30, "the text"),
                                               (40, 50, "the text")]
    assert carve([(0, 100, "x")], [(10, 20), (50, 60)]) == [(0, 10, "x"), (20, 50, "x"),
                                                             (60, 100, "x")]


def test_anchors_of_places_a_table_with_no_span_or_label_by_its_rows():
    """A thesis's tables, sheets of a supplement: no span, no label printed."""
    text = "Methods.\n\nRegion\tx\ty\tz\nLeft insula\t-38\t12\t4\nRight amygdala\t24\t-4\t-18"
    tables = [{"table_id": "t1", "label": None, "caption": None,
               "rows": [{"row": 2, "cells": ["Right amygdala", "24", "-4", "-18"]}]}]
    entries = [{"key": "a", "table_id": "t1", "points": [{"row": 2}]}]

    assert anchors_of(text, entries, tables) == [
        (text.index("Right amygdala"), text.index("Right amygdala") + len("Right amygdala"))]
