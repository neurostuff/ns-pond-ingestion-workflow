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
