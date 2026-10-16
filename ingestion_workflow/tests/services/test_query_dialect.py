"""One query, parsed once and rendered in each provider's dialect."""

import pytest
from ingestion_workflow.services.query_dialect import (
    And,
    Field,
    Not,
    Or,
    Phrase,
    QueryError,
    Term,
    Years,
    parse,
    to_openalex,
    to_pubmed,
    to_sciencedirect,
    to_semantic_scholar,
)

FMRI = (
    'fMRI OR "functional MRI" OR "functional magnetic resonance" OR "BOLD signal" '
    'OR "blood oxygen level dependent"'
)


@pytest.fixture()
def fmri_2026():
    return parse(FMRI, Years.parse("2026"))


def test_the_measured_fmri_query_in_pubmed(fmri_2026):
    assert to_pubmed(fmri_2026) == (
        '("fMRI"[tiab] OR "functional MRI"[tiab] OR "functional magnetic resonance"[tiab] '
        'OR "BOLD signal"[tiab] OR "blood oxygen level dependent"[tiab]) '
        'AND "journal article"[pt] AND 2026[dp]'
    )


def test_the_measured_fmri_query_in_semantic_scholar(fmri_2026):
    assert to_semantic_scholar(fmri_2026) == {
        "query": 'fMRI | "functional MRI" | "functional magnetic resonance" | "BOLD signal" '
        '| "blood oxygen level dependent"',
        "year": "2026",
    }


def test_the_measured_fmri_query_in_openalex(fmri_2026):
    assert to_openalex(fmri_2026) == (
        'title_and_abstract.search:(fMRI OR "functional MRI" OR "functional magnetic resonance" '
        'OR "BOLD signal" OR "blood oxygen level dependent"),type:article,publication_year:2026'
    )


def test_and_binds_tighter_than_or_and_not_excludes():
    query = parse('(fMRI OR PET) AND memory NOT (rats OR mice) OR "working memory"')
    assert query.expr == Or(
        (
            And(
                (
                    Or((Term("fMRI"), Term("PET"))),
                    Term("memory"),
                    Not(Or((Term("rats"), Term("mice")))),
                )
            ),
            Phrase("working memory"),
        )
    )


def test_nested_groups_render_parenthesized_in_every_dialect():
    query = parse("(fMRI OR PET) AND memory NOT (rats OR mice)", Years.parse("2025:2026"))
    # PubMed reads left to right, so the groups carry the precedence
    assert to_pubmed(query) == (
        '(("fMRI"[tiab] OR "PET"[tiab]) AND "memory"[tiab] NOT ("rats"[tiab] OR "mice"[tiab])) '
        'AND "journal article"[pt] AND 2025:2026[dp]'
    )
    assert to_semantic_scholar(query) == {
        "query": "(fMRI | PET) + memory + -(rats | mice)",
        "year": "2025-2026",
    }
    assert to_openalex(query) == (
        "title_and_abstract.search:((fMRI OR PET) AND memory NOT (rats OR mice)),"
        "type:article,publication_year:2025-2026"
    )


def test_without_years_a_query_searches_1990_onward_in_every_dialect():
    query = parse("fMRI")
    assert str(query.years) == "1990 onward"
    assert to_pubmed(query) == '"fMRI"[tiab] AND "journal article"[pt] AND 1990:3000[dp]'
    assert to_semantic_scholar(query) == {"query": "fMRI", "year": "1990-"}
    assert to_openalex(query) == (
        "title_and_abstract.search:(fMRI),type:article,publication_year:>1989"
    )
    assert to_sciencedirect(query) == {"qs": "fMRI", "date": "1990-9999"}


def test_an_explicit_year_overrides_the_default():
    assert to_semantic_scholar(parse("fMRI", Years.parse("1980:1989")))["year"] == "1980-1989"


def test_a_hyphenated_term_is_quoted_where_a_hyphen_is_an_operator():
    query = parse("resting-state AND fMRI")
    assert to_semantic_scholar(query)["query"] == '"resting-state" + fMRI'
    assert (
        to_openalex(query) == 'title_and_abstract.search:("resting-state" AND fMRI),type:article,'
        "publication_year:>1989"
    )


@pytest.mark.parametrize(
    "prefix, field, pubmed_tag, openalex_filter",
    [
        ("title:", Field.TITLE, "ti", "title.search"),
        ("abstract:", Field.ABSTRACT, "ab", "abstract.search"),
    ],
)
def test_a_leading_field_limits_where_the_query_matches(
    prefix, field, pubmed_tag, openalex_filter
):
    query = parse(f"{prefix} fMRI OR PET")
    assert query.field is field
    assert (
        to_pubmed(query)
        == f'("fMRI"[{pubmed_tag}] OR "PET"[{pubmed_tag}]) AND "journal article"[pt] '
        "AND 1990:3000[dp]"
    )
    assert to_openalex(query) == (
        f"{openalex_filter}:(fMRI OR PET),type:article,publication_year:>1989"
    )
    with pytest.raises(QueryError, match="Semantic Scholar"):
        to_semantic_scholar(query)


def test_a_comma_cannot_be_said_to_openalex():
    query = parse('"memory, working" OR fMRI')
    assert to_pubmed(query).startswith('("memory, working"[tiab] OR')
    with pytest.raises(QueryError, match="OpenAlex"):
        to_openalex(query)


@pytest.mark.parametrize(
    "text, message",
    [
        ("fmri pet", "put AND or OR before 'pet'"),
        ("fmri or pet", "operators are upper case"),
        ("NOT rats", "NOT excludes from what comes before it"),
        ("fMRI AND", "ends where a term was expected"),
        ("OR fMRI", "needs a term before it"),
        ("(fMRI OR PET", "never closed"),
        ("fMRI)", "missing"),
        ('"functional MRI', "no closing quote"),
        ('fMRI OR ""', "empty phrase"),
        ("fMRI[tiab]", "not part of the search syntax"),
        ("fMRI | PET", "not part of the search syntax"),
        ("fMRI*", "not part of the search syntax"),
        ("fMRI AND title:memory", "not part of the search syntax"),
        ("fMRI AND -rats", "write NOT 'rats'"),
        ('"fMRI[tiab]"', "not part of the search syntax"),
        ('"fMRI | PET"', "not part of the search syntax"),
        ("fMRI+PET", "not part of the search syntax"),
        ("   ", "empty"),
    ],
)
def test_what_the_syntax_does_not_say_is_refused(text, message):
    with pytest.raises(QueryError, match=message):
        parse(text)


@pytest.mark.parametrize(
    "text, years", [("2026", Years(2026, 2026)), ("2025:2026", Years(2025, 2026))]
)
def test_years(text, years):
    assert Years.parse(text) == years


@pytest.mark.parametrize("text", ["26", "2026-2027", "2026:2025", "recent"])
def test_bad_years_are_refused(text):
    with pytest.raises(QueryError):
        Years.parse(text)


def test_the_measured_fmri_query_in_sciencedirect(fmri_2026):
    assert to_sciencedirect(fmri_2026) == {
        "qs": 'fMRI OR "functional MRI" OR "functional magnetic resonance" OR "BOLD signal" '
        'OR "blood oxygen level dependent"',
        "date": "2026",
    }


def test_sciencedirect_searches_the_title_or_the_full_text():
    assert to_sciencedirect(parse("title: fMRI NOT rats", Years.parse("2025:2026"))) == {
        "title": "fMRI NOT rats",
        "date": "2025-2026",
    }
    with pytest.raises(QueryError, match="ScienceDirect"):
        to_sciencedirect(parse("abstract: fMRI"))


def test_sciencedirect_refuses_a_query_over_its_length_limit():
    long = " OR ".join(f'"functional magnetic resonance {n}"' for n in range(8))
    with pytest.raises(QueryError, match="ScienceDirect accepts at most 250"):
        to_sciencedirect(parse(long))


def test_a_phrase_s_spacing_is_collapsed():
    assert parse('"functional   MRI"').expr == Phrase("functional MRI")


def test_sciencedirect_takes_exactly_250_characters():
    assert len(to_sciencedirect(parse("a" * 250))["qs"]) == 250
    with pytest.raises(QueryError, match="this one is 251"):
        to_sciencedirect(parse("a" * 251))
