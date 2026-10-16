"""ScienceDirect search, planned from counts and paged to the end."""

import os

import ingestion_workflow.services.search as search
import pytest
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient, identifier_of, key_of
from ingestion_workflow.config import Settings
from ingestion_workflow.services.query_dialect import QueryError, Years, parse
from ingestion_workflow.services.search import ScienceDirectSearchService

# every request is the same PUT; the body (query, offset, page size) tells them apart
BY_BODY = ["method", "scheme", "host", "port", "path", "query", "body"]


@pytest.fixture()
def settings():
    # the key header is dropped from the cassettes; any value replays
    return Settings(elsevier_api_key=os.environ.get("ELSEVIER_API_KEY", "replay"))


def _search(settings, text, years=None):
    service = ScienceDirectSearchService(
        parse(text, Years.parse(years) if years else None), settings
    )
    service.plan()
    return service, list(service.search().identifiers)


@pytest.mark.vcr(match_on=BY_BODY)
def test_one_page_holds_everything_found(settings):
    service, found = _search(settings, "fMRI AND misophonia", "2026")

    assert service.plan_report == ['{"qs": "fMRI AND misophonia", "date": "2026"}: 17 found']
    assert len(found) == 17
    assert all(i.doi == i.doi.lower() for i in found if i.doi)
    assert all(i.other_ids["pii"] for i in found)


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_one_past_a_page_is_paged_to_its_last_record(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 25)
    service, found = _search(settings, "fMRI AND hyperacusis", "2022")

    assert service.plan_report == ['{"qs": "fMRI AND hyperacusis", "date": "2022"}: 26 found']
    assert len({i.doi for i in found}) == 26  # offsets 0 and 25


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_of_whole_pages_asks_for_no_page_past_it(settings, monkeypatch):
    # the cassette holds the one page of 10; asking for offset 10 would not match
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    service, found = _search(settings, "fMRI AND hyperacusis", "2020")

    assert len(found) == 10


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_search_past_the_last_offset_is_split_by_year_then_by_or_clause(settings, monkeypatch):
    # 54 found in all; the API is taken to serve 30, so it is split
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    q = "(fMRI AND misophonia) OR (fMRI AND hyperacusis)"
    service, found = _search(settings, q, "2025:2026")

    assert service.plan_report == [
        f'{{"qs": "{q}", "date": "2025-2026"}}: 54 found; split by year: 21 + 33',
        f'  {{"qs": "{q}", "date": "2025"}}: 21 found',
        f'  {{"qs": "{q}", "date": "2026"}}: 33 found; split into halves of its OR clauses',
        '    {"qs": "fMRI AND misophonia", "date": "2026"}: 17 found',
        '    {"qs": "fMRI AND hyperacusis", "date": "2026"}: 23 found',
    ]
    # the two OR halves overlap; each article is one record, and all 54 are there
    assert len(found) == len({i.doi for i in found}) == 54


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_at_the_limit_is_not_split(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 17)
    service, found = _search(settings, "fMRI AND misophonia", "2026")

    assert len(service.plan_report) == 1 and len(found) == 17


@pytest.mark.vcr(match_on=BY_BODY)
def test_one_year_over_the_limit_is_split_on_a_word(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 16)
    service, found = _search(settings, "fMRI AND misophonia", "2026")

    # memory, the first word, puts 11 of 17 (65%) on one side, so it is taken
    assert service.plan_report == [
        '{"qs": "fMRI AND misophonia", "date": "2026"}: 17 found; split on \'memory\': 11 + 6',
        '  {"qs": "fMRI AND misophonia AND memory", "date": "2026"}: 11 found',
        '  {"qs": "fMRI AND misophonia NOT memory", "date": "2026"}: 6 found',
    ]
    assert len({i.doi for i in found}) == 17


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_word_that_would_pass_250_characters_is_not_split_on(settings, monkeypatch):
    # no OR to halve and one year; the shortest word, " AND mice", makes it 251 characters
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 10)
    text = (
        '"functional magnetic resonance imaging" NOT "blood oxygen level dependent" NOT '
        '"resting state functional connectivity" NOT "voxel based morphometry" NOT '
        '"default mode network" NOT "anterior cingulate cortex" NOT "posterior cingulate cortices"'
    )
    assert len(text) == 242
    service = ScienceDirectSearchService(parse(text, Years.parse("2026")), settings)
    with pytest.raises(QueryError, match="within 250 characters"):
        service.plan()


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_query_no_split_applies_to_is_refused(settings, monkeypatch):
    # eight ANDs leave no room for a word, and it has no year range or OR clauses
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 1)
    text = (
        "fMRI AND brain AND cortex AND memory AND patients AND human AND data AND analysis "
        "AND model"
    )
    service = ScienceDirectSearchService(parse(text, Years.parse("2026")), settings)
    with pytest.raises(QueryError, match="none of its splits applies"):
        service.plan()


def test_a_query_past_the_connector_limit_that_is_no_or_is_refused(settings):
    text = " AND ".join(["fMRI"] + [f"w{n}" for n in range(9)])
    service = ScienceDirectSearchService(parse(text), settings)
    with pytest.raises(QueryError, match="honours at most 8 AND, OR and NOT"):
        service.plan()


def test_a_result_is_named_by_its_doi_or_else_its_pii():
    assert identifier_of({"doi": "10.1016/J.X", "pii": "S1"}).doi == "10.1016/j.x"
    no_doi = identifier_of({"pii": "S0001"})
    assert no_doi.doi is None and no_doi.other_ids == {"pii": "S0001"}
    neither = identifier_of({"title": "Preface"})
    assert neither.doi is None and neither.other_ids is None


def test_sciencedirect_needs_its_key():
    with pytest.raises(ValueError, match="elsevier_api_key"):
        ScienceDirectSearchService(parse("fMRI"), Settings.model_construct(elsevier_api_key=None))


class _Counts:
    """ScienceDirect's answers by query, standing in for the API where it was never
    seen to disagree with itself: the parts of a split always added up when asked.
    """

    def __init__(self, table):
        self.table = table  # (qs, date) to a count, or a function of them
        self.asked = []

    def __call__(self, body):
        key = body.get("qs"), body.get("date")
        self.asked.append(key)
        found = self.table(*key) if callable(self.table) else self.table[key]
        return {"resultsFound": found, "results": []}


@pytest.fixture()
def counted(monkeypatch, settings):
    """Plan `text` in `years` against a table of counts, the API taken to serve 30."""

    def plan(text, years, table, pivots=("memory", "disease", "treatment")):
        counts = _Counts(table)
        monkeypatch.setattr(ScienceDirectClient, "_request", lambda client, body: counts(body))
        monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
        monkeypatch.setattr(search, "SCIENCEDIRECT_PIVOTS", pivots)
        monkeypatch.setattr(search, "_this_year", lambda: 2026)
        service = ScienceDirectSearchService(
            parse(text, Years.parse(years)) if years else parse(text), settings
        )
        service.plan()
        return service.plan_report, counts.asked

    return plan


def test_a_word_whose_counts_do_not_add_up_is_passed_for_the_next(counted):
    report, _ = counted(
        "fMRI",
        "2026",
        {
            ("fMRI", "2026"): 40,
            ("fMRI AND memory", "2026"): 20,  # balanced, but 20 + 15 is not 40
            ("fMRI NOT memory", "2026"): 15,
            ("fMRI AND disease", "2026"): 25,
            ("fMRI NOT disease", "2026"): 15,
        },
    )
    assert report[0].endswith("40 found; split on 'disease': 25 + 15")


def test_a_word_in_every_hit_or_in_none_is_passed_over(counted):
    report, asked = counted(
        "fMRI",
        "2026",
        {
            ("fMRI", "2026"): 40,
            ("fMRI AND memory", "2026"): 40,
            ("fMRI AND disease", "2026"): 0,
            ("fMRI AND treatment", "2026"): 18,
            ("fMRI NOT treatment", "2026"): 22,
        },
    )
    assert report[0].endswith("split on 'treatment': 18 + 22")
    assert ("fMRI NOT memory", "2026") not in asked


def test_the_first_balanced_word_is_taken_and_its_count_reused(counted):
    report, asked = counted(
        "fMRI",
        "2026",
        {
            ("fMRI", "2026"): 40,
            ("fMRI AND memory", "2026"): 20,
            ("fMRI NOT memory", "2026"): 20,
        },
    )
    assert report[0].endswith("split on 'memory': 20 + 20")
    # the parts are not counted again, and no later word is counted at all
    assert asked == [("fMRI", "2026"), ("fMRI AND memory", "2026"), ("fMRI NOT memory", "2026")]


def test_a_word_of_the_query_is_not_split_on(counted):
    report, asked = counted(
        "fMRI AND memory",
        "2026",
        {
            ("fMRI AND memory", "2026"): 40,
            ("fMRI AND memory AND disease", "2026"): 25,
            ("fMRI AND memory NOT disease", "2026"): 15,
        },
    )
    assert ("fMRI AND memory AND memory", "2026") not in asked
    assert "split on 'disease'" in report[0]


def test_an_or_is_halved_before_any_word_is_tried(counted):
    report, asked = counted(
        "fMRI OR PET",
        "2026",
        {("fMRI OR PET", "2026"): 40, ("fMRI", "2026"): 25, ("PET", "2026"): 20},
    )
    assert report == [
        '{"qs": "fMRI OR PET", "date": "2026"}: 40 found; split into halves of its OR clauses',
        '  {"qs": "fMRI", "date": "2026"}: 25 found',
        '  {"qs": "PET", "date": "2026"}: 20 found',
    ]
    assert all("memory" not in qs for qs, _ in asked)


def test_without_years_a_search_is_split_from_1990_to_its_latest_year(counted):
    report, _ = counted(
        "fMRI",
        None,
        {
            ("fMRI", "1990-9999"): 40,
            ("fMRI", "2027-9999"): 0,  # nothing dated after this year
            ("fMRI", "1990-2008"): 15,
            ("fMRI", "2009-2026"): 25,
        },
    )
    assert report == [
        '{"qs": "fMRI", "date": "1990-9999"}: 40 found; split by year: 15 + 25',
        '  {"qs": "fMRI", "date": "1990-2008"}: 15 found',
        '  {"qs": "fMRI", "date": "2009-2026"}: 25 found',
    ]


def test_hits_dated_years_ahead_move_the_latest_year(counted):
    def count(qs, date):
        start, end = (int(y) for y in date.split("-"))
        if end == 9999:
            return 40 if start == 1990 else (1 if start <= 2031 else 0)
        return 20  # 1990-2010 and 2011-2031

    report, _ = counted("fMRI", None, count)
    assert report[0].endswith("split by year: 20 + 20")
    assert report[2] == '  {"qs": "fMRI", "date": "2011-2031"}: 20 found'


def test_an_issue_dated_across_two_years_is_in_both_halves(counted):
    report, _ = counted(
        "fMRI",
        "1984:1985",
        {("fMRI", "1984-1985"): 40, ("fMRI", "1984"): 22, ("fMRI", "1985"): 21},
    )
    assert report[0].endswith("split by year: 22 + 21 (3 in both)")


def test_years_whose_counts_do_not_add_up_are_refused(counted):
    with pytest.raises(
        QueryError, match="only 20 \\+ 15 in 2025 and 2026; the split would lose the rest"
    ):
        counted(
            "fMRI",
            "2025:2026",
            {("fMRI", "2025-2026"): 40, ("fMRI", "2025"): 20, ("fMRI", "2026"): 15},
        )


def test_a_result_without_a_doi_or_pii_is_told_apart_by_title_and_date():
    assert key_of({"doi": "10.1016/J.X", "pii": "S1"}) == "10.1016/j.x"
    assert key_of({"pii": "S1"}) == "S1"
    assert key_of({"title": "Preface", "publicationDate": "2026-01-31"}) == "Preface|2026-01-31"
