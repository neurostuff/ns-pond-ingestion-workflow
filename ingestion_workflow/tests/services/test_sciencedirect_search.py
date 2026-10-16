"""ScienceDirect search, planned from counts and paged to the end."""

import os

import ingestion_workflow.services.search as search
import pytest
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient, identifier_of
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
def test_a_search_past_the_last_offset_is_split_by_year_then_on_a_word(settings, monkeypatch):
    # 54 found in all; the API is taken to serve 30, so it is split
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    service, found = _search(
        settings, "(fMRI AND misophonia) OR (fMRI AND hyperacusis)", "2025:2026"
    )

    q = "(fMRI AND misophonia) OR (fMRI AND hyperacusis)"
    assert service.plan_report == [
        f'{{"qs": "{q}", "date": "2025-2026"}}: 54 found; split by year: 21 + 33 = 54',
        f'  {{"qs": "{q}", "date": "2025"}}: 21 found',
        f'  {{"qs": "{q}", "date": "2026"}}: 33 found; split on \'disease\': 17 + 16 = 33',
        f'    {{"qs": "({q}) AND disease", "date": "2026"}}: 17 found',
        f'    {{"qs": "({q}) NOT disease", "date": "2026"}}: 16 found',
    ]
    # every part paged to its end (21 + 17 + 16 records), and the parts are disjoint
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

    assert service.plan_report == [
        '{"qs": "fMRI AND misophonia", "date": "2026"}: 17 found; '
        "split on 'patients': 9 + 8 = 17",
        '  {"qs": "fMRI AND misophonia AND patients", "date": "2026"}: 9 found',
        '  {"qs": "fMRI AND misophonia NOT patients", "date": "2026"}: 8 found',
    ]
    assert len({i.doi for i in found}) == 17


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_word_the_api_does_not_honour_is_passed_over(settings, monkeypatch):
    # past its connector limit the API ignores the added word: with and without
    # it count the same as the query, so no word splits it and its OR halves do
    monkeypatch.setattr(search, "SCIENCEDIRECT_MAX_CONNECTORS", 9)
    monkeypatch.setattr(search, "SCIENCEDIRECT_PIVOTS", ("memory", "disease"))
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 10602)  # one fewer than found
    text = (
        "misophonia OR hyperacusis OR phonophobia OR synesthesia OR tinnitus OR stuttering "
        "OR dyslexia OR insomnia OR narcolepsy"
    )
    service = ScienceDirectSearchService(parse(text, Years.parse("2026")), settings)
    service.plan()

    # the cassette holds both words' counts: each the query's own 10,603
    assert service.plan_report == [
        f'{{"qs": "{text}", "date": "2026"}}: 10,603 found; '
        "split into halves of its OR clauses (they overlap; their union is exact)",
        '  {"qs": "misophonia OR hyperacusis OR phonophobia OR synesthesia OR tinnitus", '
        '"date": "2026"}: 1,722 found',
        '  {"qs": "stuttering OR dyslexia OR insomnia OR narcolepsy", "date": "2026"}: '
        "9,145 found",
    ]


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_split_that_would_pass_250_characters_is_refused(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 10)
    text = (
        '"functional magnetic resonance imaging" OR "blood oxygen level dependent" OR '
        '"resting state functional connectivity" OR "voxel based morphometry" OR '
        '"default mode network" OR "anterior cingulate cortex" OR '
        '"posterior cingulate cortex" OR "insular"'
    )
    # 247 characters and 7 connectors: an added word would make it 260
    assert len(text) == 247
    service = ScienceDirectSearchService(parse(text, Years.parse("2026")), settings)
    with pytest.raises(QueryError, match="ScienceDirect accepts at most 250"):
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
        self.table = table
        self.asked = []

    def __call__(self, body):
        key = body.get("qs"), body.get("date")
        self.asked.append(key)
        return {"resultsFound": self.table[key], "results": []}


def test_a_word_whose_counts_do_not_add_up_is_passed_for_the_next(settings, monkeypatch):
    counts = _Counts(
        {
            ("fMRI", "2026"): 40,
            ("fMRI AND memory", "2026"): 20,  # nearest to half, but 20 + 15 is not 40
            ("fMRI NOT memory", "2026"): 15,
            ("fMRI AND disease", "2026"): 25,
            ("fMRI NOT disease", "2026"): 15,
        }
    )
    monkeypatch.setattr(ScienceDirectClient, "_request", lambda client, body: counts(body))
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    monkeypatch.setattr(search, "SCIENCEDIRECT_PIVOTS", ("memory", "disease"))
    service = ScienceDirectSearchService(parse("fMRI", Years.parse("2026")), settings)
    service.plan()

    assert service.plan_report[0].endswith("split on 'disease': 25 + 15 = 40")


def test_a_word_of_the_query_is_not_split_on(settings, monkeypatch):
    counts = _Counts(
        {
            ("fMRI AND memory", "2026"): 40,
            ("fMRI AND memory AND disease", "2026"): 25,
            ("fMRI AND memory NOT disease", "2026"): 15,
        }
    )
    monkeypatch.setattr(ScienceDirectClient, "_request", lambda client, body: counts(body))
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    monkeypatch.setattr(search, "SCIENCEDIRECT_PIVOTS", ("memory", "disease"))
    service = ScienceDirectSearchService(parse("fMRI AND memory", Years.parse("2026")), settings)
    service.plan()

    assert ("fMRI AND memory AND memory", "2026") not in counts.asked
    assert "split on 'disease'" in service.plan_report[0]


def test_years_whose_counts_do_not_add_up_are_refused(settings, monkeypatch):
    counts = _Counts({("fMRI", "2025-2026"): 40, ("fMRI", "2025"): 20, ("fMRI", "2026"): 15})
    monkeypatch.setattr(ScienceDirectClient, "_request", lambda client, body: counts(body))
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    service = ScienceDirectSearchService(parse("fMRI", Years.parse("2025:2026")), settings)
    with pytest.raises(QueryError, match="35 across its years"):
        service.plan()
