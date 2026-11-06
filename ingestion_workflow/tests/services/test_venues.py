"""The venue each paper appeared in (`services.venues`), from recorded answers.

The papers and journal names are production's; OpenAlex, Semantic Scholar, Crossref and
PubMed are recorded, and NLM's journal list is the records of it that the names reach.
"""

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Notes
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.services import venues
from ingestion_workflow.services.metadata import (
    J_MEDLINE_CACHE,
    OPENALEX_SOURCES_CACHE_DIR,
    MetadataService,
)
from ingestion_workflow.tests import cassette_secrets
from study_schema.models import paper_parse as pp

NOW = datetime(2026, 11, 5, tzinfo=timezone.utc)
#: The records of NLM's J_Medline.txt (2026-11-05) that the names below reach, verbatim.
J_MEDLINE = Path(__file__).parents[1] / "data" / "venues" / "J_Medline.txt"


def _settings(tmp_path, **given):
    """Semantic Scholar's key is dropped from the cassettes, so any value replays; OpenAlex
    is asked with the placeholder address and no key."""
    return Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond",
        **{"pubmed_email": None, "openalex_api_key": None,
           "OPENALEX_EMAIL": cassette_secrets.REPLAY_EMAIL,
           "semantic_scholar_api_key": os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"),
           **given})


def _with_journal_list(settings):
    """NLM's journal list in the cache, as the service keeps it."""
    target = settings.get_cache_dir("metadata") / J_MEDLINE_CACHE
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(J_MEDLINE, target)


def _decided(settings, identifier, dois=None):
    """The service's record of the paper, and its notes."""
    service = MetadataService(settings)
    content = ExtractedContent(slug=identifier.slug, source=DownloadSource.PDF,
                               identifier=identifier)
    found = service.enrich_metadata(
        [content], {}, {identifier.slug: dois or [d for d in [identifier.doi] if d]}, NOW)
    return found[identifier.slug], service.notes[identifier.slug], service


@pytest.mark.vcr()
def test_a_journal_location_is_the_venue_when_a_repository_is_the_primary_one(tmp_path):
    """OpenAlex holds four works with Movement Disorders' DOI 10.1002/mds.26295; the one its
    batch answer gives last, which the client keeps, has Radboud Repository as its primary
    location and the journal as its second."""
    identifier = Identifier(pmid="26095114", doi="10.1002/mds.26295")
    meta, notes, _ = _decided(_settings(tmp_path), identifier)

    work = meta.raw_metadata["openalex"][0]
    assert work["primary_location"]["source"]["display_name"] == \
        "Radboud Repository (Radboud University)"
    assert meta.venue == pp.Venue(openalex_id="S163027424", match=pp.VenueMatch.doi)
    assert venues.FIELD not in notes.errors
    # the venue survives the artifact
    assert ArticleMetadata.from_dict(meta.to_dict()).venue == meta.venue


@pytest.mark.vcr()
def test_a_paper_known_by_its_pmid_alone_takes_the_venue_of_its_pmid_s_work(tmp_path):
    """PMID 15814932, an AJNR paper without a DOI: OpenAlex's work for it has PubMed Central
    as its primary location and the journal after it."""
    meta, _, _ = _decided(_settings(tmp_path), Identifier(pmid="15814932"))

    assert meta.raw_metadata["openalex"][0]["primary_location"]["source"]["type"] == "repository"
    assert meta.venue == pp.Venue(openalex_id="S101278519", match=pp.VenueMatch.pmid)


@pytest.mark.vcr()
def test_a_work_listing_only_repository_copies_leaves_the_venue_to_the_issn(tmp_path):
    """PMID 15090336, an AJNR paper: OpenAlex's work for it lists only PubMed Central and
    PubMed, which hold copies of it; PubMed's ISSN names the journal."""
    settings = _settings(tmp_path, pubmed_email=cassette_secrets.REPLAY_EMAIL)
    _with_journal_list(settings)
    meta, _, _ = _decided(settings, Identifier(pmid="15090336"))

    work = meta.raw_metadata["openalex"][0]
    assert {(loc.get("source") or {}).get("type") for loc in work["locations"]} <= {
        "repository", None}
    assert meta.venue == pp.Venue(openalex_id="S101278519", match=pp.VenueMatch.issn)


def _licence_settings(tmp_path):
    """The settings the licence tests recorded their cassettes with."""
    return Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                    ns_pond_root=tmp_path / "ns-pond", pubmed_email=None,
                    semantic_scholar_api_key="replay", openalex_api_key="replay")


@pytest.mark.vcr("TestLicences.test_a_preprint_s_licence_is_the_one_its_server_deposits.yaml")
def test_a_paper_known_only_by_a_preprint_takes_its_server_s_source(tmp_path):
    """10.1101/2025.05.13.653896, known only by its bioRxiv DOI."""
    meta, _, _ = _decided(_licence_settings(tmp_path),
                          Identifier(doi="10.1101/2025.05.13.653896"),
                          ["10.1101/2025.05.13.653896"])

    assert meta.venue == pp.Venue(openalex_id="S4306402567",
                                  match=pp.VenueMatch.preprint_server)


@pytest.mark.vcr("test_a_paper_with_a_preprint_takes_its_journal_s_licence.yaml")
def test_a_paper_with_a_preprint_takes_its_journal_not_its_server(tmp_path):
    """dpl6phhniqbj holds 10.1002/hbm.25137 and its bioRxiv preprint: Human Brain Mapping."""
    identifier = Identifier(doi="10.1002/hbm.25137",
                            preprint_dois=["10.1101/2020.03.25.008318"])
    meta, _, _ = _decided(_licence_settings(tmp_path), identifier,
                          ["10.1002/hbm.25137", "10.1101/2020.03.25.008318"])

    assert meta.venue == pp.Venue(openalex_id="S121666818", match=pp.VenueMatch.doi)


@pytest.mark.vcr()
def test_an_issn_pubmed_gives_names_the_source_openalex_has_no_work_for(tmp_path):
    """PMID 5107855 (Arzneimittel-Forschung, 1971): OpenAlex has no work for it, and PubMed
    gives the journal's ISSN, 0004-4172. The journal's name is in NLM's list too, which only
    the ISSN comes before. The source looked up is cached, and read."""
    settings = _settings(tmp_path, pubmed_email=cassette_secrets.REPLAY_EMAIL)
    _with_journal_list(settings)
    meta, _, service = _decided(settings, Identifier(pmid="5107855"))

    assert not meta.raw_metadata.get("openalex")
    assert meta.venue == pp.Venue(openalex_id="S132758211", match=pp.VenueMatch.issn)
    cached = settings.get_cache_dir("metadata") / OPENALEX_SOURCES_CACHE_DIR / "0004-4172.json"
    assert cached in service.records_read[Identifier(pmid="5107855").slug]
    assert "https://openalex.org/S132758211" in cached.read_text()


# -- a journal's name, against NLM's journal list -------------------------------------------


@pytest.fixture
def by_name():
    """`venues.decide` for a paper known by nothing but `journal`, its sources looked up in
    OpenAlex (recorded), and its notes."""
    client = OpenAlexClient(cassette_secrets.REPLAY_EMAIL)
    index = venues.JournalIndex.parse(J_MEDLINE.read_text(encoding="utf-8"))

    def decide(journal):
        notes = Notes()
        venue = venues.decide(pp.ArticleIdentifiers(publication_status="published"),
                              Identifier(), None, None, None, journal, client.source_by_issn,
                              lambda: index, notes)
        return venue, notes.errors.get(venues.FIELD)

    return decide


@pytest.mark.parametrize("journal, key", [
    ("Cerebral cortex (New York, N.Y. : 1991)", "cerebral cortex"),
    ("The Journal of neuroscience : the official journal of the Society for Neuroscience",
     "journal of neuroscience"),
    ("Suicide & life-threatening behavior", "suicide and life threatening behavior"),
    ("Medical Biol. Eng. Comput.", "medical biol eng comput"),
    ("Zhonghua yi xue za zhi = Chinese medical journal", "zhonghua yi xue za zhi"),
    ("Revista Médica del Hospital General de México",
     "revista medica del hospital general de mexico"),
    ("Clinical neurophysiology ; official journal", "clinical neurophysiology"),
])
def test_a_name_is_compared_folded(journal, key):
    assert venues.normalized(journal) == key


@pytest.mark.vcr()
def test_a_name_in_nlm_s_list_is_its_titles_source(by_name):
    assert by_name("Cerebral cortex (New York, N.Y. : 1991)") == (
        pp.Venue(openalex_id="S117898428", match=pp.VenueMatch.title), None)


@pytest.mark.vcr()
def test_a_name_three_titles_share_and_none_dominates_is_noted_not_decided(by_name):
    """Three journals NLM lists are "Psychiatry" once their qualifiers are set aside; OpenAlex
    has 3,912 works in the first, 790 in the second and none in the third."""
    venue, noted = by_name("Psychiatry")
    assert venue is None
    assert noted == ("journal 'Psychiatry' names several sources: S959787170 (Psychiatry), "
                     "S4210195357 (Psychiatry), S2754778914 (Psychiatry): for review")


@pytest.mark.vcr()
def test_a_name_close_to_one_title_is_that_title_s_source_with_its_score(by_name):
    assert by_name("Suicide and Life-Threatening Behaviour") == (
        pp.Venue(openalex_id="S50059124", match=pp.VenueMatch.fuzzy, score=98.7), None)


@pytest.mark.vcr()
def test_of_two_sources_a_name_reaches_one_with_ten_times_the_works_is_taken(by_name):
    """"Life Science" is as close to "Life sciences" (37,883 works) as to "Life sciences
    (1962)" (none)."""
    assert by_name("Life Science") == (
        pp.Venue(openalex_id="S175775512", match=pp.VenueMatch.fuzzy, score=96.0), None)


def test_a_name_only_somewhat_like_a_title_is_noted_for_review(by_name):
    """It is nearest The Journal of dairy research: no venue, and nothing is looked up."""
    assert by_name("Journal of dentistry research") == (
        None, "journal 'Journal of dentistry research' is 88.9 like 'The Journal of dairy "
              "research' (NLM 2985125R): for review")


def test_a_paper_with_neither_ids_nor_a_name_has_no_venue(by_name):
    assert by_name(None) == (None, None)


@pytest.mark.vcr()
def test_the_sources_of_many_issns_are_one_filter_and_match_each_asked_alone():
    """Movement Disorders' print ISSN, NeuroImage's and one no source holds, in one
    `issn:` filter; each answer is what the single lookup gives."""
    client = OpenAlexClient(cassette_secrets.REPLAY_EMAIL)
    found = client.sources_by_issn(["0885-3185", "1053-8119", "0000-0000"])

    assert found["0885-3185"]["id"] == "https://openalex.org/S163027424"
    assert found["0000-0000"] is None
    for issn in ("0885-3185", "1053-8119"):
        assert found[issn] == client.source_by_issn(issn)
