"""A paper's author list: its ids and affiliations, which source's list it is, and who of
its authors corresponds."""

from __future__ import annotations

import pytest
from study_schema.models.paper_parse import Affiliation, Institution

from ingestion_workflow.clients.openalex import _authors_from
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.models.metadata import ArticleMetadata, Author, CorrespondingAuthor
from ingestion_workflow.services.corresponding_authors import mark_corresponding
from ingestion_workflow.services.metadata import _decide_authors
from ingestion_workflow.utils import authors as author_ids

# ORCID's own documentation iD (a fictional researcher), so no real person is named.
ORCID = "0000-0002-1825-0097"
UT = Institution(ror="00hj54h04", name="The University of Texas at Austin", country="US")


@pytest.mark.parametrize("given, expected", [
    ("https://orcid.org/0000-0002-1825-0097", ORCID),
    ("0000000218250097", ORCID),
    ("0000-0002-1825-009x", "0000-0002-1825-009X"),
    ("not an orcid", None),
])
def test_an_orcid_is_kept_bare_and_hyphenated(given, expected):
    assert author_ids.orcid(given) == expected


def test_openalex_and_ror_ids_are_kept_bare():
    assert author_ids.openalex_author("https://openalex.org/A5054079188") == "A5054079188"
    assert author_ids.ror("https://ror.org/00hj54h04/") == "00hj54h04"
    assert author_ids.ror("https://ror.org/not-an-id") is None


@pytest.mark.parametrize("printed, kept", [
    # PubMed ends a corresponding author's affiliation with their address.
    ("Department of Psychology, University of Texas, Austin, TX, USA. "
     "Electronic address: someone@example.org.",
     "Department of Psychology, University of Texas, Austin, TX, USA"),
    ("Lab A, Institute B; e-mail: someone@example.org; Hospital C",
     "Lab A, Institute B; Hospital C"),
    ("Lab A, Institute B, U.S.A.", "Lab A, Institute B, U.S.A."),
    ("  ", None),
])
def test_an_affiliation_keeps_no_email_address(printed, kept):
    assert author_ids.affiliation_text(printed) == kept


def _authorship(name, *affiliations, institutions=(), raw=None, orcid=None):
    return {"author": {"id": "https://openalex.org/A5000000001", "display_name": name,
                       "orcid": orcid},
            "institutions": list(institutions),
            "raw_affiliation_strings": raw if raw is not None else [
                a["raw_affiliation_string"] for a in affiliations],
            "affiliations": list(affiliations)}


UT_OPENALEX = {"id": "https://openalex.org/I86519309",
               "display_name": "The University of Texas at Austin",
               "ror": "https://ror.org/00hj54h04", "country_code": "US"}


def test_openalex_ties_each_affiliation_to_the_institutions_it_names():
    (author,) = _authors_from({"authorships": [_authorship(
        "Jerry Tang",
        {"raw_affiliation_string": "UT Austin",
         "institution_ids": ["https://openalex.org/I86519309"]},
        {"raw_affiliation_string": "A lab OpenAlex has no institution for",
         "institution_ids": []},
        institutions=[UT_OPENALEX], orcid="https://orcid.org/" + ORCID)]})
    assert (author.name, author.orcid, author.openalex_id) == (
        "Jerry Tang", ORCID, "A5000000001")
    assert author.affiliations == [
        Affiliation(text="UT Austin", institutions=[UT]),
        Affiliation(text="A lab OpenAlex has no institution for")]


def test_an_openalex_text_without_its_tie_takes_the_institutions_only_when_alone():
    (one,) = _authors_from({"authorships": [{
        **_authorship("A", institutions=[UT_OPENALEX], raw=["UT Austin"]),
        "affiliations": None}]})
    (two,) = _authors_from({"authorships": [{
        **_authorship("B", institutions=[UT_OPENALEX], raw=["UT Austin", "Elsewhere"]),
        "affiliations": None}]})
    assert one.affiliations == [Affiliation(text="UT Austin", institutions=[UT])]
    assert two.affiliations == [Affiliation(text="UT Austin"), Affiliation(text="Elsewhere")]


def test_pubmed_gives_every_affiliation_and_the_orcid_without_the_address():
    authors = PubMedClient(email="x@example.org")._parse_authors({"Author": [{
        "LastName": "Lovelace", "ForeName": "Ada",
        "Identifier": {"@Source": "ORCID", "#text": "https://orcid.org/" + ORCID},
        "AffiliationInfo": [
            {"Affiliation": "Department of Mathematics, Example University."},
            {"Affiliation": "Example Hospital. Electronic address: ada@example.org."}],
    }]})
    assert [(a.name, a.orcid, [f.text for f in a.affiliations]) for a in authors] == [
        ("Ada Lovelace", ORCID,
         ["Department of Mathematics, Example University.", "Example Hospital"])]


def _said(name=None, email=None, by="jats"):
    return CorrespondingAuthor(name=name, email=email, stated_by=by)


def test_the_decided_corresponding_authors_are_marked_on_the_list():
    listed = [Author(name="Ada Lovelace"), Author(name="Charles Babbage"),
              Author(name="Mary Somerville")]
    marked, unpaired = mark_corresponding(listed, [
        _said("A. Lovelace", "ada@example.org"),
        _said(None, "somerville.m@example.org"),   # an address naming no one, spelling her
        _said(None, "lab@example.org"),            # an address belonging to no author
    ])
    assert [(a.name, a.corresponding, a.emails) for a in marked] == [
        ("Ada Lovelace", True, ["ada@example.org"]),
        ("Charles Babbage", False, []),
        ("Mary Somerville", True, ["somerville.m@example.org"])]
    assert unpaired == ["lab@example.org"]
    assert listed[0].corresponding is None  # the provider's own list is left as it was


def test_no_one_is_marked_either_way_until_a_source_says_who_corresponds():
    marked, unpaired = mark_corresponding([Author(name="Ada Lovelace")], [])
    assert ([a.corresponding for a in marked], unpaired) == ([None], [])


def _record(source, *authors):
    return ArticleMetadata(title="T", authors=list(authors), source=source)


def test_the_list_is_openalex_s_and_pubmed_fills_what_it_lacks_by_place_and_surname():
    openalex = _record("openalex", Author(name="Ada Lovelace", openalex_id="A1"),
                       Author(name="Charles Babbage", openalex_id="A2"))
    pubmed = _record("pubmed",
                     Author(name="Ada Lovelace", orcid=ORCID,
                            affiliations=[Affiliation(text="Example University")]),
                     Author(name="Someone Else", orcid="0000-0001-5109-3700"))
    metadata = _record("semantic_scholar", Author(name="A Lovelace"))

    _decide_authors(metadata, {"pubmed": pubmed}, openalex, [])

    assert metadata.authors_provider == "openalex"
    ada, charles = metadata.authors
    assert (ada.openalex_id, ada.orcid, [f.text for f in ada.affiliations]) == (
        "A1", ORCID, ["Example University"])
    assert (charles.orcid, charles.affiliations) == (None, [])  # another surname: not filled


def test_without_openalex_the_list_is_pubmed_s():
    pubmed = _record("pubmed", Author(name="Ada Lovelace", orcid=ORCID))
    metadata = _record("semantic_scholar", Author(name="A Lovelace"))
    _decide_authors(metadata, {"pubmed": pubmed}, None, [_said("Ada Lovelace")])
    assert (metadata.authors_provider, [(a.name, a.corresponding) for a in metadata.authors]) \
        == ("pubmed", [("Ada Lovelace", True)])
