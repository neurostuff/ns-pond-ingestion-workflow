import requests

from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.services import citation_markers as M
from ingestion_workflow.services import reference_lists as L

MESSAGE = {"publisher": "Elsevier BV", "reference": [
    {"key": "bib1", "DOI": "10.1523/X.2001", "doi-asserted-by": "crossref"},
    {"key": "bib2", "author": "Gusnard", "year": "2001", "article-title": "Searching for a baseline"},
    {"key": "bib3", "unstructured": "Lee B. (2004) Vision. J Vis 4:1-2."},
]}


class _OpenAlex:
    def get_cited_works(self, dois):
        return {"10.1523/x.2001": {"authors": ["Jane A. Smith", "K. Jones"], "year": 2001, "pmid": "111"}}


def test_crossref_entries_keep_their_order_and_their_identifiers():
    refs = L.from_crossref(MESSAGE)

    assert [r["id"] for r in refs] == ["cr1", "cr2", "cr3"] and [r["position"] for r in refs] == [1, 2, 3]
    assert refs[0]["doi"] == "10.1523/x.2001" and refs[0]["id_providers"] == {"doi": "crossref"}
    assert refs[1]["authors"] == ["Gusnard"] and refs[1]["year"] == 2001
    assert refs[2]["text"].startswith("Lee B.")


def test_openalex_names_an_entry_that_is_a_bare_doi():
    refs = L.from_crossref(MESSAGE)

    assert L.enrich(refs, _OpenAlex()) == 1
    assert refs[0]["authors"] == ["Smith", "Jones"] and refs[0]["pmid"] == "111"
    assert refs[0]["id_providers"]["pmid"] == "openalex"


def test_a_source_entry_without_a_doi_borrows_crossref_s():
    listed = L.from_crossref(MESSAGE)
    L.enrich(listed, _OpenAlex())
    own = [{"id": "bib1", "doi": None, "pmid": None, "authors": ["Smith"], "year": 2001},
           {"id": "x9", "doi": None, "pmid": None, "authors": ["Nobody"], "year": 1990}]

    assert L.fill_identifiers(own, listed) == 1
    assert own[0]["doi"] == "10.1523/x.2001" and own[0]["pmid"] == "111"
    assert own[0]["id_providers"] == {"doi": "crossref", "pmid": "openalex"}
    assert own[1]["doi"] is None


def test_surnames_drop_initials_whichever_way_round():
    assert L.surname("Silberstein SD", given_first=False) == "Silberstein"
    assert L.surname("Jane A. Smith", given_first=True) == "Smith"
    assert L.surname("Smith, J.", given_first=True) == "Smith"


def test_a_crossref_404_is_no_record(monkeypatch):
    client = CrossrefClient("me@example.org")

    class _R:
        status_code = 404

        def raise_for_status(self):
            raise requests.HTTPError("404")

    monkeypatch.setattr(client._session, "get", lambda *a, **k: _R())
    assert client.work("10.1/none") is None


LISTED = [
    {"id": "cr1", "position": 1, "label": None, "text": "", "authors": ["Gusnard"], "year": 2001},
    {"id": "cr2", "position": 2, "label": None, "text": "", "authors": ["Smith"], "year": 2003},
    {"id": "cr3", "position": 3, "label": None, "text": "", "authors": ["Lee"], "year": 2004},
]


def test_author_year_markers_match_a_list_that_names_only_first_authors():
    text = "The default network is active at rest (Gusnard and Raichle, 2001; Smith et al., 2003)."

    found, style = M.find(text, LISTED)

    assert style == "author_year"
    assert [(c["text_span"]["text"], c["references"], c["method"]) for c in found] == [
        ("Gusnard and Raichle, 2001", ["cr1"], "author_year"), ("Smith et al., 2003", ["cr2"], "author_year")]
    assert found[0]["sentence"] == {"start_char": 0, "end_char": len(text)}
    assert found[0]["confidence"] == M.CONFIDENCE["author_year"]


def test_numbered_markers_index_the_list_and_statistics_are_not_markers():
    text = ("Attention filters perception [1, 3]. Effects were large (F (1, 22) = 4.2).\n"
            "Region\t[2]\t-42\n")

    found, style = M.find(text, LISTED)

    assert style == "numbered"
    assert [(c["text_span"]["text"], c["references"]) for c in found] == [("[1, 3]", ["cr1", "cr3"])]
