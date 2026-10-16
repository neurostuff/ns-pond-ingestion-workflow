import pytest

from ingestion_workflow.utils.preprints import (
    PREPRINT_SERVERS,
    is_preprint_doi,
    is_preprint_host,
    preprint_server,
)

#: Real DOIs from the catalog, each with the server the registry names.
PREPRINTS = [
    ("10.1101/2019.12.11.873174", "biorxiv"),
    ("10.1101/870287", "biorxiv"),
    ("10.1101/015230", "biorxiv"),
    ("10.64898/2025.11.28.690060", "biorxiv"),
    ("10.1101/2019.12.17.19014415", "medrxiv"),
    ("10.1101/19001834", "medrxiv"),
    ("10.64898/2025.11.26.25341017", "medrxiv"),
    ("10.48550/arxiv.2111.08743", "arxiv"),
    ("10.48550/arxiv.hep-th/9901001", "arxiv"),
    ("10.48550/arxiv.cond-mat.mes-hall/0101001", "arxiv"),
    ("10.48550/arxiv.math.gt/0309136", "arxiv"),
    ("10.31219/osf.io/6meyw", "osf_preprints"),
    ("10.31222/osf.io/abcde", "metaarxiv"),
    ("10.31234/osf.io/32nzp", "psyarxiv"),
    ("10.31234/osf.io/mzg9x_v2", "psyarxiv"),
    ("10.31224/osf.io/m4tqn", "engrxiv"),
    ("10.31232/osf.io/3g7h2", "nutrixiv"),
    ("10.21203/rs.3.rs-10020555/v1", "researchsquare"),
    ("10.21203/rs.3.rs-2556690/v2", "researchsquare"),
    ("10.21203/rs.2.12243/v1", "researchsquare"),
    ("10.2139/ssrn.3349212", "ssrn"),
    ("10.20944/preprints202005.0098.v1", "preprints_org"),
    ("10.22541/au.157878401.17412796", "authorea"),
    ("10.22541/au.164340169.91748604/v1", "authorea"),
    ("10.36227/techrxiv.15010803.v1", "techrxiv"),
    ("10.36227/techrxiv.16611568", "techrxiv"),
    ("10.26434/chemrxiv.11597133.v1", "chemrxiv"),
    ("10.26434/chemrxiv.11977707", "chemrxiv"),
    ("10.26434/chemrxiv-2022-nbwr4", "chemrxiv"),
    ("10.26434/chemrxiv-2023-5rglc-v2", "chemrxiv"),
    ("10.26434/chemrxiv.15010587/v1", "chemrxiv"),
    ("10.2196/preprints.25731", "jmir_preprints"),
]

NOT_PREPRINTS = [
    # Cold Spring Harbor Laboratory journals share bioRxiv's 10.1101
    "10.1101/lm.022889.111",
    "10.1101/gr.112680.110",
    "10.1101/pdb.prot076513",
    "10.1101/cshperspect.a041496",
    "10.1101/mcs.a006280",
    "10.1101/sqb.2014.79.10",
    # an unversioned Research Square DOI is not registered
    "10.21203/rs.3.rs-2556690",
    # OSF projects and the OSF members that are not preprint servers
    "10.17605/osf.io/35n7e",
    "10.31237/osf.io/3kunr",
    # journals of publishers that also run a preprint server
    "10.2196/jmir.1002",
    "10.7554/elife.00425",
    "10.1016/j.neuroimage.2020.117000",
    "10.11011/x",
    "10.1101/2019.12.11.8731",
    "10.1101/2019.12.11.873174x",
    # a version is part of a registered DOI only in each server's own form
    "10.48550/arxiv.2111.08743v2",
    "10.20944/preprints202005.0098",
    "10.21203/rs.3.pex-1234567/v1",
    "10.2139/ssrn.123abc",
    "10.2139/ssrn.",
    "10.26434/chemrxiv",
    "10.26434/chemrxiv_123",
]


@pytest.mark.parametrize("doi,server", PREPRINTS)
def test_a_preprint_doi_names_its_server(doi, server):
    assert preprint_server(doi) == server
    assert is_preprint_doi(doi)


@pytest.mark.parametrize("doi", NOT_PREPRINTS)
def test_a_journal_or_data_doi_is_no_preprint(doi):
    assert preprint_server(doi) is None
    assert not is_preprint_doi(doi)


def test_the_spelling_of_a_doi_does_not_change_its_server():
    assert preprint_server("https://doi.org/10.1101/2019.12.11.873174") == "biorxiv"
    assert preprint_server("DOI:10.48550/arXiv.2111.08743") == "arxiv"
    assert preprint_server("10.31234/OSF.IO/32NZP") == "psyarxiv"


#: study_schema's PreprintServer values, pinned from study_schema#40. Ingestion does not
#: depend on study_schema yet, so this list is the contract until it does; the bump comes
#: when #40 merges, and this becomes the enum itself.
STUDY_SCHEMA_PREPRINT_SERVERS = {
    "biorxiv", "medrxiv", "arxiv", "researchsquare", "osf_preprints", "arabixiv",
    "metaarxiv", "engrxiv", "focus_archive", "frenxiv", "ina_rxiv", "lawarxiv",
    "lis_scholarship_archive", "marxiv", "mindrxiv", "nutrixiv", "paleorxiv", "psyarxiv",
    "socarxiv", "sportrxiv", "africarxiv", "mediarxiv", "bodoarxiv", "edarxiv",
    "biohackrxiv", "ssrn", "preprints_org", "authorea", "techrxiv", "chemrxiv",
    "jmir_preprints",
}


def test_the_registrys_servers_are_study_schemas_preprint_servers():
    assert {s.name for s in PREPRINT_SERVERS} == STUDY_SCHEMA_PREPRINT_SERVERS


def test_no_doi_is_no_preprint():
    assert preprint_server(None) is None
    assert preprint_server("") is None


def test_the_rxiv_servers_split_on_the_length_of_the_numeric_suffix():
    assert preprint_server("10.1101/2020.01.01.123456") == "biorxiv"
    assert preprint_server("10.1101/2020.01.01.12345678") == "medrxiv"
    assert preprint_server("10.64898/2026.01.01.123456") == "biorxiv"
    assert preprint_server("10.64898/2026.01.01.12345678") == "medrxiv"
    assert preprint_server("10.64898/2026.01.01.1234") is None


def test_no_doi_belongs_to_two_servers():
    for doi, _ in PREPRINTS:
        assert [s.name for s in PREPRINT_SERVERS if s.matches(doi)] == [preprint_server(doi)]


@pytest.mark.parametrize(
    "host",
    ["arxiv.org", "export.arxiv.org", "www.biorxiv.org", "www.medrxiv.org", "osf.io",
     "preprints.osf.io", "psyarxiv.com", "www.preprints.org", "www.researchsquare.com",
     "papers.ssrn.com"],
)
def test_a_preprint_server_host_and_its_subdomains_are_preprint_hosts(host):
    assert is_preprint_host(host)


@pytest.mark.parametrize("host", ["notarxiv.org", "arxiv.org.example.com", "doi.org", "", "www.nature.com"])
def test_other_hosts_are_not(host):
    assert not is_preprint_host(host)
