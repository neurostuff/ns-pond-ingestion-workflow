import pytest
import requests
from ingestion_workflow.utils.preprints import (
    PREPRINT_SERVERS,
    is_preprint_doi,
    is_preprint_host,
    preprint_server,
    server_of_filed_preprint_doi,
    preprint_server_of_url,
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
    ("10.31224/4330", "engrxiv"),
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
    ("10.7287/peerj.preprints.737", "peerj_preprints"),
    ("10.7287/peerj.preprints.1071v2", "peerj_preprints"),
    ("10.32388/219hxw", "qeios"),
    ("10.32388/599711.2", "qeios"),
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
    "10.31224/4330x",
    "10.31224/",
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
    # PeerJ's journal DOIs, and the reviews 10.7287 registers for them
    "10.7717/peerj.923",
    "10.7287/peerj.10052v0.1/reviews/2",
    # a Qeios id is six characters
    "10.32388/219hx",
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


@pytest.mark.parametrize("doi, server", [
    # 10.7554/eLife.106070 and 10.7554/eLife.112197 are Crossref's `posted-content` base DOIs of
    # reviewed preprints, `.1` their first version; eLife.92563 is a published paper.
    ("10.7554/elife.112197", "elife"),
    ("10.7554/elife.112197.1", "elife"),
    ("10.1101/2023.09.27.559792", "biorxiv"),
    ("10.31234/osf.io/edaxh_v3", "psyarxiv"),
])
def test_a_doi_filed_as_a_preprint_s_names_its_server(doi, server):
    assert server_of_filed_preprint_doi(doi) == server


def test_an_elife_doi_is_no_preprint_until_it_is_filed_as_one():
    assert not is_preprint_doi("10.7554/elife.112197")
    assert not is_preprint_doi("10.7554/elife.92563")


def test_a_doi_no_server_registered_names_no_server():
    assert server_of_filed_preprint_doi("10.1016/j.neuroimage.2020.117000") is None


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
     "papers.ssrn.com", "chemrxiv.org", "www.techrxiv.org", "www.authorea.com",
     "preprints.jmir.org", "engrxiv.org"],
)
def test_a_preprint_server_host_and_its_subdomains_are_preprint_hosts(host):
    assert is_preprint_host(host)


@pytest.mark.parametrize("host", ["notarxiv.org", "arxiv.org.example.com", "doi.org", "", "www.nature.com",
                   "jmir.org", "www.jmir.org", "medinform.jmir.org"])
def test_other_hosts_are_not(host):
    assert not is_preprint_host(host)


@pytest.mark.parametrize("url, server", [
    ("https://export.arxiv.org/pdf/2111.08743", "arxiv"),
    ("https://www.medrxiv.org/content/10.1101/2021.01.01.21249999v1.full.pdf", "medrxiv"),
    ("https://psyarxiv.com/x2vhs/download", "psyarxiv"),
    ("https://edarxiv.org/rkzna", "edarxiv"),
    ("https://chemrxiv.org/doi/pdf/10.26434/chemrxiv-2023-5rglc-v2", "chemrxiv"),
    ("https://www.techrxiv.org/doi/pdf/10.36227/techrxiv.15010803.v1", "techrxiv"),
    ("https://www.authorea.com/doi/pdf/10.22541/au.164340169.91748604/v1", "authorea"),
    ("https://preprints.jmir.org/preprint/25731", "jmir_preprints"),
    ("https://engrxiv.org/preprint/view/4330", "engrxiv"),
    ("https://www.jmir.org/2021/1/e25731", None),
    ("https://osf.io/preprints/socarxiv/kv2qc", "socarxiv"),
    ("https://osf.io/preprints/lissa/abcde/download", "lis_scholarship_archive"),
    ("https://osf.io/preprints/osf/ynf42_v1", "osf_preprints"),
    # a URL with no provider in it is OSF Preprints'
    ("https://osf.io/x2vhs/download", "osf_preprints"),
    ("https://www.nature.com/articles/x.pdf", None),
])
def test_a_url_names_the_preprint_server_it_is_on(url, server):
    found = preprint_server_of_url(url)
    assert (found.name if found else None) == server


@pytest.mark.parametrize("doi, server", [
    ("10.31234/osf.io/47dx2_v1", "psyarxiv"),
    ("10.31730/osf.io/gub7d", "africarxiv"),
    # not an OSF provider's: osf.io does not serve arXiv
    ("10.48550/arxiv.2111.08743", "osf_preprints"),
    ("10.1093/schbul/sbad113", "osf_preprints"),
])
def test_on_osf_io_the_article_s_doi_names_the_provider(doi, server):
    assert preprint_server_of_url("https://osf.io/47dx2/download", doi).name == server


def test_the_path_names_the_provider_over_the_doi():
    assert preprint_server_of_url("https://osf.io/preprints/psyarxiv/47dx2",
                                  "10.31235/osf.io/47dx2").name == "psyarxiv"


@pytest.mark.vcr
@pytest.mark.parametrize("doi, server", [
    ("10.26434/chemrxiv-2023-5rglc-v2", "chemrxiv"),
    ("10.36227/techrxiv.15010803.v1", "techrxiv"),
    ("10.22541/au.164340169.91748604/v1", "authorea"),
    ("10.2196/preprints.25731", "jmir_preprints"),
    ("10.31224/4330", "engrxiv"),
])
def test_the_page_crossref_records_for_a_preprint_is_on_its_servers_host(doi, server):
    """Crossref's own record of where each DOI resolves, recorded once."""
    record = requests.get(f"https://api.crossref.org/works/{doi}", timeout=30).json()["message"]
    page = record["resource"]["primary"]["URL"]
    assert preprint_server(doi) == server
    assert preprint_server_of_url(page).name == server


def test_the_servers_that_hold_only_preprints():
    """arXiv, SSRN and OSF's providers also take authors' accepted manuscripts; Qeios
    publishes articles it calls peer reviewed."""
    assert {s.name for s in PREPRINT_SERVERS if s.preprints_only} == {
        "biorxiv", "medrxiv", "researchsquare", "preprints_org", "authorea", "techrxiv",
        "chemrxiv", "jmir_preprints", "peerj_preprints"}
