"""The links an article gives, read from real articles.

- `data/links/europepmc_PMC13214571/article.xml`: Europe PMC's JATS of Stoll et al. 2026,
  Imaging Neuroscience 10.1162/IMAG.a.1210 (CC BY 4.0), whose data statement links its
  OpenNeuro dataset by the word "OpenNeuro" alone, and whose reference list holds the OSF
  project's DOI.
- `data/links/pdf_PMC12862833/text.txt`: the PDF extractor's text (docling) of Ashby et al.
  2025, Frontiers in Neuroscience 10.3389/fnins.2025.1661523 (CC BY 4.0), whose OSF link
  and OpenNeuro DOI are broken across lines.
- `data/links/elsevier_PMC12084501/content.xml`: Elsevier's full-text XML of Bierlich et al.
  2025, NeuroImage: Clinical 10.1016/j.nicl.2025.103793 (CC BY 4.0, its
  `openaccessUserLicense`), fetched through ns-pond's Elsevier client. Its OSF project is a
  `ce:inter-ref` in the Methods and again in its "Data availability" section, and its
  reference list prints an OSF DOI no `ce:inter-ref` marks.
- `data/sample_outputs/6dYthnbocsU4`: ACE's text of NeuroImage 10.1016/j.neuroimage.2021.117808,
  which glued a section number to the OSF link before it.
- `data/test_html/Nature neuroscience/33106677.html`: a Nature Neuroscience page.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.extractors.elsevier_extractor import article_text_and_captions
from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.services import paper_links as L
from ingestion_workflow.services.citations import read_elsevier, read_jats
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.reference_lists import from_crossref
from ingestion_workflow.tests import cassette_secrets
from lxml import etree

DATA = Path(__file__).parents[1] / "data"
JATS = DATA / "links" / "europepmc_PMC13214571" / "article.xml"
ELSEVIER = DATA / "links" / "elsevier_PMC12084501" / "content.xml"
PDF_TEXT = DATA / "links" / "pdf_PMC12862833" / "text.txt"
ACE_TEXT = DATA / "sample_outputs" / "6dYthnbocsU4" / "processed" / "ace" / "text.txt"
NATURE_PAGE = DATA / "test_html" / "Nature neuroscience" / "33106677.html"


@pytest.fixture(scope="module")
def jats():
    """(the text pubget's extractor makes of the XML, the links read from both and from
    the reference list the references stage reads)."""
    text = article_text(etree.parse(str(JATS)), JATS.parent)
    links = L.read_links(text, L._jats(JATS, text, {}), read_jats(JATS, text).references)
    return text, {link["url"]: link for link in links}


@pytest.fixture(scope="module")
def elsevier():
    """(the text Elsevier's extraction keeps, the links read from it, the XML and the
    references stage's list)."""
    text = article_text_and_captions(ELSEVIER.read_bytes())[0]
    listed = read_elsevier(ELSEVIER, text).references
    links = L.read_links(text, L._elsevier(ELSEVIER, text, {}), listed)
    return text, {link["url"]: link for link in links}


def _found(links, url):
    return [(o["found_by"], o["section"]) for o in links[url]["occurrences"]]


# -- normalizing ---------------------------------------------------------------------


@pytest.mark.parametrize("given, url", [
    # hrefs as these articles give them
    ("http://OpenNeuro.org", "http://openneuro.org"),
    ("https://osf.io/4b9v8/", "https://osf.io/4b9v8"),
    ("https://www.nitrc.org/projects/gppi", "https://nitrc.org/projects/gppi"),
    ("www.fil.ion.ucl.ac.uk/spm/", "https://fil.ion.ucl.ac.uk/spm"),
    ("osf.io/4drz6/overview", "https://osf.io/4drz6/overview"),
    ("https://www.who.int/health-topics/infodemic#tab=tab_1", "https://who.int/health-topics/infodemic"),
    ("https://osf.io/x7z9q/?view_only=8a1b", "https://osf.io/x7z9q?view_only=8a1b"),
    # a DOI, at a resolver, bare or as a JATS doi ext-link gives it: lower case
    ("https://doi.org/10.1162/IMAG.a.1210#supplementary-data", "https://doi.org/10.1162/imag.a.1210"),
    ("https://dx.doi.org/10.18112/openneuro.ds007115.v1.0.0",
     "https://doi.org/10.18112/openneuro.ds007115.v1.0.0"),
    ("10.17605/OSF.IO/4DRZ6", "https://doi.org/10.17605/osf.io/4drz6"),
    ("doi:10.5281/zenodo.123", "https://doi.org/10.5281/zenodo.123"),
])
def test_a_link_is_normalized_as_paper_link_url_says(given, url):
    assert L.normalize_url(given) == url


@pytest.mark.parametrize("given", [
    "fnins-19-1661523.pdf",  # a supplement's file name
    "mailto:stefania_ashby@byu.edu",
    "journal=OpenNeuro&title=Data for",  # Europe PMC's google-scholar ext-link
    "SCR_007037",
])
def test_what_is_not_an_http_link_is_none(given):
    assert L.normalize_url(given) is None


@pytest.mark.parametrize("url, target", [
    ("https://osf.io/4b9v8", L.Target("osf", "4b9v8")),
    ("https://osf.io/4drz6/overview", L.Target("osf", "4drz6")),
    ("https://osf.io/x7z9q?view_only=8a1b", L.Target("osf", "x7z9q", view_only="8a1b")),
    ("https://doi.org/10.17605/osf.io/4drz6", L.Target("osf", "4drz6")),
    ("https://openneuro.org/datasets/ds006128", L.Target("openneuro", "ds006128")),
    ("https://openneuro.org/datasets/ds006128/versions/1.0.9",
     L.Target("openneuro", "ds006128", "1.0.9")),
    ("https://doi.org/10.18112/openneuro.ds007115.v1.0.0",
     L.Target("openneuro", "ds007115", "1.0.0")),
    ("http://openneuro.org", None),  # the site, not a dataset
    ("https://osf.io/preprints/psyarxiv/abcde", None),
    ("https://neurovault.org/collections/9096", None),  # recorded, never probed
])
def test_only_an_osf_project_or_openneuro_dataset_is_a_probe_target(url, target):
    assert L.probe_target(url) == target


# -- the text ------------------------------------------------------------------------


def test_links_broken_across_lines_in_a_pdf_s_text_are_joined():
    text = PDF_TEXT.read_text(encoding="utf-8")
    found = {url: text[start:end] for url, start, end in L.from_text(text)}

    assert found["https://osf.io/4b9v8"] == "https://osf. io/4b9v8/"
    assert "https://doi.org/10.18112/openneuro.ds007115.v1.0.0" in found
    assert found["https://data.who.int/dashboards/covid19/more-resources"] == \
        "https://data.who.int/dashboards/ covid19/more-resources"
    # the bracket and the full stop the sentence closes on are not the link's
    assert text[text.index("https://osf. io") - 1] == "("
    # a word of the sentence after a link is not joined to it
    assert not any(" (Accessed" in raw or " and " in raw for raw in found.values())


def test_a_dataset_doi_the_text_prints_bare_is_a_link_and_another_doi_is_not():
    text = PDF_TEXT.read_text(encoding="utf-8")
    urls = [url for url, _, _ in L.from_text(text)]

    # "doi: 10.18112/openneuro. ds007115.v1.0.0", twice, each broken in its own place
    assert urls.count("https://doi.org/10.18112/openneuro.ds007115.v1.0.0") == 2
    # the article's own DOI, printed bare in its front matter, is not a dataset's
    assert "https://doi.org/10.3389/fnins.2025.1661523" not in urls


def test_a_section_number_a_page_s_text_glued_to_a_link_is_not_the_link_s():
    text = ACE_TEXT.read_text(encoding="utf-8")
    found = {url: text[start:end] for url, start, end in L.from_text(text)}

    assert "available at the Open Science Framework (OSF) https://osf.io/w6khb.2.6. fMRI" in text
    assert found["https://osf.io/w6khb"] == "https://osf.io/w6khb"
    # the heading after a link is not joined to it: "...collection:9096.3. Results3.1."
    assert not any("Results" in raw for raw in found.values())


# -- markup --------------------------------------------------------------------------


def test_jats_markup_gives_a_link_the_text_names_by_a_word(jats):
    text, links = jats
    (occurrence,) = [o for o in links["https://openneuro.org/datasets/ds006128"]["occurrences"]
                     if o["start_char"] is not None][:1]

    # the text says only "OpenNeuro"; the ext-link's href is the dataset
    assert text[occurrence["start_char"]:occurrence["end_char"]] == "OpenNeuro"
    assert occurrence["found_by"] == "markup"


def test_one_link_given_many_times_is_one_entry_with_every_occurrence(jats):
    _, links = jats

    assert _found(links, "https://osf.io/4drz6/overview") == [("markup", "data_availability")] * 4


def test_the_reference_list_gives_its_urls_and_its_dataset_dois(jats):
    _, links = jats

    # <ext-link ext-link-type="doi" xlink:href="10.17605/OSF.IO/4DRZ6"/> in reference b82
    (osf,) = links["https://doi.org/10.17605/osf.io/4drz6"]["occurrences"]
    assert (osf["found_by"], osf["reference_id"]) == ("reference", "IMAG.a.1210-b82")
    assert osf["start_char"] is None  # pubget's text holds no reference list
    assert _found(links, "https://openneuro.org/datasets/ds006128/versions/1.0.9") == [
        ("reference", None)]
    # a cited article's DOI is a citation, not a link
    cited = [url for url in links if L.is_doi_link(url) and not L.is_data_doi_link(url)]
    assert cited == ["https://doi.org/10.1162/imag.a.1210"]  # the paper's own supplement link
    assert all(o["found_by"] == "markup" for o in links[cited[0]]["occurrences"])


def test_a_link_s_occurrences_are_in_text_order_and_its_unplaced_entry_last(jats):
    """The GitHub repository is an ext-link twice in the data statement, and an ext-link in
    reference b81 between them in the XML, which pubget's text does not print."""
    text, links = jats

    occurrences = links["https://github.com/Kriaese/manuscript-zoomprf"]["occurrences"]
    assert [(o["found_by"], o.get("reference_id"), o["section"]) for o in occurrences] == [
        ("markup", None, "data_availability"), ("markup", None, "data_availability"),
        ("reference", "IMAG.a.1210-b81", None)]
    first, second, _ = occurrences
    assert first["start_char"] < second["start_char"]
    assert [text[o["start_char"]:o["end_char"]] for o in (first, second)] == ["GitHub"] * 2


def test_a_link_the_text_also_prints_is_the_markup_s(jats):
    """`(https://www.quaerosys.com/)` is an ext-link: one occurrence, not a second from
    the text pass."""
    _, links = jats

    assert _found(links, "https://quaerosys.com") == [("markup", "methods")]


def test_a_page_s_anchors_that_print_a_link_are_links_and_its_furniture_is_not():
    links = L._html(NATURE_PAGE, "")
    urls = {o.url for o in links}

    assert {"https://openneuro.org", "https://github.com/git", "https://balsa.wustl.edu"} <= urls
    # "PubMed", "Google Scholar", "Log in", "Follow us on Twitter"
    assert not any(host in url for url in urls
                   for host in ("ncbi.nlm.nih.gov", "scholar.google", "idp.nature", "twitter.com"))


def test_elsevier_inter_refs_are_links_placed_in_the_text_in_text_order(elsevier):
    text, links = elsevier

    osf = links["https://osf.io/cw7n4"]
    assert _found(links, "https://osf.io/cw7n4") == [("markup", "methods"),
                                                     ("markup", "data_availability")]
    assert [text[o["start_char"]:o["end_char"]] for o in osf["occurrences"]] == [
        "https://osf.io/cw7n4"] * 2
    # "Statistical maps of the fMRI analyses ... are provided" follows the second
    assert [o["context"] for o in osf["occurrences"]] == [[], ["statistical maps"]]
    assert osf["nifti_context"] is True
    # the inter-ref in reference b0070, which Elsevier's text does not print: its href,
    # with its own scheme
    assert [(o["found_by"], o["reference_id"], o["start_char"])
            for o in links["http://ncbi.nlm.nih.gov/pubmed/9931269"]["occurrences"]] == [
        ("reference", "b0070", None)]
    # a cited article's DOI (`ce:doi` of an `sb:reference`) is a citation, not a link
    assert not [url for url in links if L.is_doi_link(url) and not L.is_data_doi_link(url)]


def test_a_dataset_doi_the_reference_list_alone_prints_names_its_entry(elsevier):
    """`<ce:other-ref><ce:textref>M.A. Goss-Sampson ... https://doi.org/10.17605/OSF.IO/CKNXM.`
    in entry b0150: no markup marks it and the text does not print it."""
    _, links = elsevier

    assert links["https://doi.org/10.17605/osf.io/cknxm"]["occurrences"] == [
        {"found_by": "reference", "raw": "https://doi.org/10.17605/OSF.IO/CKNXM",
         "start_char": None, "end_char": None, "section": None, "context": [],
         "reference_id": "b0150"}]


def test_markup_in_an_entry_names_it_only_beside_the_list_that_holds_it():
    text = article_text_and_captions(ELSEVIER.read_bytes())[0]
    pubmed = "http://ncbi.nlm.nih.gov/pubmed/9931269"

    def named(listed):
        links = {link["url"]: link
                 for link in L.read_links(text, L._elsevier(ELSEVIER, text, {}), listed)}
        return [o.get("reference_id") for o in links[pubmed]["occurrences"]]

    assert named(None) == [None]
    assert named([{"id": "b0071", "text": "another entry"}]) == [None]
    assert named([{"id": "b0070", "text": "Dale, A. M. (1999). NeuroImage, 9(2), 195-207."}]) \
        == ["b0070"]


# -- the reference list Crossref holds -------------------------------------------------


@pytest.mark.vcr()
def test_crossref_s_list_gives_the_links_its_entries_print():
    """Crossref's record of 10.1162/IMAG.a.1210: the publisher deposited the paper's own
    DOI on its two dataset entries, and printed their links in the entries' text."""
    message = CrossrefClient(cassette_secrets.REPLAY_EMAIL).work("10.1162/IMAG.a.1210")
    occurrences = L.from_reference_list(from_crossref(message))
    found = {(o.url, o.reference_id) for o in occurrences}

    assert ("https://doi.org/10.17605/osf.io/4drz6", "cr82") in found
    assert ("https://github.com/Kriaese/manuscript-zoomprf", "cr81") in found
    links = {link["url"] for link in L.read_links("", [], from_crossref(message))}
    assert "https://doi.org/10.1162/imag.a.1210" not in links


# -- sections and context --------------------------------------------------------------


@pytest.mark.parametrize("heading", [
    "## Data availability", "## Data and Code Availability", "### Data Availability Statement",
    "## 5. Data and materials availability", "## Availability of data and materials",
    "## Code and data availability",
])
def test_a_data_availability_heading_is_its_own_section(heading):
    text = f"## Methods\nm\n{heading}\nd\n## References\nr\n"
    assert [label for _, _, label in sectionize(text)] == [
        "methods", "data_availability", "back"]


def test_a_pdf_s_bare_data_availability_line_is_its_own_section():
    text = PDF_TEXT.read_text(encoding="utf-8")
    links = {link["url"]: link for link in L.read_links(text, None)}

    assert _found(links, "https://osf.io/4b9v8") == [("text", "data_availability")]
    # the methods print the dataset DOI beside "NIfTI" (and "Brain Imaging Data
    # Structure"); the data statement does not
    occurrences = links["https://doi.org/10.18112/openneuro.ds007115.v1.0.0"]["occurrences"]
    assert [(o["section"], o["context"]) for o in occurrences] == [
        ("methods", ["nifti", "imaging data"]), ("data_availability", [])]
    assert links["https://doi.org/10.18112/openneuro.ds007115.v1.0.0"]["nifti_context"] is True


def test_the_links_are_json():
    text = PDF_TEXT.read_text(encoding="utf-8")
    assert json.loads(json.dumps(L.read_links(text, None)))
