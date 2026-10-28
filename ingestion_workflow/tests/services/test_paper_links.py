"""The links an article gives, read from real articles.

- `data/links/europepmc_PMC13214571/article.xml`: Europe PMC's JATS of Stoll et al. 2026,
  Imaging Neuroscience 10.1162/IMAG.a.1210 (CC BY 4.0), whose data statement links its
  OpenNeuro dataset by the word "OpenNeuro" alone, and whose reference list holds the OSF
  project's DOI.
- `data/links/pdf_PMC12862833/text.txt`: the PDF extractor's text (docling) of Ashby et al.
  2025, Frontiers in Neuroscience 10.3389/fnins.2025.1661523 (CC BY 4.0), whose OSF link
  and OpenNeuro DOI are broken across lines.
- `data/sample_outputs/6dYthnbocsU4`: ACE's text of NeuroImage 10.1016/j.neuroimage.2021.117808,
  which glued a section number to the OSF link before it.
- `data/test_html/Nature neuroscience/33106677.html`: a Nature Neuroscience page.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.services import paper_links as L
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.reference_lists import from_crossref
from ingestion_workflow.tests import cassette_secrets
from lxml import etree

DATA = Path(__file__).parents[1] / "data"
JATS = DATA / "links" / "europepmc_PMC13214571" / "article.xml"
PDF_TEXT = DATA / "links" / "pdf_PMC12862833" / "text.txt"
ACE_TEXT = DATA / "sample_outputs" / "6dYthnbocsU4" / "processed" / "ace" / "text.txt"
NATURE_PAGE = DATA / "test_html" / "Nature neuroscience" / "33106677.html"


@pytest.fixture(scope="module")
def jats():
    """(xml path, the text pubget's extractor makes of it, the links read from both)."""
    text = article_text(etree.parse(str(JATS)), JATS.parent)
    notes = {}
    links = L.read_links(text, L._jats(JATS, text, notes))
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
    assert (osf["found_by"], osf["reference"]) == ("reference", "IMAG.a.1210-b82")
    assert _found(links, "https://openneuro.org/datasets/ds006128/versions/1.0.9") == [
        ("reference", None)]
    # a cited article's DOI is a citation, not a link
    cited = [url for url in links if L.is_doi_link(url) and not L.is_data_doi_link(url)]
    assert cited == ["https://doi.org/10.1162/imag.a.1210"]  # the paper's own supplement link
    assert all(o["found_by"] == "markup" for o in links[cited[0]]["occurrences"])


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


ELSEVIER = b"""<?xml version="1.0" encoding="UTF-8"?>
<full-text-retrieval-response xmlns="http://www.elsevier.com/xml/svapi/article/dtd" xmlns:ce="http://www.elsevier.com/xml/common/dtd" xmlns:sb="http://www.elsevier.com/xml/common/struct-bib/dtd" xmlns:ja="http://www.elsevier.com/xml/ja/dtd" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:prism="http://prismstandard.org/namespaces/basic/2.0/" xmlns:xocs="http://www.elsevier.com/xml/xocs/dtd" xmlns:xlink="http://www.w3.org/1999/xlink">
<coredata><dc:title>A test article</dc:title><prism:doi>10.1016/j.test.2020.1</prism:doi></coredata>
<originalText><xocs:doc><xocs:serial-item><ja:article>
<ja:head><ce:title>A test article</ce:title></ja:head>
<ja:body><ce:sections><ce:section id="s1"><ce:section-title>Methods</ce:section-title>
<ce:para>Images were analysed in SPM12 (<ce:inter-ref xlink:href="http://www.fil.ion.ucl.ac.uk/spm/">www.fil.ion.ucl.ac.uk/spm</ce:inter-ref>).</ce:para>
</ce:section>
<ce:section id="s2"><ce:section-title>Data availability</ce:section-title>
<ce:para>Unthresholded statistical maps are on <ce:inter-ref xlink:href="https://neurovault.org/collections/9096/">NeuroVault</ce:inter-ref>.</ce:para>
</ce:section></ce:sections></ja:body>
<ja:tail><ce:bibliography><ce:bibliography-sec>
<ce:bib-reference id="bib1"><ce:label>Smith, 2001</ce:label><sb:reference><sb:host><sb:issue><sb:series><sb:title><sb:maintitle>J Neurosci</sb:maintitle></sb:title></sb:series><sb:date>2001</sb:date></sb:issue><ce:doi>10.1523/x.2001</ce:doi></sb:host></sb:reference></ce:bib-reference>
<ce:bib-reference id="bib2"><ce:label>Data, 2021</ce:label><sb:reference><sb:host><sb:e-host><ce:inter-ref xlink:href="https://osf.io/w6khb/">https://osf.io/w6khb/</ce:inter-ref></sb:e-host><ce:doi>10.17605/OSF.IO/W6KHB</ce:doi></sb:host></sb:reference></ce:bib-reference>
</ce:bibliography-sec></ce:bibliography></ja:tail>
</ja:article></xocs:serial-item></xocs:doc></originalText></full-text-retrieval-response>"""


def test_elsevier_inter_refs_are_links_placed_in_the_text(tmp_path):
    """No real Elsevier article is held here (its API needs a key); the markup is the shape
    `test_citations` reads, with Elsevier's `ce:inter-ref` and `ce:doi`."""
    from elsevier_coordinate_extraction.extract.text import (
        extract_text_from_article,
        format_article_text,
    )

    path = tmp_path / "content.xml"
    path.write_bytes(ELSEVIER)
    text = format_article_text(extract_text_from_article(ELSEVIER, True, True))
    links = {link["url"]: link for link in L.read_links(text, L._elsevier(path, text, {}))}

    (vault,) = links["https://neurovault.org/collections/9096"]["occurrences"]
    assert text[vault["start_char"]:vault["end_char"]] == "NeuroVault"
    assert (vault["found_by"], vault["section"]) == ("markup", "data_availability")
    assert vault["context"] == ["unthresholded", "statistical maps"]
    assert links["https://neurovault.org/collections/9096"]["nifti_context"] is True
    # the href's own scheme is kept: http
    assert _found(links, "http://fil.ion.ucl.ac.uk/spm") == [("markup", "methods")]
    assert _found(links, "https://osf.io/w6khb") == [("reference", None)]
    assert _found(links, "https://doi.org/10.17605/osf.io/w6khb") == [("reference", None)]
    assert "https://doi.org/10.1523/x.2001" not in links


# -- the reference list Crossref holds -------------------------------------------------


@pytest.mark.vcr()
def test_crossref_s_list_gives_the_links_its_entries_print():
    """Crossref's record of 10.1162/IMAG.a.1210: the publisher deposited the paper's own
    DOI on its two dataset entries, and printed their links in the entries' text."""
    message = CrossrefClient(cassette_secrets.REPLAY_EMAIL).work("10.1162/IMAG.a.1210")
    occurrences = L.from_reference_list(from_crossref(message))
    found = {(o.url, o.reference) for o in occurrences}

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
