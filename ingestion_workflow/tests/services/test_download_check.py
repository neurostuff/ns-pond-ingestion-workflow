"""Which downloaded pages are the article, judged on real stored pages.

Each fixture is a page from the ns-pond catalog, named by its article id, with
scripts, styles and most attributes cut; the check gives the trimmed copy the
same verdict as the original.
"""

import gzip
from pathlib import Path

import pytest
from ingestion_workflow.extractors.ace_extractor import _validate_downloaded_html
from ingestion_workflow.services import download_check
from ingestion_workflow.services.download_check import (
    ABSTRACT_ONLY,
    CITATION_STUB,
    EMPTY_SHELL,
    ERROR_PAGE,
    PAYWALL,
    check_page,
    reason_of,
)

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"


def real(article_id: str) -> bytes:
    return gzip.decompress((PAGES / f"{article_id}.html.gz").read_bytes())


@pytest.mark.parametrize(
    "article_id, reason, says",
    [
        # BMJ: "The abstract/extract view of this article is free" over the abstract
        ("bvqhehtd4k4d", PAYWALL, "abstract/extract view"),
        # Karger: an abstract and its references, 753 words of prose, under
        # "You do not currently have access"
        ("llunnvaq5twj", PAYWALL, "do not currently have access"),
        ("a6mheoqpeocv", ERROR_PAGE, "Currently Unavailable"),
        # Thieme-Connect's bot-protection page, 65 words, stored as a successful download
        ("11823921", ERROR_PAGE, "malicious requests caused by AI crawlers"),
        ("2hj3g3mppft2", EMPTY_SHELL, "0 characters"),
        # PubMed's record of the article: its abstract, then lists of similar articles
        ("cgubhcqx6dou", CITATION_STUB, "PubMed"),
        # Wiley: title, authors and links, no prose at all
        ("2v4lzvqpm7wx", CITATION_STUB, "0 words of prose"),
        # Wiley: 31 words of prose, beside a cookie notice and a footer of hundreds more
        ("rftvf4hxbvy3", CITATION_STUB, "31 words of prose"),
    ],
)
def test_a_page_that_is_not_the_article_is_named(article_id, reason, says):
    bad = check_page(real(article_id))
    assert bad is not None and bad.reason == reason
    assert says in bad.detail


@pytest.mark.parametrize(
    "article_id",
    [
        # Karger behind a long cookie-consent dialog; ACE's text of it was a citation
        # pop-up of 453 characters, but the page holds the whole article
        "af3skhwhkgkm",
        # "Subscribe" in the footer and the header around the full text
        "onv6piwppvwo",
        # Karger printing the abstract before the full text
        "ytpicngb4emb",
        # A windows-1252 <meta> over UTF-8 bytes, behind a Cookiebot dialog
        "htdtw2yqpj6o",
        # JAMA, whose wrapper around the whole article is named for its cookie banner
        "44yiqcp3u3na",
    ],
)
def test_a_full_article_passes(article_id):
    assert check_page(real(article_id)) is None


def test_an_article_is_not_judged_by_a_notice_beside_it():
    """Publishers print paywall wording around full texts they serve to a subscriber."""
    html = real("ytpicngb4emb").replace(b"</body>", b"<aside>Purchase this article</aside></body>")
    assert check_page(html) is None


def test_an_article_that_mentions_ai_crawlers_in_its_prose_passes():
    html = real("ytpicngb4emb").replace(
        b"</body>",
        b"<p>Sites are protecting their servers from malicious requests caused by AI "
        b"crawlers, as our discussion of web scraping in research notes.</p></body>",
    )
    assert check_page(html) is None


def test_a_page_opening_with_nul_bytes_is_read_past_them():
    assert check_page(b"\x00" * 4096 + real("ytpicngb4emb")) is None


def test_a_file_of_nul_bytes_is_an_empty_shell(tmp_path):
    path = tmp_path / "page.html"
    path.write_bytes(b"\x00" * 4096)
    assert download_check.check_file(path).reason == EMPTY_SHELL


def test_the_reason_reads_back_from_the_failure_message():
    bad = check_page(real("bvqhehtd4k4d"))
    assert reason_of(f"ACE scrape produced invalid HTML: {bad.message}") == PAYWALL
    assert reason_of("HTML payload was empty.") is None


def test_ace_rejects_the_page_with_the_reason(tmp_path):
    path = tmp_path / "page.html"
    path.write_bytes(real("a6mheoqpeocv"))
    ok, message = _validate_downloaded_html(path)
    assert not ok and reason_of(message) == ERROR_PAGE
    path.write_bytes(real("onv6piwppvwo"))
    assert _validate_downloaded_html(path) == (True, None)


@pytest.mark.parametrize(
    "article_id",
    [
        # Full short articles whose citation title ends or runs on with page 404:
        # "... 66 (3): 404 -- Journal of Neurology, ..."
        "6hhu36c7z2wa",
        "p4frymsevkls",
        # A JAMA letter served under "Access Provided by University of Texas", beside
        # "Access through your institution", "Purchase access" and "Rent this article"
        "ipxutfjxf2p7",
    ],
)
def test_a_short_article_passes_whatever_its_page_number_or_storefront(article_id):
    assert check_page(real(article_id)) is None


def test_a_status_code_heading_over_a_short_page_is_an_error_page():
    bad = check_page(real("bgqw56xoums3"))
    assert (bad.reason, bad.detail) == (ERROR_PAGE, "heading 'ScienceDirect - 404'")


def test_a_status_code_at_a_heading_end_counts_only_over_a_short_page():
    # the short article above, titled as if a site had appended its status code
    titled = real("6hhu36c7z2wa").replace(b"</title>", b" - 404</title>", 1)
    assert check_page(titled) is None


def test_an_offer_to_sell_the_article_is_a_paywall_when_no_access_was_provided():
    # BMJ: the abstract under "Buy this article", 364 words of prose
    bad = check_page(real("hijvw5niyh6f"))
    assert bad.reason == PAYWALL and "Buy this article" in bad.detail
    banner = b"<p>Access provided by a library</p></body>"
    served = real("hijvw5niyh6f").replace(b"</body>", banner)
    assert check_page(served).reason == PAYWALL  # a banner over an abstract is not access


def test_ace_moves_a_rejected_page_out_of_its_cache_and_says_where(tmp_path, monkeypatch):
    from ingestion_workflow.config import Settings
    from ingestion_workflow.extractors import ace_extractor
    from ingestion_workflow.extractors.ace_extractor import ACEExtractor
    from ingestion_workflow.models.ids import Identifier

    monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)  # nothing is parsed

    settings = Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data",
                        ace_cache_root=tmp_path / "ace" / "html", ace_max_workers=1)
    extractor = ACEExtractor(settings=settings, download_mode="browser")
    cached = tmp_path / "ace" / "html" / "html" / "Brain" / "123.html"

    class Scraper:
        def process_article(self, pmid, journal, **kwargs):
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(real("a6mheoqpeocv"))
            return str(cached), True

    for n in range(2):  # a second rejection of the same page keeps the first copy too
        result = extractor._download_single(Identifier(pmid="123"), scraper=Scraper())
        assert not result.success and reason_of(result.error_message) == ERROR_PAGE
        assert not cached.exists()
        [(original, moved)] = result.moved_aside.items()
        assert original == str(cached)
        quarantine = tmp_path / "ace" / "html-quarantine" / "html" / "Brain"
        assert Path(moved) == quarantine / ("123.html" if n == 0 else "123.html.1")
        assert Path(moved).read_bytes() == real("a6mheoqpeocv")


@pytest.mark.parametrize(
    "article_id, says",
    [
        # AHA Stroke under its site-wide "Access Provided by University of Texas--Austin"
        # banner: the body holds only the access box
        ("26vmoziyyfvl", "Get full access to this article"),
        ("dxlmu53h3tvn", "Get full access to this article"),
        # Brain Connectivity "No access": abstract and references, over 800 words of
        # prose, and the access box where the body would be
        ("22q7d4w5vmbg", "Get full access to this article"),
        # JAMA Surgery under the same kind of banner: a 145-word first-page excerpt
        # beside "Purchase access" and "Rent this article"
        ("rioq37zk7spc", "Purchase access"),
    ],
)
def test_a_paywall_under_an_access_banner_is_still_a_paywall(article_id, says):
    bad = check_page(real(article_id))
    assert bad.reason == PAYWALL and says in bad.detail


def test_an_abstract_with_nothing_but_page_furniture_is_abstract_only():
    """JOCN: the abstract, printed twice, beside the press's blurb, a login dialog and
    lorem ipsum; 947 words in prose blocks, so it clears the prose gate."""
    bad = check_page(real("kmtfrgqccuhz"))
    assert bad is not None and bad.reason == ABSTRACT_ONLY
    assert "body sections: none" in bad.detail
    assert bad.detail.endswith("947 words in prose blocks")
    assert reason_of(bad.message) == ABSTRACT_ONLY


def test_other_papers_abstracts_listed_beside_an_abstract_are_not_its_body():
    """Brain Research: the abstract over "Recommended articles", 2,303 words in prose
    blocks, most of them other papers' abstracts."""
    bad = check_page(real("ox5vn5dnq74h"))
    assert bad is not None and bad.reason == ABSTRACT_ONLY


@pytest.mark.parametrize(
    "article_id",
    [
        # Karger and Pediatrics full articles whose ACE text came out short
        "af3skhwhkgkm",
        "ytpicngb4emb",
        "gdsqtwkjfl24",
        # JAMA: a full article with no heading named for a section
        "44yiqcp3u3na",
        # A JAMA letter and two short JNNP items: no abstract, no sections
        "ipxutfjxf2p7",
        "6hhu36c7z2wa",
        "p4frymsevkls",
    ],
)
def test_articles_and_letters_are_not_abstract_only(article_id):
    assert check_page(real(article_id)) is None


def test_a_letter_shows_no_abstract_and_an_article_shows_its_sections():
    letter = download_check.page_prose(real("ipxutfjxf2p7"))
    assert letter.abstract_sentences < download_check.ABSTRACT_SENTENCES
    assert letter.sections == ()
    article = download_check.page_prose(real("af3skhwhkgkm"))
    assert {"introduction", "results", "discussion"} <= set(article.sections)


def test_a_structured_abstract_s_headings_are_not_the_body_s():
    page = real("kmtfrgqccuhz")
    labels = b"<h3>Methods</h3><p>x</p><h3>Results</h3>"
    in_abstract = page.replace(b'<div class="abstractSection abstractInFull">',
                               b'<div class="abstractSection abstractInFull">' + labels, 1)
    assert check_page(in_abstract).reason == ABSTRACT_ONLY
    in_body = page.replace(b"</body>", labels + b"</body>", 1)
    assert check_page(in_body) is None


def test_a_sentence_repeated_on_the_page_counts_once():
    sentence = "The participants performed the task in the scanner for twenty minutes. "
    assert download_check.prose_sentences(sentence * 3) == [sentence.strip()]


def test_reference_entries_and_table_rows_are_not_prose():
    assert download_check.prose_sentences(
        "Smith J, Jones K (2003) Attention modulates the response of visual cortex in "
        "humans. J Neurosci 23:100-110. PubMed Google Scholar\n"
        "Left\tamygdala\t-22\t-4\t-18\t4.21\n"
    ) == []


@pytest.mark.parametrize(
    "article_id",
    [
        # Cognitive Neuroscience commentary: an abstract and 1,300 words of body, no
        # section headings, its sentences full of "et al." and "Citation1950"
        "zjuytbbzrh7f",
        # NeuroImage "Comments and Controversies", about 970 words of body
        "utc3rmca2hzo",
        # Chinese full texts with an English abstract, 7,000-8,000 characters of body
        "gqboky4v37pk",
        "ejg6mnmruo3n",
    ],
)
def test_a_body_without_sections_or_in_another_script_is_not_abstract_only(article_id):
    assert check_page(real(article_id)) is None
    found = download_check.page_prose(real(article_id))
    assert found.abstract_sentences >= download_check.ABSTRACT_SENTENCES
    assert found.body_words >= download_check.BODY_PROSE_WORDS


def test_an_abstract_page_s_furniture_is_not_body_prose():
    found = download_check.page_prose(real("kmtfrgqccuhz"))
    assert found.body_words < download_check.BODY_PROSE_WORDS
    assert "words of body prose" in check_page(real("kmtfrgqccuhz")).detail


def test_the_sentence_ceiling_counts_the_abstract_and_leaves_out_the_references():
    """kmtfrgqccuhz, its abstract's 8 sentences and 6 others, with distinct sentences of
    prose added: at 19 it is still the abstract alone, at 20 it is not."""
    page = real("kmtfrgqccuhz")
    found = download_check.page_prose(page)
    assert (found.abstract_sentences, found.sentences - found.reference_sentences) == (8, 14)
    room = 6

    def added(n):
        body = " ".join(f"Volunteer {chr(65 + i)} lay still in the scanner while the task "
                        f"was described to them slowly." for i in range(n))
        return page.replace(b"</body>", f"<div><p>{body}</p></div></body>".encode(), 1)

    assert check_page(added(room - 1)).reason == ABSTRACT_ONLY
    assert check_page(added(room)) is None


def test_a_short_communication_passes():
    """Springer Short Communication: an abstract and about 800 words of unheaded body."""
    assert check_page(real("kl5ech23kwak")) is None


def test_an_abstract_page_under_the_prose_gate_is_abstract_only():
    """NeuroImage: the abstract alone, 153 words in prose blocks."""
    bad = check_page(real("qjfjp3q5tpk7"))
    assert bad.reason == ABSTRACT_ONLY
    assert bad.detail.endswith("153 words in prose blocks") and 153 < download_check.ARTICLE_WORDS
    assert check_page(real("fi4fhyepaqsc")).reason == ABSTRACT_ONLY


def test_reference_titles_do_not_make_an_abstract_page_an_article():
    """Brain Connectivity: the abstract and 49 reference titles, which read as sentences.
    Without its access box the page is still only the abstract."""
    import re

    page = re.sub(download_check._ACCESS_BOX.pattern.encode(), b"", real("22q7d4w5vmbg"),
                  flags=re.I)
    assert download_check._ACCESS_BOX.search(page.decode("utf-8", "replace")) is None
    bad = check_page(page)
    assert bad is not None and bad.reason == ABSTRACT_ONLY


def test_one_body_heading_is_not_a_body():
    page = real("kmtfrgqccuhz").replace(b"</body>", b"<h2>Introduction</h2></body>", 1)
    assert check_page(page).reason == ABSTRACT_ONLY


def test_page_footers_are_not_prose():
    footer = " ".join(f"Footer note {chr(65 + i)} explains how the society runs its many "
                      f"journals and meetings." for i in range(12))
    page = real("kmtfrgqccuhz").replace(b"</body>", f"<footer><p>{footer}</p></footer></body>"
                                        .encode(), 1)
    assert check_page(page).reason == ABSTRACT_ONLY


ABSTRACT = (
    "The participants rested quietly in the scanner while their brain activity was recorded "
    "for ten minutes, and they were asked to keep their eyes open and to think of nothing in "
    "particular. Each of them then performed a simple working memory task with letters shown "
    "one at a time on a screen in front of them, and they pressed a button when a letter "
    "matched the one shown two steps before. Activity in the frontal cortex rose with the "
    "number of letters that had to be held in mind during the delay. "
)
REFERENCES = "".join(
    f"<li>Author {i}, Other A. A paper on memory. J Neurosci. 200{i};{i}:1-9.</li>"
    for i in range(8))


def synthetic(abstract: str, *, headed: bool, body: str = "") -> bytes:
    if headed:
        top = f"<h2>Abstract</h2><p>{abstract}</p>"
    else:
        top = f'<div class="abstract"><p>{abstract}</p></div>'
    return (f"<html><head><title>A study</title></head><body><h1>A study of memory</h1>"
            f"<div>{top}{body}<h2>References</h2><ol>{REFERENCES}</ol></div></body></html>"
            ).encode()


def test_an_abstract_found_by_its_heading_alone_counts():
    assert check_page(synthetic(ABSTRACT, headed=True)).reason == ABSTRACT_ONLY


def test_body_sections_after_an_abstract_heading_are_the_body_s():
    body = ("<h2>Introduction</h2><p>Working memory holds letters for a few seconds while "
            "people use them.</p><h2>Methods</h2><p>Twenty adults took part in the study.</p>")
    page = synthetic(ABSTRACT, headed=True, body=body)
    assert download_check.page_prose(page).sections == ("introduction", "methods")
    assert check_page(page) is None


def test_an_abstract_takes_three_sentences():
    three = synthetic(ABSTRACT, headed=False)
    assert download_check.page_prose(three).abstract_sentences == 3
    assert check_page(three).reason == ABSTRACT_ONLY
    two = synthetic(ABSTRACT.rsplit(" Activity", 1)[0] + " ", headed=False)
    assert download_check.page_prose(two).abstract_sentences == 2
    assert check_page(two) is None


@pytest.mark.parametrize("sentence", [
    # each dropped by one rule alone: a citing word, two years, too few words
    "The response of visual cortex in healthy humans was modulated by attention, see PubMed.",
    "The response of visual cortex was studied from 2001 to 2003 in healthy adult volunteers.",
    "The extraordinarily comprehensive neuroimaging investigation concluded.",
])
def test_reference_shaped_sentences_are_not_prose(sentence):
    assert download_check.prose_sentences(sentence) == []
    assert download_check.prose_sentences(sentence.replace("PubMed", "them")
                                          .replace("2001 to 2003", "one year to the next")
                                          .replace("investigation concluded",
                                                   "investigation was concluded in the end")
                                          ) != []


def test_a_short_block_holds_no_prose():
    sentence = "The participants rested quietly in the scanner for ten long minutes."
    page = f"<html><body><div><p>{sentence}</p><p>{'word ' * 300}</p></div></body></html>"
    assert download_check.page_prose(page).sentences == 0


def test_a_blank_page_says_so():
    assert check_page(b"\x00  \n").detail == "page is blank"


def test_an_apa_psycnet_record_without_access_is_a_paywall():
    """The notice stands where the article would be. Beside the abstract the page has
    435 words of its citation, the notice and export dialogs."""
    assert check_page(real("3ibpqc3bqayb")).reason == PAYWALL


@pytest.mark.parametrize(
    "article_id",
    [
        # T&F: the abstract and its five authors' biographies, 43-78 words each
        "k4mo4emqukwi",
        # ScienceDirect "Article preview": the abstract and four section snippets
        "2z4oyqth3im3",
    ],
)
def test_biographies_and_preview_snippets_are_not_body_prose(article_id):
    assert download_check.page_prose(real(article_id)).body_words < download_check.BODY_PROSE_WORDS
    assert check_page(real(article_id)).reason == ABSTRACT_ONLY


def test_cookie_box_copyright_and_disclaimer_beside_an_abstract_are_not_a_body():
    """Karger: 321 words of them, the most of any stored page that is only an abstract."""
    found = download_check.page_prose(real("ur4v7562a2cz"))
    assert 300 < found.body_words < download_check.BODY_PROSE_WORDS
    assert check_page(real("ur4v7562a2cz")).reason == ABSTRACT_ONLY


def test_a_reference_list_after_the_abstract_heading_is_not_the_abstract():
    """Karger: the references follow the Abstract heading with no heading between, so
    their titles were read as the abstract's 19th and 20th sentences."""
    found = download_check.page_prose(real("upk5ukfiwn33"))
    assert (found.abstract_sentences, found.reference_sentences, found.sentences) == (18, 2, 20)
    assert check_page(real("upk5ukfiwn33")).reason == ABSTRACT_ONLY


@pytest.mark.parametrize(
    "block",
    [
        # the abstract again, five times over, as a page's hidden copies and teasers
        "".join(f"<div><p>Version {i}. {ABSTRACT}</p></div>" for i in range(5)),
        # a run of keywords with no stop
        "<div><p>" + "working memory task frontal cortex letters delay " * 60 + "</p></div>",
        # menu items in title case
        "<div><p>" + "Browse The Journal. Current Issue And Archive. " * 60 + "</p></div>",
        # reference entries: a year among names and titles
        "".join(f"<div><p>Smith A and Brown B reported in Brain Research {1990 + i} that the "
                f"effect of Load holds in Memory. Jones C and the Others found it again.</p>"
                f"</div>" for i in range(20)),
    ],
)
def test_text_that_is_not_running_prose_is_not_body_prose(block):
    page = synthetic(ABSTRACT, headed=False, body=block)
    assert download_check.page_prose(page).body_words == 0
    assert check_page(page).reason == ABSTRACT_ONLY


def test_running_prose_beside_an_abstract_is_body_prose():
    body = "".join(f"<div><p>In study {i} the volunteers held letters in mind for a few "
                   f"seconds. They then named the letters aloud, and the delay was varied "
                   f"from one trial to the next.</p></div>" for i in range(16))
    page = synthetic(ABSTRACT, headed=False, body=body)
    assert download_check.page_prose(page).body_words >= download_check.BODY_PROSE_WORDS
