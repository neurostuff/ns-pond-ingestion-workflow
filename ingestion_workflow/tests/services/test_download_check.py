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
