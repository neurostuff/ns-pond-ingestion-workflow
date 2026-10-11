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
