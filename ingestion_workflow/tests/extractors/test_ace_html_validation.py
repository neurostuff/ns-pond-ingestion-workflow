"""Which saved pages the ACE download keeps.

A rejected page is deleted, so a false rejection loses an article that was on
disk. Publishers put a reCAPTCHA login widget on article pages; only a page
that is the challenge should count as one.
"""

from ingestion_workflow.extractors.ace_extractor import _validate_downloaded_html

ARTICLE_TEXT = " ".join(["Participants viewed alcohol cues during fMRI scanning."] * 400)


def page(tmp_path, html):
    path = tmp_path / "page.html"
    path.write_text(html, encoding="utf-8")
    return _validate_downloaded_html(path)


def test_an_article_with_a_recaptcha_login_widget_is_kept(tmp_path):
    """Wiley's article pages carry `password-recaptcha-ajax`."""
    html = (
        "<html><head><title>Redefining working memory | Eur J Neurosci</title>"
        '<script src="https://www.gstatic.com/recaptcha/releases/x/recaptcha__en.js"></script>'
        "</head><body><h2>Abstract</h2><p>" + ARTICLE_TEXT + "</p>"
        '<div class="password-recaptcha-ajax"></div>'
        "<table><tr><td>-42</td><td>18</td><td>30</td></tr></table></body></html>"
    )
    assert page(tmp_path, html) == (True, None)


def test_a_challenge_page_is_rejected(tmp_path):
    html = (
        "<html><head><title>Checking your browser - reCAPTCHA</title></head><body>"
        "Checking your browser before accessing pmc.ncbi.nlm.nih.gov ... "
        + "<!-- padding -->" * 60 + "</body></html>"
    )
    ok, reason = page(tmp_path, html)
    assert not ok and "CAPTCHA" in reason


def test_a_page_with_little_but_a_captcha_is_rejected(tmp_path):
    html = (
        "<html><head><title>Journal</title><script>" + "var x=1;" * 200 + "</script></head>"
        '<body><div class="g-recaptcha"></div>Please confirm you are human.</body></html>'
    )
    ok, reason = page(tmp_path, html)
    assert not ok and "CAPTCHA" in reason
