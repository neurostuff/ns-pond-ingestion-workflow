"""The JPND Global Cohort Portal (https://neurodegenerationresearch.eu/cohorts/): cohorts of
neurodegeneration studies, each a WordPress post, read through the WordPress API."""

from __future__ import annotations

import html
import re
from typing import Iterator

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient

URL = "https://neurodegenerationresearch.eu/wp-json/wp/v2/posts"
COHORTS = 285
PAGE = 100


def _field(lines, label: str) -> str:
    for line in lines:
        if line.lower().startswith(label.lower() + " "):
            return line[len(label):].strip()
    return ""


def parse(post: dict) -> dict:
    """{id, slug, name, acronym, types, brain_imaging, website, link} of one cohort's post."""
    rendered = post["content"]["rendered"]
    body = html.unescape(re.sub(r"<[^>]+>", "", re.sub(r"<br\s*/?>|</p>", "\n", rendered)))
    lines = [re.sub(r"\s+", " ", line).strip() for line in body.split("\n")]
    site = re.search(r'website\s*<a href="([^"]+)"', rendered)
    return {
        "id": post["id"],
        "slug": post["slug"],
        "name": html.unescape(post["title"]["rendered"]).strip(),
        "acronym": _field(lines, "cohort acronym"),
        "types": _field(lines, "Types"),
        "brain_imaging": _field(lines, "Brain imaging"),
        "website": site.group(1) if site else "",
        "link": post["link"],
    }


class JpndClient:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "ns-pond-ingestion-workflow (https://github.com/neurostuff/ns-pond-ingestion-workflow)")
        self._pace = Pacer()

    @retry_transient(4, 16)
    def _page(self, page: int) -> requests.Response:
        self._pace.wait(1)
        response = self._session.get(URL, timeout=60, params={
            "categories": COHORTS, "per_page": PAGE, "page": page,
            "_fields": "id,slug,link,title,content"})
        response.raise_for_status()
        return response

    def cohorts(self) -> Iterator[dict]:
        page, pages = 1, 1
        while page <= pages:
            response = self._page(page)
            pages = int(response.headers.get("X-WP-TotalPages") or 1)
            yield from (parse(post) for post in response.json())
            page += 1
