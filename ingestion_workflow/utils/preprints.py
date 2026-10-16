"""The preprint servers, as one registry: a DOI pattern and the hosts of each.

A DOI is a preprint server's when a pattern matches all of its canonical form.
Prefix alone is not enough: Cold Spring Harbor Laboratory journals share
bioRxiv's 10.1101, and eLife's reviewed preprints are journal articles.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

from ingestion_workflow.utils.doi import normalize_doi


@dataclass(frozen=True)
class PreprintServer:
    name: str
    #: Regexes matched against the whole canonical (lower-case) DOI.
    patterns: Tuple[str, ...]
    #: Hosts whose pages, and those of their subdomains, are this server's.
    hosts: Tuple[str, ...] = ()

    def __post_init__(self):
        object.__setattr__(
            self, "_compiled", tuple(re.compile(p) for p in self.patterns))

    def matches(self, doi: str) -> bool:
        return any(p.fullmatch(doi) for p in self._compiled)


def _osf(name: str, prefix: str, *hosts: str) -> PreprintServer:
    return PreprintServer(name, (rf"10\.{prefix}/osf\.io/[a-z0-9]{{5}}(_v\d+)?",),
                          hosts or ("osf.io",))


#: bioRxiv and medRxiv share 10.1101 and, since 2025-11, 10.64898 (openRxiv); only the
#: length of the numeric suffix tells them apart: 6 digits bioRxiv, 8 medRxiv. The
#: undated numeric form is the legacy one.
#: OSF's providers each have a prefix (Crossref member 15934, less Thesis Commons and
#: Lifecycle Journal, which are not preprint servers) and share one DOI shape; engrXiv
#: (10.31224) is run by another member on the same platform.
PREPRINT_SERVERS: Tuple[PreprintServer, ...] = (
    PreprintServer("biorxiv", (r"10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d{6}|\d{6})",
                               r"10\.64898/\d{4}\.\d{2}\.\d{2}\.\d{6}"), ("biorxiv.org",)),
    PreprintServer("medrxiv", (r"10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d{8}|\d{8})",
                               r"10\.64898/\d{4}\.\d{2}\.\d{2}\.\d{8}"), ("medrxiv.org",)),
    PreprintServer(
        "arxiv", (r"10\.48550/arxiv\.(\d{4}\.\d{4,5}|[a-z-]+(\.[a-z]{2})?/\d{7})",),
        ("arxiv.org",)),
    _osf("osf", "31219"),
    _osf("arabixiv", "31221"),
    _osf("bitss", "31222"),
    _osf("engrxiv", "31224"),
    _osf("focusarchive", "31225"),
    _osf("frenxiv", "31226"),
    _osf("ina-rxiv", "31227"),
    _osf("lawarxiv", "31228"),
    _osf("lis-scholarship-archive", "31229"),
    _osf("marxiv", "31230"),
    _osf("mindrxiv", "31231"),
    _osf("nutrixiv", "31232"),
    _osf("paleorxiv", "31233"),
    _osf("psyarxiv", "31234", "osf.io", "psyarxiv.com"),
    _osf("socarxiv", "31235"),
    _osf("sportrxiv", "31236"),
    _osf("africarxiv", "31730"),
    _osf("banglarxiv", "32943"),
    _osf("mediarxiv", "33767"),
    _osf("bodoarxiv", "34055"),
    _osf("edarxiv", "35542"),
    _osf("biohackrxiv", "37044"),
    PreprintServer("researchsquare", (r"10\.21203/rs\.\d+\.(rs-)?\d+/v\d+",),
                   ("researchsquare.com",)),
    PreprintServer("ssrn", (r"10\.2139/ssrn\.\d+",), ("ssrn.com",)),
    PreprintServer("preprints_org", (r"10\.20944/preprints\d{6}\.\d{4}\.v\d+",),
                   ("preprints.org",)),
    PreprintServer("authorea", (r"10\.22541/au\.\d+\.\d+(/v\d+)?",)),
    PreprintServer("techrxiv", (r"10\.36227/techrxiv\.\d+(\.v\d+)?",)),
    PreprintServer("chemrxiv", (r"10\.26434/chemrxiv[.-][a-z0-9.-]+",)),
    PreprintServer("jmir_preprints", (r"10\.2196/preprints\.\d+",)),
)


def preprint_server(doi: Optional[str]) -> Optional[str]:
    """The name of the preprint server `doi` belongs to, or None."""
    canonical = normalize_doi(doi)
    if canonical is None:
        return None
    return next((s.name for s in PREPRINT_SERVERS if s.matches(canonical)), None)


def is_preprint_doi(doi: Optional[str]) -> bool:
    return preprint_server(doi) is not None


def is_preprint_host(host: str) -> bool:
    """Whether `host`, as a URL's lower-case netloc, is a preprint server or a subdomain of one."""
    return any(host == h or host.endswith("." + h) for s in PREPRINT_SERVERS for h in s.hosts)
