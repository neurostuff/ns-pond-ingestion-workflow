"""The preprint servers, as one registry: a DOI pattern and the hosts of each.

A DOI is a preprint server's when a pattern matches all of its canonical form.
Prefix alone is not enough: Cold Spring Harbor Laboratory journals share
bioRxiv's 10.1101, and eLife's reviewed preprints are journal articles.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlsplit

from ingestion_workflow.utils.doi import normalize_doi


@dataclass(frozen=True)
class PreprintServer:
    name: str
    #: Regexes matched against the whole canonical (lower-case) DOI.
    patterns: Tuple[str, ...]
    #: Hosts whose pages, and those of their subdomains, are this server's.
    hosts: Tuple[str, ...] = ()
    #: Whether everything it holds is a preprint, whichever version a listing labels it.
    preprints_only: bool = True
    #: Its ids as an OSF provider, which name it in osf.io/preprints/<id>/ URLs.
    osf_ids: Tuple[str, ...] = ()

    def __post_init__(self):
        object.__setattr__(
            self, "_compiled", tuple(re.compile(p) for p in self.patterns))

    def matches(self, doi: str) -> bool:
        return any(p.fullmatch(doi) for p in self._compiled)


#: Every OSF provider serves from osf.io; some also from a domain of their own.
OSF = "osf.io"


def _osf(name: str, prefix: str, osf_ids: Tuple[str, ...], *domains: str) -> PreprintServer:
    return PreprintServer(name, (rf"10\.{prefix}/osf\.io/[a-z0-9]{{5}}(_v\d+)?",),
                          (OSF, *domains), preprints_only=False, osf_ids=osf_ids)


#: bioRxiv and medRxiv share 10.1101 and, since 2025-11, 10.64898 (openRxiv); only the
#: length of the numeric suffix tells them apart: 6 digits bioRxiv, 8 medRxiv. The
#: undated numeric form is the legacy one.
#: OSF's providers each have a prefix (Crossref member 15934, less Thesis Commons and
#: Lifecycle Journal, which are not preprint servers) and share one DOI shape; engrXiv
#: (10.31224) is run by another member on the same platform, and its newer DOIs are a bare
#: number (10.31224/4330). JMIR Preprints has a host of its own because jmir.org also
#: serves the journals. PeerJ Preprints (closed 2019) has no host of its own: peerj.com
#: also serves the journals, and 10.7287 also registers their peer reviews. Qeios also
#: publishes articles it calls peer reviewed.
PREPRINT_SERVERS: Tuple[PreprintServer, ...] = (
    PreprintServer("biorxiv", (r"10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d{6}|\d{6})",
                               r"10\.64898/\d{4}\.\d{2}\.\d{2}\.\d{6}"), ("biorxiv.org",)),
    PreprintServer("medrxiv", (r"10\.1101/(\d{4}\.\d{2}\.\d{2}\.\d{8}|\d{8})",
                               r"10\.64898/\d{4}\.\d{2}\.\d{2}\.\d{8}"), ("medrxiv.org",)),
    PreprintServer(
        "arxiv", (r"10\.48550/arxiv\.(\d{4}\.\d{4,5}|[a-z-]+(\.[a-z-]+)?/\d{7})",),
        ("arxiv.org",), preprints_only=False),
    _osf("osf_preprints", "31219", ("osf",)),
    _osf("arabixiv", "31221", ("arabixiv",), "arabixiv.org"),
    _osf("metaarxiv", "31222", ("metaarxiv",)),
    PreprintServer("engrxiv", (r"10\.31224/osf\.io/[a-z0-9]{5}(_v\d+)?", r"10\.31224/\d+"),
                   ("engrxiv.org",), preprints_only=False, osf_ids=("engrxiv",)),
    _osf("focus_archive", "31225", ("focusarchive",)),
    _osf("frenxiv", "31226", ("frenxiv",), "frenxiv.org"),
    _osf("ina_rxiv", "31227", ("inarxiv",)),
    _osf("lawarxiv", "31228", ("lawarxiv", "lawarchive")),
    _osf("lis_scholarship_archive", "31229", ("lissa",)),
    _osf("marxiv", "31230", ("marxiv",), "marxiv.org"),
    _osf("mindrxiv", "31231", ("mindrxiv",), "mindrxiv.org"),
    _osf("nutrixiv", "31232", ("nutrixiv",)),
    _osf("paleorxiv", "31233", ("paleorxiv",), "paleorxiv.org"),
    _osf("psyarxiv", "31234", ("psyarxiv",), "psyarxiv.com"),
    _osf("socarxiv", "31235", ("socarxiv",)),
    _osf("sportrxiv", "31236", ("sportrxiv",)),
    _osf("africarxiv", "31730", ("africarxiv",)),
    _osf("mediarxiv", "33767", ("mediarxiv",), "mediarxiv.org"),
    _osf("bodoarxiv", "34055", ("bodoarxiv",)),
    _osf("edarxiv", "35542", ("edarxiv",), "edarxiv.org"),
    _osf("biohackrxiv", "37044", ("biohackrxiv",), "biohackrxiv.org"),
    PreprintServer("researchsquare", (r"10\.21203/rs\.\d+\.(rs-)?\d+/v\d+",),
                   ("researchsquare.com",)),
    PreprintServer("ssrn", (r"10\.2139/ssrn\.\d+",), ("ssrn.com",), preprints_only=False),
    PreprintServer("preprints_org", (r"10\.20944/preprints\d{6}\.\d{4}\.v\d+",),
                   ("preprints.org",)),
    PreprintServer("authorea", (r"10\.22541/au\.\d+\.\d+(/v\d+)?",),
                   ("authorea.com",)),
    PreprintServer("techrxiv", (r"10\.36227/techrxiv\.\d+(\.v\d+)?",),
                   ("techrxiv.org",)),
    PreprintServer("chemrxiv", (r"10\.26434/chemrxiv[.-][a-z0-9.-]+(/v\d+)?",),
                   ("chemrxiv.org",)),
    PreprintServer("jmir_preprints", (r"10\.2196/preprints\.\d+",),
                   ("preprints.jmir.org",)),
    PreprintServer("peerj_preprints", (r"10\.7287/peerj\.preprints\.\d+(v\d+)?",)),
    PreprintServer("qeios", (r"10\.32388/[a-z0-9]{6}(\.\d+)?",), ("qeios.com",),
                   preprints_only=False),
)


def preprint_server(doi: Optional[str]) -> Optional[str]:
    """The name of the preprint server `doi` belongs to, or None."""
    canonical = normalize_doi(doi)
    if canonical is None:
        return None
    return next((s.name for s in PREPRINT_SERVERS if s.matches(canonical)), None)


def is_preprint_doi(doi: Optional[str]) -> bool:
    return preprint_server(doi) is not None


#: eLife's reviewed preprints and its published articles share 10.7554, so no pattern can
#: tell them apart. Crossref types a reviewed preprint's base DOI `posted-content`; a
#: producer that has read that files the DOI as a preprint's, and only then is it eLife's.
ELIFE_PREFIX = "10.7554/"


def server_of_filed_preprint_doi(doi: str) -> Optional[str]:
    """The server of a DOI already filed as a preprint's: the registry's, else eLife's."""
    canonical = normalize_doi(doi)
    if canonical is not None and canonical.startswith(ELIFE_PREFIX):
        return "elife"
    return preprint_server(canonical)


def _server_of_host(host: str) -> Optional[PreprintServer]:
    return next((s for s in PREPRINT_SERVERS for h in s.hosts
                 if host == h or host.endswith("." + h)), None)


def preprint_server_of_url(url: str, doi: Optional[str] = None) -> Optional[PreprintServer]:
    """The preprint server whose site `url` is on, else None.

    A provider's own domain names it. On osf.io the provider is the one the URL's
    /preprints/<id>/ path names, else the one whose DOI `doi` (the article's) is, else
    OSF Preprints.
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    server = _server_of_host(host)
    if not (host == OSF or host.endswith("." + OSF)):
        return server
    path = parts.path.lower().split("/")
    named = path[2] if len(path) > 2 and path[1] == "preprints" else None
    by_doi = preprint_server(doi)
    return next((s for s in PREPRINT_SERVERS if named in s.osf_ids), None) or next(
        (s for s in PREPRINT_SERVERS if s.osf_ids and s.name == by_doi), server)


def is_preprint_host(host: str) -> bool:
    """Whether `host`, as a URL's lower-case netloc, is a preprint server or a subdomain of one."""
    return _server_of_host(host) is not None
