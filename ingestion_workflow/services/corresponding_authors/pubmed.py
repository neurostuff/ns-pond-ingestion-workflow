"""PubMed: the authors whose affiliation prints an email.

PubMed marks no corresponding author, but the email a publisher deposits sits in the
affiliation of the author it belongs to ("Electronic address: ..."), so an author with
one there is taken to be corresponding. Read from the efetch record as xmltodict reads
it, which the provider cache keeps whole.
"""

from __future__ import annotations

from typing import List, Mapping

from study_schema.models.paper_parse import MetadataSource

from ingestion_workflow.models.metadata import CorrespondingAuthor

from .common import authors, emails, squash


def from_pubmed(article: Mapping) -> List[CorrespondingAuthor]:
    citation = article.get("MedlineCitation") or {}
    listed = ((citation.get("Article") or {}).get("AuthorList") or {}).get("Author") or []
    found = []
    for author in listed if isinstance(listed, list) else [listed]:
        if not isinstance(author, Mapping):
            continue
        name = _text(author.get("CollectiveName")) or squash(
            " ".join(_text(author.get(k)) for k in ("ForeName", "LastName") if author.get(k)))
        infos = author.get("AffiliationInfo") or []
        infos = infos if isinstance(infos, list) else [infos]
        found += [(name, email) for email in
                  [e for info in infos if isinstance(info, Mapping)
                   for e in emails(_text(info.get("Affiliation")))][:1]]
    return authors(found, MetadataSource.pubmed)


def _text(value) -> str:
    if isinstance(value, Mapping):
        value = value.get("#text")
    return squash(value if isinstance(value, str) else "")
