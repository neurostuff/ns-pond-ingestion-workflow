"""Retraction, erratum and other notices PubMed links to a paper."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ingestion_workflow.utils.doi import find_doi

#: PubMed's `CommentsCorrections/@RefType` for a notice about this paper, mapped
#: to study_schema's `CorrectionKind`. The "...Of" forms say this paper is the
#: notice, so they are not corrections of it.
CORRECTION_KINDS = {
    "RetractionIn": "retraction",
    "ErratumIn": "erratum",
    "ExpressionOfConcernIn": "expression_of_concern",
    "CommentIn": "comment",
    "UpdateIn": "update",
}

Correction = Dict[str, Optional[str]]


class PartialAnswer(RuntimeError):
    """A batched lookup failed part-way; `found` holds what earlier batches returned."""

    def __init__(self, message: str, found: Dict[str, Any]) -> None:
        super().__init__(message)
        self.found = found


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _text(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        value = value.get("#text")
    return str(value) if value else None


def corrections_from_pubmed(article: Dict[str, Any]) -> Tuple[List[Correction], bool]:
    """Read the notices on one efetch `PubmedArticle` (xmltodict form).

    Returns `(corrections, is_retraction_notice)`; each correction is
    `{"kind", "pmid", "doi", "source"}`, the doi taken from the notice's `RefSource`
    citation when it carries one.
    """
    citation = article.get("MedlineCitation") or {}
    listed = (citation.get("CommentsCorrectionsList") or {}).get("CommentsCorrections")
    corrections: List[Correction] = []
    notice = False
    for ref in _as_list(listed):
        if not isinstance(ref, dict):
            continue
        ref_type = ref.get("@RefType")
        if ref_type == "RetractionOf":
            notice = True
        kind = CORRECTION_KINDS.get(ref_type)
        if kind is None:
            continue
        corrections.append(
            {
                "kind": kind,
                "pmid": _text(ref.get("PMID")),
                "doi": find_doi(_text(ref.get("RefSource"))),
                "source": "pubmed",
            }
        )
    # A retracted paper whose notice PubMed has not linked still says so here.
    types = ((citation.get("Article") or {}).get("PublicationTypeList") or {}).get(
        "PublicationType"
    )
    names = {_text(t) for t in _as_list(types)}
    if "Retracted Publication" in names and not any(
        c["kind"] == "retraction" for c in corrections
    ):
        corrections.append({"kind": "retraction", "pmid": None, "doi": None, "source": "pubmed"})
    return corrections, notice or "Retraction Notice" in names


def retraction_of(corrections: List[Correction]) -> Optional[Correction]:
    """The first retraction listed, which is what neurostore keeps as the notice."""
    return next((c for c in corrections if c.get("kind") == "retraction"), None)


def openalex_retraction() -> Correction:
    """The retraction OpenAlex's `is_retracted` reports; it names no notice."""
    return {"kind": "retraction", "pmid": None, "doi": None, "source": "openalex"}
