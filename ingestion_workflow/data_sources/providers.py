"""Each catalogue's records of imaging data sources, as `Record`s.

A record is kept only when what the catalogue says of it names an imaging modality and, for
the general catalogues, the brain. Its names
and acronyms are given tiers by `scraped_aliases`.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, Iterable, List

from .merge import Record, curated_records, scraped_aliases, split

MRI = re.compile(r"\b(?:f?MRI|magnetic resonance imaging|DTI|diffusion (?:tensor|weighted)|"
                 r"connectom\w*|neuroimaging|brain imaging)\b", re.I)
PET = re.compile(r"\b(?:PET|positron emission)\b", re.I)
NEURO = re.compile(r"\b(?:brain|neuro\w*|cogniti\w*|psychiatr\w*|mental|cerebr\w*)", re.I)
#: SciCrunch resource types, as the data-source kinds neurostore keeps.
SCICRUNCH_KINDS = {
    "consortium": "consortium",
    "data set": "study",
    "image collection": "study",
    "biobank": "study",
}


def modalities(*texts: str) -> List[str]:
    """The modalities `texts` name; neuroimaging in general reads as MRI."""
    text = " ".join(texts)
    return [m for m, pattern in (("MRI", MRI), ("PET", PET)) if pattern.search(text)]


def re3data(client) -> List[Record]:
    out = []
    for re3data_id, repo in client.repositories():
        names = repo.get("repositoryName") or []
        if not names:
            continue
        said = [*names, *repo.get("keyword", []), *repo.get("description", []),
                *repo.get("subject", [])]
        found = modalities(*said)
        if not found or not NEURO.search(" ".join(said)):
            continue
        additional = repo.get("additionalName", [])
        out.append(Record(
            provider="re3data", record_id=re3data_id, name=names[0],
            aliases=scraped_aliases([*names, *(n for n in additional if " " in n)],
                                    [n for n in additional if " " not in n]),
            kinds=["repository"], modalities=found,
            url=(repo.get("repositoryURL") or [""])[0], re3data_ids=[re3data_id]))
    return out


def _named(values, key="name") -> List[str]:
    return [v[key] for v in values or [] if isinstance(v, dict) and v.get(key)]


def scicrunch(client) -> List[Record]:
    out = []
    for item in client.resources():
        rrid, name = item.get("identifier") or "", (item.get("name") or "").strip()
        if not rrid or not name:
            continue
        types = _named(item.get("types"))
        said = [name, item.get("description") or "", *_named(item.get("synonyms")),
                *_named(item.get("keywords"), "keyword")]
        found = modalities(*said)
        if not found or not NEURO.search(" ".join(said)):
            continue
        out.append(Record(
            provider="scicrunch", record_id=rrid, name=name,
            aliases=scraped_aliases([name, *_named(item.get("synonyms"))],
                                    _named(item.get("abbreviations"))),
            kinds=sorted({SCICRUNCH_KINDS.get(t, "repository") for t in types}),
            modalities=found, rrids=[rrid]))
    return out


def jpnd(client) -> List[Record]:
    out = []
    for cohort in client.cohorts():
        found = modalities(cohort["brain_imaging"])
        if not found:
            continue
        out.append(Record(
            provider="jpnd", record_id=f"jpnd_{cohort['slug']}", name=cohort["name"],
            aliases=scraped_aliases([cohort["name"]], split(cohort["acronym"])),
            kinds=["study"], modalities=found,
            url=cohort["website"] or cohort["link"]))
    return out


def pullers(settings) -> Dict[str, Callable[[], Iterable[Record]]]:
    """{provider: the function pulling its records}, in merge order. A provider whose
    credentials are not set is left out."""
    from ingestion_workflow.clients.jpnd import JpndClient
    from ingestion_workflow.clients.re3data import Re3dataClient
    from ingestion_workflow.clients.scicrunch import SciCrunchClient

    found = {
        "curated": curated_records,
        "re3data": lambda: re3data(Re3dataClient()),
        "scicrunch": lambda: scicrunch(SciCrunchClient(settings.scicrunch_api_key)),
        "jpnd": lambda: jpnd(JpndClient()),
    }
    if not getattr(settings, "scicrunch_api_key", None):
        del found["scicrunch"]
    return found
