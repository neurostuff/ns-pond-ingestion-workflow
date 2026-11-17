"""NLM's list of the journals MEDLINE indexes (J_Medline): each title's abbreviations, NLM id
and ISSNs. Updated daily; no key or fee (https://www.nlm.nih.gov/databases/download/terms_and_conditions.html)."""

from __future__ import annotations

import requests

from ingestion_workflow.utils.http import retry_transient

J_MEDLINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt"


@retry_transient(4, 16)
def j_medline() -> str:
    """J_Medline.txt as NLM serves it now (about 9 MB)."""
    response = requests.get(J_MEDLINE_URL, timeout=120)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text
