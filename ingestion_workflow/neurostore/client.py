"""HTTP client for interacting with Neurostore's public REST API."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.config import Settings


class NeurostoreError(RuntimeError):
    """Raised when the Neurostore API returns an error."""


class NeurostoreClient:
    """Thin wrapper around Neurostore REST endpoints."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client = httpx.Client(base_url=settings.neurostore_base_url, timeout=60.0)

    def _auth_headers(self) -> Dict[str, str]:
        if not self.settings.neurostore_token:
            return {}
        return {"Authorization": f"Bearer {self.settings.neurostore_token}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def post(self, endpoint: str, payload: Dict[str, object]) -> Dict[str, object]:
        response = self._client.post(
            endpoint,
            json=payload,
            headers=self._auth_headers(),
        )
        if response.status_code >= 400:
            raise NeurostoreError(
                f"Neurostore request failed: {response.status_code} {response.text}"
            )
        return response.json()

    def upload_studies(
        self, studies: Iterable[Dict[str, object]]
    ) -> List[Dict[str, object]]:
        """
        Upload a batch of study payloads to Neurostore.

        Parameters
        ----------
        studies:
            Iterable of payloads ready for the `base-studies` endpoint.
        """
        batch = list(studies)
        if not batch:
            return []
        response = self.post("/base-studies", {"data": batch})
        if not isinstance(response, dict):
            raise NeurostoreError("Unexpected response format from Neurostore")
        return response.get("data", [])

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NeurostoreClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()
