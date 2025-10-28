"""
Configuration helpers for the ingestion workflow.

Settings are sourced from environment variables and optional `.env` files.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv


class Settings(BaseSettings):
    """Runtime configuration for the ingestion workflow."""

    model_config = SettingsConfigDict(env_prefix="INGEST_", case_sensitive=False)

    data_root: Path = Field(
        default=Path("./data"),
        description="Root directory where workflow data will be staged and stored.",
    )
    cache_root: Path = Field(
        default=Path("./.cache/ingestion_workflow"),
        description="Directory used for cached downloads and metadata lookups.",
    )
    ns_pond_root: Path = Field(
        default=Path("./ns-pond"),
        description="Destination directory that mirrors Neurostore content locally.",
    )

    neurostore_base_url: str = Field(
        default="https://neurostore.org/api",
        description="Base URL for Neurostore public REST API.",
    )
    neurostore_token: str = Field(
        default="",
        description="Authentication token for Neurostore API access.",
    )

    extractor_order: List[str] = Field(
        default_factory=lambda: [
            "pubget",
            "semantic_scholar",
            "elsevier",
            "ace",
        ],
        description="Default priority order for extractor execution.",
    )

    llm_provider: str = Field(
        default="",
        description="Identifier for the LLM provider used in coordinate extraction.",
    )

    pubmed_email: Optional[str] = Field(
        default=None,
        description="Contact email for PubMed API requests (recommended by NCBI).",
    )
    pubmed_api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for PubMed E-utilities.",
    )
    pubmed_retmax: int = Field(
        default=500,
        description="Number of records to request per PubMed page.",
    )
    pubmed_max_results: int = Field(
        default=5000,
        description="Upper bound on total PubMed results to retrieve per query.",
    )

    @field_validator("data_root", "cache_root", "ns_pond_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()


@lru_cache()
def get_settings(env_file: str | None = ".env") -> Settings:
    """
    Load workflow settings.

    Parameters
    ----------
    env_file:
        Optional path to an environment file. When provided and exists it will
        be loaded before reading environment variables.
    """
    if env_file:
        load_dotenv(env_file)
    return Settings()


__all__ = ["Settings", "get_settings"]
