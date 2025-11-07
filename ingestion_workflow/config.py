"""
Configuration helpers for the ingestion workflow.

Settings are sourced from environment variables and optional `.env` files.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Union

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
    pubmed_max_results: Optional[int] = Field(
        default=None,
        description="Upper bound on total PubMed results to retrieve per query.",
    )

    ace_html_root: Path = Field(
        default=Path("./tests/data/test_html"),
        description="Root directory containing HTML files for ACE ingestion.",
    )
    ace_metadata_root: Optional[Path] = Field(
        default=None,
        description="Optional directory for cached PubMed metadata used by ACE.",
    )
    ace_use_readability: bool = Field(
        default=False,
        description="Whether ACE should attempt to use readability-based HTML cleaning.",
    )
    ace_save_table_html: bool = Field(
        default=True,
        description="When True, ACE stores raw table HTML in source/ace/tables.",
    )
    ace_download_mode: str = Field(
        default="browser",
        description="Retrieval mode used by ACE scraper ('browser' or 'requests').",
    )
    ace_prefer_pmc_source: Union[str, bool] = Field(
        default=False,
        description="Hints to ACE scraper for preferring PubMed Central sources ('only' or bool).",
    )
    pubget_cache_root: Optional[Path] = Field(
        default=None,
        description="Optional directory containing cached Pubget outputs to reuse before downloading.",
    )
    semantic_scholar_api_key: Optional[str] = Field(
        default=None,
        description="API key for the Semantic Scholar Graph API.",
    )
    openalex_email: str = Field(
        default="jamesdkent21@gmail.com",
        description="Contact email supplied to OpenAlex API requests.",
    )
    metadata_provider_order: List[str] = Field(
        default_factory=lambda: ["semantic_scholar", "pubmed", "openalex"],
        description="Ordered list of metadata enrichment providers.",
    )
    retry_on_missing_coordinates: bool = Field(
        default=False,
        description=(
            "When True, identifiers with downloaded articles but no coordinate tables "
            "are passed to the next extractor."
        ),
    )

    @field_validator("data_root", "cache_root", "ns_pond_root", "ace_html_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("ace_metadata_root", "pubget_cache_root", mode="before")
    @classmethod
    def _expand_optional_path(cls, value: str | Path | None) -> Path | None:
        if value is None:
            return None
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
