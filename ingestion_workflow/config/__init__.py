"""
Configuration for the ingestion workflow.
There are three levels of configuration in order of priority
1. cli options
2. yaml config file
3. environment variables
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ingestion_workflow.models import (
    DownloadSource,
)


class UploadBehavior(str, Enum):
    UPDATE = "update"
    INSERT_NEW = "insert_new"


class UploadMetadataMode(str, Enum):
    FILL = "fill"
    OVERWRITE = "overwrite"


class Settings(BaseSettings):
    """
    Application configuration with support for:
    - Environment variables)
    - YAML configuration file
    - CLI argument overrides

    Precedence: CLI args > YAML config > Environment variables > Defaults
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ===== Core directories =====
    data_root: Path = Field(
        default=Path("./data"),
        description=("Root directory for workflow data (manifests, staging, etc.)"),
    )
    cache_root: Path = Field(
        default=Path("./.cache"),
        description="Root directory for all cached indices",
    )

    ns_pond_root: Path = Field(
        default=Path("./ns-pond"),
        description=("Local mirror of Neurostore content organized by base study ID"),
    )

    # ===== PubMed configuration =====
    pubmed_email: Optional[str] = Field(
        default=None,
        description=("Contact email for PubMed API (required by NCBI guidelines)"),
        # Accept PUBMED_EMAIL specifically, or fall back to generic EMAIL
        validation_alias=AliasChoices("PUBMED_EMAIL", "EMAIL"),
    )

    pubmed_api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for increased PubMed rate limits",
    )

    pubmed_batch_size: int = Field(
        default=500,
        description="Number of records per PubMed API request",
    )

    # ===== External API configuration =====
    semantic_scholar_api_key: Optional[str] = Field(
        default=None,
        description=("Optional API key for Semantic Scholar (increases rate limits)"),
    )

    openalex_email: Optional[str] = Field(
        default=None,
        description="Contact email for OpenAlex API (enables polite pool)",
        # Accept OPENALEX_EMAIL specifically, or fall back to generic EMAIL
        validation_alias=AliasChoices("OPENALEX_EMAIL", "EMAIL"),
    )

    # ===== Download configuration =====
    download_sources: List[str] = Field(
        # Preserve enum declaration order
        default_factory=lambda: [src.value for src in DownloadSource],
        description="Ordered list of download sources to attempt (enum order)",
    )

    cache_only_mode: bool = Field(
        default=False,
        description=("If True, only use cached downloads and never fetch new content"),
    )

    elsevier_api_key: Optional[str] = Field(
        default=None,
        description="API key for Elsevier full-text access",
    )

    # ===== Elsevier proxy configuration =====
    elsevier_http_proxy: Optional[str] = Field(
        default=None,
        description=(
            "HTTP proxy URL to use for Elsevier requests (e.g., socks5://127.0.0.1:1080)"
        ),
    )
    elsevier_https_proxy: Optional[str] = Field(
        default=None,
        description=(
            "HTTPS proxy URL to use for Elsevier requests (e.g., socks5://127.0.0.1:1080)"
        ),
    )
    elsevier_use_proxy: bool = Field(
        default=False,
        description=("Whether to route Elsevier requests through configured proxy"),
    )

    llm_api_key: Optional[str] = Field(
        default=None,
        description="API key for the configured LLM provider",
    )

    llm_api_base: Optional[str] = Field(
        default=None,
        description="Optional base URL for the configured LLM provider",
    )

    llm_model: str = Field(
        default="gpt-5-mini",
        description="Specific model to use for coordinate extraction",
    )

    export: bool = Field(
        default=False,
        description="Enable exporting extraction outputs to disk mirrors",
    )

    export_overwrite: bool = Field(
        default=True,
        description="Overwrite previously exported files if they exist",
    )

    sync_overwrite: bool = Field(
        default=True,
        description="Overwrite individual files when writing ns-pond sync outputs",
    )

    n_llm_workers: int = Field(
        default=2,
        description="Maximum number of concurrent LLM workers",
    )

    # ===== Neurostore configuration =====
    neurostore_base_url: str = Field(
        default="https://neurostore.org/api",
        description="Base URL for Neurostore REST API",
    )

    neurostore_token: Optional[str] = Field(
        default=None,
        description="Authentication token for Neurostore API",
    )

    neurostore_batch_size: int = Field(
        default=50,
        description="Number of studies to upload in a single batch",
    )

    # ===== PubMed tooling =====
    pubmed_tool: Optional[str] = Field(
        default=None,
        description=(
            "Optional tool name reported to NCBI; if not provided, "
            "API calls may use a library default"
        ),
    )

    # ===== Metadata enrichment configuration =====
    metadata_providers: List[str] = Field(
        default_factory=lambda: ["semantic_scholar", "pubmed", "openalex"],
        description="Ordered list of metadata providers to query",
    )

    # ===== Parallelism configuration =====
    max_workers: int = Field(
        default=4,
        description=("Maximum number of parallel workers for concurrent operations"),
    )

    ace_max_workers: int = Field(
        default=4,
        description=("Maximum number of parallel workers dedicated to ACE downloads"),
    )

    # ===== Behavior flags =====
    force_redownload: bool = Field(
        default=False,
        description="Force re-download even if files exist in cache",
    )

    force_reextract: bool = Field(
        default=False,
        description="Force re-extraction even if results exist in cache",
    )

    ignore_cache_stages: List[str] = Field(
        default_factory=list,
        description="Pipeline stages whose caches should be ignored and regenerated",
    )

    verbose: bool = Field(
        default=False,
        description="Enable verbose logging output",
    )

    dry_run: bool = Field(
        default=False,
        description=("Perform dry run without making external API calls or file changes"),
    )

    stages: List[str] = Field(
        default_factory=lambda: [
            "gather",
            "download",
            "extract",
            "create_analyses",
            "upload",
            "sync",
        ],
        description="Ordered pipeline stages to execute",
    )

    log_to_file: bool = Field(
        default=True,
        description="Persist logs to a file (defaults to <data_root>/logs/pipeline.log)",
    )
    log_to_console: bool = Field(
        default=True,
        description="Emit selected logs to the console in addition to the log file",
    )
    log_file: Optional[Path] = Field(
        default=None,
        description="Optional override for log file path",
    )
    show_progress: bool = Field(
        default=True,
        description="Show tqdm progress bars on the console",
    )

    manifest_path: Optional[Path] = Field(
        default=None,
        description=("Path to an identifiers manifest when gather stage is skipped"),
    )

    use_cached_inputs: bool = Field(
        default=True,
        description="Use cached outputs when prerequisite stages are skipped",
    )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> Settings:
        """
        Load settings from a YAML file.

        The YAML file values will override defaults but can still be
        overridden by environment variables.

        Parameters
        ----------
        yaml_path : Path
            Path to YAML configuration file

        Returns
        -------
        Settings
            Configured settings instance
        """
        if not yaml_path.exists():
            raise FileNotFoundError(f"Settings file not found: {yaml_path}")

        with yaml_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

        if not isinstance(data, dict):
            raise ValueError("Settings YAML must contain a mapping at the root")

        return cls(**data)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Settings:
        """
        Create settings from a dictionary.

        Useful for programmatic configuration or CLI argument overrides.

        Parameters
        ----------
        config_dict : dict
            Dictionary of configuration values

        Returns
        -------
        Settings
            Configured settings instance
        """
        return cls(**config_dict)

    def merge_overrides(self, overrides: Dict[str, Any]) -> Settings:
        """
        Create a new Settings instance with specific values overridden.

        Parameters
        ----------
        overrides : dict
            Dictionary of values to override (typically from CLI args)

        Returns
        -------
        Settings
            New settings instance with overrides applied
        """
        overrides = overrides or {}
        if not overrides:
            return self

        return self.model_copy(update=overrides)

    def ensure_directories(self) -> None:
        """
        Create all required directories if they don't exist.

        Should be called during initialization to ensure the workspace
        is properly set up.
        """
        # Always ensure the core directories exist
        for directory in (self.data_root, self.cache_root, self.ns_pond_root):
            directory.mkdir(parents=True, exist_ok=True)

        # Optionally ensure per-source cache roots exist if configured
        # These are separate from the unified cache_root and are used by
        # specific providers
        for optional_dir in (
            getattr(self, "pubget_cache_root", None),
            getattr(self, "ace_cache_root", None),
            getattr(self, "elsevier_cache_root", None),
        ):
            if isinstance(optional_dir, Path):
                optional_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_dir(self, cache_type: str) -> Path:
        """
        Get the cache directory for a specific cache type.

        Parameters
        ----------
        cache_type : str
            Type of cache (e.g., 'downloads', 'ids', 'metadata', 'extraction')

        Returns
        -------
        Path
            Path to the specific cache directory
        """
        cache_path = self.cache_root / cache_type
        cache_path.mkdir(parents=True, exist_ok=True)
        return cache_path

    # ===== Provider-specific cache roots =====
    # These default to None; if provided via env or YAML, they'll be used by
    # provider-specific services to store their own caches.
    pubget_cache_root: Optional[Path] = Field(
        default=None,
        description="Optional override for Pubget cache root directory",
    )
    ace_cache_root: Optional[Path] = Field(
        default=None,
        description="Optional override for ACE cache root directory",
    )
    elsevier_cache_root: Optional[Path] = Field(
        default=None,
        description="Optional override for Elsevier cache root directory",
    )

    # ===== Upload configuration =====
    upload_use_ssh: bool = Field(
        default=True,
        description="Enable SSH tunneling for upload database connections",
    )
    upload_ssh_host: str = Field(
        default="neurostore.xyz",
        description="SSH host for tunneling to the remote database",
    )
    upload_ssh_user: str = Field(
        default="jdkent",
        description="SSH user for tunneling to the remote database",
    )
    upload_ssh_key: Path = Field(
        default=Path("~/.ssh/id_ed25519"),
        description="Path to SSH private key for tunneling",
    )
    upload_remote_bind_host: str = Field(
        default="store-store-pgsql17-1",
        description="Remote bind host inside SSH tunnel (container hostname)",
    )
    upload_remote_bind_port: int = Field(
        default=5432,
        description="Remote bind port for the Postgres service",
    )
    upload_local_forward_port: int = Field(
        default=6543,
        description="Local port to forward to the remote database via SSH",
    )
    upload_db_host: str = Field(
        default="localhost",
        description="Database host used by SQLAlchemy (often localhost when tunneling)",
    )
    upload_db_name: str = Field(
        default="neurostore",
        description="Database name for uploads",
    )
    upload_db_user: str = Field(
        default="postgres",
        description="Database user for uploads",
    )
    upload_db_password: str = Field(
        default="example",
        description="Database password for uploads",
    )
    upload_connect_timeout: int = Field(
        default=30,
        description="Connection timeout (seconds) for upload database sessions",
    )
    upload_behavior: UploadBehavior = Field(
        default=UploadBehavior.UPDATE,
        description=(
            "Behavior for existing studies with source 'llm': "
            "'update' to modify in place, 'insert_new' to create a new study"
        ),
    )
    upload_metadata_only: bool = Field(
        default=False,
        description="When true, only update metadata fields without touching coordinates",
    )
    upload_metadata_mode: UploadMetadataMode = Field(
        default=UploadMetadataMode.FILL,
        description="Metadata update strategy: 'fill' (only empty fields) or 'overwrite'",
    )


def load_settings(
    yaml_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Settings:
    """
    Load settings with proper precedence handling.

    Precedence order (highest to lowest):
    1. Overrides (typically from CLI args)
    2. YAML config file
    3. Environment variables
    4. Defaults

    Parameters
    ----------
    yaml_path : Path, optional
        Path to YAML configuration file
    overrides : dict, optional
        Dictionary of override values (typically from CLI)

    Returns
    -------
    Settings
        Configured settings instance
    """
    overrides = overrides or {}

    settings = Settings()

    if yaml_path is not None:
        yaml_settings = Settings.from_yaml(yaml_path)
        settings = settings.merge_overrides(yaml_settings.model_dump())

    if overrides:
        settings = settings.merge_overrides(overrides)

    settings.ensure_directories()
    return settings
