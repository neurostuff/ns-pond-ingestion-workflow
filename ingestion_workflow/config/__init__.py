"""
Configuration for the ingestion workflow.
There are three levels of configuration in order of priority
1. cli options
2. yaml config file
3. environment variables
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ingestion_workflow.models import (
    DownloadSource,
)


class NeurostoreEnv(str, Enum):
    """Which Neurostore deployment to talk to."""

    STAGING = "staging"
    DEV = "dev"
    PRODUCTION = "production"


#: Infrastructure that has to move together when the environment changes.
#: Docker names containers `<compose-project>-<service>-<index>`, so the
#: container name and its network are deployment-specific and cannot be
#: guessed from the hostname.
#:
#: staging and dev are observed from `docker ps` on neurostore.xyz. production
#: is derived from store/docker-compose.yml -- an unprefixed `store` project on
#: the external `nginx-proxy` network -- and has NOT been verified against the
#: live host. Confirm before uploading to it.
NEUROSTORE_PROFILES: Dict[str, Dict[str, Any]] = {
    NeurostoreEnv.STAGING.value: {
        "upload_ssh_host": "neurostore.xyz",
        "upload_ssh_user": "jdkent",
        "upload_remote_bind_host": "neurostore-staging-store-store-pgsql17-1",
        "upload_remote_container_network": "neurostore-staging-store_default",
        "upload_local_forward_port": 6543,
    },
    NeurostoreEnv.DEV.value: {
        "upload_ssh_host": "neurostore.xyz",
        "upload_ssh_user": "jdkent",
        "upload_remote_bind_host": "neurostore-dev-store-store-pgsql17-1",
        "upload_remote_container_network": "neurostore-dev-store_default",
        "upload_local_forward_port": 6544,
    },
    NeurostoreEnv.PRODUCTION.value: {
        "upload_ssh_host": "neurostore.org",
        "upload_ssh_user": "james",
        "upload_remote_bind_host": "store-store-pgsql17-1",
        "upload_remote_container_network": "nginx-proxy",
        "upload_local_forward_port": 6545,
    },
}


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

    catalog_root: Path = Field(
        default=Path("./.catalog"),
        description="Directory holding catalog.sqlite and the blob store",
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

    llm_service_tier: Optional[str] = Field(
        default=None,
        description=(
            "OpenAI service tier for coordinate parsing. 'flex' trades latency "
            "for roughly half the price; leave unset for the default tier"
        ),
    )

    llm_timeout: Optional[float] = Field(
        default=None,
        description=(
            "Per-request timeout in seconds. Flex queues behind spare capacity, "
            "so it needs far longer than the 10 minute default"
        ),
    )

    llm_tier_fallback: bool = Field(
        default=True,
        description=(
            "When the gateway says flex has no capacity, retry the same call "
            "on the default tier instead of losing it"
        ),
    )

    llm_max_retries: Optional[int] = Field(
        default=None,
        description=(
            "Retries for a call that never landed. Flex is capacity-scheduled "
            "and answers 429 when there is none, so it needs more than the "
            "SDK default of 2"
        ),
    )

    llm_reasoning_effort: Optional[str] = Field(
        default=None,
        description=(
            "Reasoning effort to send with coordinate parsing; leave unset for "
            "models that reject the parameter. Reasoning models that refuse "
            "function tools alongside reasoning need 'none'"
        ),
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


    verbose: bool = Field(
        default=False,
        description="Enable verbose logging output",
    )

    dry_run: bool = Field(
        default=False,
        description=("Perform dry run without making external API calls or file changes"),
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
        description="Default identifiers manifest, when none is given on the command line",
    )

    max_attempts: int = Field(
        default=3,
        description="Failed stage attempts before an article is left alone",
    )

    retry_after_hours: int = Field(
        default=24,
        description="Hours to wait before retrying a failed stage attempt",
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
        for directory in (
            self.data_root,
            self.cache_root,
            self.catalog_root,
            self.ns_pond_root,
        ):
            directory.mkdir(parents=True, exist_ok=True)

        # Optionally ensure per-source cache roots exist if configured
        # These are separate from the unified cache_root and are used by
        # specific providers
        for optional_dir in (
            getattr(self, "pubget_cache_root", None),
            getattr(self, "ace_cache_root", None),
            getattr(self, "elsevier_cache_root", None),
            getattr(self, "pdf_cache_root", None),
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
    pdf_cache_root: Optional[Path] = Field(
        default=None,
        description="Optional override for downloaded-PDF cache root directory",
    )
    pdf_url_providers: List[str] = Field(
        default_factory=lambda: ["semantic_scholar", "openalex"],
        description="Ordered providers queried for an open-access PDF URL",
    )
    pdf_extract_workers: Optional[int] = Field(
        default=None,
        description=(
            "Docling worker processes for PDF extraction; None means one per "
            "CUDA device, or 1 on CPU"
        ),
    )

    # ===== Upload configuration =====
    neurostore_env: NeurostoreEnv = Field(
        default=NeurostoreEnv.STAGING,
        description=(
            "Which deployment to upload to. Sets the ssh host, ssh user, "
            "container name, docker network and forward port together; any of "
            "those set explicitly still wins"
        ),
    )
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
        default="neurostore-staging-store-store-pgsql17-1",
        description=(
            "Container name of the Postgres service on the remote host. "
            "Compose namespaces these per deployment, so find yours with "
            "`ssh <host> docker ps --format '{{.Names}}' | grep pgsql`"
        ),
    )
    upload_remote_container_network: str = Field(
        default="neurostore-staging-store_default",
        description=(
            "Docker network to look the remote bind host up on when it is a "
            "container name that does not resolve outside the remote host"
        ),
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


def _read_yaml_mapping(yaml_path: Path) -> Dict[str, Any]:
    if not yaml_path.exists():
        raise FileNotFoundError(f"Settings file not found: {yaml_path}")
    with yaml_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Settings YAML must contain a mapping at the root")
    return data


def _was_given(name: str, yaml_data: Mapping[str, Any], overrides: Mapping[str, Any]) -> bool:
    """Whether the operator named this field, at any layer."""
    return name in overrides or name in yaml_data or name.upper() in os.environ


def environment_profile(
    yaml_data: Mapping[str, Any] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """The profile values for the selected environment, minus anything already given.

    Sits above the hardcoded field defaults and below everything the operator
    set, so `neurostore_env: production` moves all five infrastructure settings
    at once while `upload_local_forward_port: 7000` still takes effect.
    """
    yaml_data = yaml_data or {}
    overrides = overrides or {}
    selected = (
        overrides.get("neurostore_env")
        or yaml_data.get("neurostore_env")
        or os.environ.get("NEUROSTORE_ENV")
        or NeurostoreEnv.STAGING.value
    )
    name = selected.value if isinstance(selected, NeurostoreEnv) else str(selected).lower()
    if name not in NEUROSTORE_PROFILES:
        raise ValueError(
            f"Unknown neurostore_env {name!r}; expected one of "
            f"{', '.join(sorted(NEUROSTORE_PROFILES))}"
        )
    return {
        field: value
        for field, value in NEUROSTORE_PROFILES[name].items()
        if not _was_given(field, yaml_data, overrides)
    }


def load_settings(
    yaml_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Settings:
    """
    Load settings with proper precedence handling.

    Precedence, highest first: overrides (CLI) > YAML > environment >
    environment profile > field defaults.
    """
    overrides = overrides or {}
    yaml_data = _read_yaml_mapping(Path(yaml_path)) if yaml_path is not None else {}

    profile = environment_profile(yaml_data, overrides)
    settings = Settings(**{**profile, **yaml_data})

    if overrides:
        settings = settings.merge_overrides(overrides)

    settings.ensure_directories()
    return settings
