"""Pipeline orchestrator coordinating ingestion stages."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from ingestion_workflow.config import Settings, get_settings
from ingestion_workflow.extractors import (
    Extractor,
    ExtractorContext,
    ExtractorRegistry,
    register_builtin_extractors,
)
from ingestion_workflow.logging_utils import get_logger
from ingestion_workflow.models import Identifier, IdentifierSet, RunManifest
from ingestion_workflow.neurostore import NeurostoreClient
from ingestion_workflow.storage import StorageManager

from .state import ManifestStore
from .gather import PubMedGatherer

logger = get_logger(__name__)


@dataclass
class PipelineConfig:
    """Execution configuration for an ingestion run."""

    sources: Optional[List[str]] = None
    env_file: Optional[str] = ".env"
    resume: bool = False
    limit: Optional[int] = None
    dry_run: bool = False
    search_query: Optional[str] = None


@dataclass
class PipelineContext:
    """Holds shared runtime state for the pipeline."""

    settings: Settings
    storage: StorageManager
    extractor_registry: ExtractorRegistry
    manifest_store: ManifestStore
    neurostore_client: NeurostoreClient
    extractors: List[Extractor] = field(default_factory=list)


class IngestionPipeline:
    """Coordinates the full ingestion workflow."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.settings = get_settings(config.env_file)
        self.storage = StorageManager(self.settings)
        self.storage.prepare()

        self.extractor_registry = ExtractorRegistry()
        register_builtin_extractors(self.extractor_registry)
        self.manifest_store = ManifestStore(self.storage)
        self.neurostore_client = NeurostoreClient(self.settings)

        self.context = PipelineContext(
            settings=self.settings,
            storage=self.storage,
            extractor_registry=self.extractor_registry,
            manifest_store=self.manifest_store,
            neurostore_client=self.neurostore_client,
        )

        # Instantiate extractors for configured sources when available.
        requested_sources = (
            config.sources if config.sources is not None else self.settings.extractor_order
        )
        extractor_context = ExtractorContext(storage=self.storage, settings=self.settings)
        self.context.extractors = self.extractor_registry.instantiate_ordered(
            requested_sources, extractor_context
        )

    def run(self) -> None:
        """Execute the full pipeline."""
        logger.info("Starting ingestion pipeline", extra={"sources": self.config.sources})
        manifest = self._prepare_manifest()

        identifiers = self.gather_identifiers(manifest)
        if not identifiers.identifiers:
            logger.warning("No identifiers to process; exiting.")
            return

        self.download_stage(identifiers, manifest)
        self.process_stage(manifest)
        self.upload_stage(manifest)
        self.sync_stage(manifest)

        self.manifest_store.save(manifest)
        logger.info("Ingestion pipeline completed.")

    def _prepare_manifest(self) -> RunManifest:
        if self.config.resume:
            manifest = self.manifest_store.load_latest()
            if manifest:
                logger.info("Loaded existing manifest for resume")
                return manifest
        manifest = self.manifest_store.create(self.settings)
        logger.info("Created new manifest", extra={"path": str(self.manifest_store.default_path)})
        return manifest

    # --- Stages ---------------------------------------------------------

    def gather_identifiers(self, manifest: RunManifest) -> IdentifierSet:
        """
        Gather identifiers for processing.

        Currently uses the PubMed API when a search query is supplied, falling
        back to identifiers already stored in the manifest.
        """
        identifiers = self._identifiers_from_manifest(manifest)

        if self.config.search_query:
            logger.info(
                "Querying PubMed for identifiers",
                extra={"query": self.config.search_query},
            )
            gatherer = PubMedGatherer(self.settings)
            pmids = gatherer.search(
                self.config.search_query,
                max_results=self.config.limit,
            )
            for pmid in pmids:
                identifier = Identifier(pmid=pmid)
                manifest.register_identifier(identifier)
                identifiers.add(identifier)
        else:
            logger.info("No PubMed query provided; using manifest identifiers only.")

        identifiers.deduplicate()
        if self.config.limit is not None:
            identifiers.identifiers = identifiers.identifiers[: self.config.limit]
        return identifiers

    def _identifiers_from_manifest(self, manifest: RunManifest) -> IdentifierSet:
        identifiers = IdentifierSet()
        for payload in manifest.items.values():
            identifier_payload = payload.get("identifier")
            if not identifier_payload:
                continue
            identifier = Identifier(
                pmid=identifier_payload.get("pmid"),
                doi=identifier_payload.get("doi"),
                pmcid=identifier_payload.get("pmcid"),
                other_ids=tuple(identifier_payload.get("other_ids", [])),
            )
            identifiers.add(identifier)
        return identifiers

    def download_stage(self, identifiers: IdentifierSet, manifest: RunManifest) -> None:
        logger.info("Starting download stage", extra={"count": len(identifiers.identifiers)})
        if not self.context.extractors:
            logger.warning("No extractors registered; skipping download stage.")
            return
        for identifier in identifiers.identifiers:
            for extractor in self.context.extractors:
                if not extractor.supports(identifier):
                    continue
                if self.config.dry_run:
                    logger.info(
                        "Dry run: would download",
                        extra={"extractor": extractor.name, "hash_id": identifier.hash_id},
                    )
                    break
                try:
                    result = extractor.download(identifier)
                    manifest.register_download(result)
                    logger.info(
                        "Downloaded paper",
                        extra={"extractor": extractor.name, "hash_id": identifier.hash_id},
                    )
                    break
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning(
                        "Extractor failed; trying next",
                        extra={
                            "extractor": extractor.name,
                            "hash_id": identifier.hash_id,
                            "error": str(exc),
                        },
                    )
            else:
                logger.error("All extractors failed for identifier", extra={"hash_id": identifier.hash_id})

    def process_stage(self, manifest: RunManifest) -> None:
        logger.info("Processing stage placeholder - no implementation yet.")

    def upload_stage(self, manifest: RunManifest) -> None:
        if self.config.dry_run:
            logger.info("Dry run: skipping upload stage.")
            return
        logger.info("Upload stage placeholder - no implementation yet.")

    def sync_stage(self, manifest: RunManifest) -> None:
        logger.info("Sync stage placeholder - no implementation yet.")
