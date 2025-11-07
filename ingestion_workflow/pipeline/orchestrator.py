"""Pipeline orchestrator coordinating ingestion stages."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from ingestion_workflow.config import Settings, get_settings
from ingestion_workflow.extractors import (
    Extractor,
    ExtractorContext,
    ExtractorRegistry,
    register_builtin_extractors,
)
from ingestion_workflow.logging_utils import get_logger
from ingestion_workflow.models import Identifier, IdentifierSet, ProcessedPaper, RunManifest
from ingestion_workflow.neurostore import NeurostoreClient
from ingestion_workflow.processors.runner import run_processing
from ingestion_workflow.storage import StorageManager

from .state import ManifestStore
from .gather import PubMedGatherer
from .provenance import ProvenanceLogger, ProvenanceEntry

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
    skip_processing_on_dry_run: bool = True


@dataclass
class PipelineContext:
    """Holds shared runtime state for the pipeline."""

    settings: Settings
    storage: StorageManager
    extractor_registry: ExtractorRegistry
    manifest_store: ManifestStore
    neurostore_client: NeurostoreClient
    provenance_logger: ProvenanceLogger
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
        provenance_path = self.storage.data_root / "provenance_log.json"
        self.provenance_logger = ProvenanceLogger(provenance_path)

        self.context = PipelineContext(
            settings=self.settings,
            storage=self.storage,
            extractor_registry=self.extractor_registry,
            manifest_store=self.manifest_store,
            neurostore_client=self.neurostore_client,
            provenance_logger=self.provenance_logger,
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
        remaining: Dict[str, Identifier] = {
            identifier.hash_id: identifier for identifier in identifiers.identifiers
        }
        for extractor in self.context.extractors:
            if not remaining:
                break
            eligible = [
                identifier for identifier in remaining.values() if extractor.supports(identifier)
            ]
            eligible_count = len(eligible)
            if not eligible_count:
                continue
            attempted_entries = self._provenance_entries(eligible)
            logger.info(
                "Extractor %s preparing download batch",
                extractor.name,
                extra={
                    "extractor": extractor.name,
                    "eligible_identifiers": eligible_count,
                },
            )
            if self.config.dry_run:
                for identifier in eligible:
                    logger.info(
                        "Dry run: would download",
                        extra={"extractor": extractor.name, "hash_id": identifier.hash_id},
                    )
                logger.info(
                    "Dry run: extractor %s would attempt %d identifiers.",
                    extractor.name,
                    eligible_count,
                )
                continue
            try:
                batch_results = extractor.download(eligible)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(
                    "Extractor batch failed; trying next extractor",
                    extra={"extractor": extractor.name, "error": str(exc)},
                )
                self.context.provenance_logger.record_batch(
                    extractor.name,
                    ProvenanceEntry(
                        attempted=attempted_entries,
                        coordinate_successes=[],
                        missing_coordinates=[],
                        passed_to_next=attempted_entries,
                        notes={"error": str(exc)},
                    ),
                )
                continue
            success_count = 0
            coordinate_success_entries: List[Dict[str, str]] = []
            missing_coordinate_entries: List[Dict[str, str]] = []
            for result in batch_results:
                hash_id = result.identifier.hash_id
                has_coordinates = result.extra_metadata.get("coordinates_found", True)
                provenance_entry = {
                    "hash_id": hash_id,
                    "label": self._identifier_label(result.identifier),
                }
                if has_coordinates:
                    coordinate_success_entries.append(provenance_entry)
                else:
                    missing_coordinate_entries.append(provenance_entry)
                if (
                    not has_coordinates
                    and self.settings.retry_on_missing_coordinates
                    and hash_id in remaining
                ):
                    logger.info(
                        "Extractor produced no coordinates; passing identifier to next extractor",
                        extra={"extractor": extractor.name, "hash_id": hash_id},
                    )
                    continue
                manifest.register_download(result)
                remaining.pop(hash_id, None)
                success_count += 1
            logger.info(
                "Extractor %s attempted %d identifiers; %d succeeded.",
                extractor.name,
                eligible_count,
                success_count,
                extra={
                    "extractor": extractor.name,
                    "eligible_identifiers": eligible_count,
                    "successful_downloads": success_count,
                },
            )
            passed_to_next_entries = self._provenance_entries(
                [identifier for identifier in eligible if identifier.hash_id in remaining]
            )
            self.context.provenance_logger.record_batch(
                extractor.name,
                ProvenanceEntry(
                    attempted=attempted_entries,
                    coordinate_successes=coordinate_success_entries,
                    missing_coordinates=missing_coordinate_entries,
                    passed_to_next=passed_to_next_entries,
                ),
            )

        for identifier in remaining.values():
            if self.config.dry_run:
                continue
            identifier_label = identifier.available_ids[0] if identifier.available_ids else identifier.hash_id
            logger.error(
                "All extractors failed for identifier %s",
                identifier_label,
                extra={"hash_id": identifier.hash_id},
            )

    @staticmethod
    def _identifier_label(identifier: Identifier) -> str:
        for value in (identifier.pmcid, identifier.pmid, identifier.doi, *identifier.other_ids):
            if value:
                return value
        return identifier.hash_id

    def _provenance_entries(self, identifiers: Iterable[Identifier]) -> List[Dict[str, str]]:
        return [
            {
                "hash_id": identifier.hash_id,
                "label": self._identifier_label(identifier),
            }
            for identifier in identifiers
        ]

    def process_stage(self, manifest: RunManifest) -> List[ProcessedPaper]:
        if self.config.dry_run and self.config.skip_processing_on_dry_run:
            logger.info("Dry run: skipping processing stage.")
            return []
        logger.info("Starting processing stage")
        processed = run_processing(manifest, self.storage)
        logger.info("Processing stage completed", extra={"count": len(processed)})
        return processed

    def upload_stage(self, manifest: RunManifest) -> None:
        if self.config.dry_run:
            logger.info("Dry run: skipping upload stage.")
            return
        logger.info("Upload stage placeholder - no implementation yet.")

    def sync_stage(self, manifest: RunManifest) -> None:
        logger.info("Sync stage placeholder - no implementation yet.")
