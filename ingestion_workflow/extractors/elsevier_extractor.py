"""Extractor implementation leveraging Elsevier ScienceDirect full-text API."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Sequence, Dict, Optional

import httpx
import pandas as pd
from lxml import etree

from ingestion_workflow.extractors.base import DownloadError, Extractor
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier

from elsevier_coordinate_extraction.cache import FileCache
from elsevier_coordinate_extraction.client import ScienceDirectClient
from elsevier_coordinate_extraction.download.api import download_articles
from elsevier_coordinate_extraction.extract.coordinates import (
    extract_coordinates,
    _manual_extract_tables,
)
from elsevier_coordinate_extraction.types import ArticleContent
from elsevier_coordinate_extraction.settings import get_settings

logger = logging.getLogger(__name__)


class ElsevierExtractor(Extractor):
    """Download Elsevier/ScienceDirect articles and extract coordinate tables."""

    name = "elsevier"

    def __init__(self, context):
        super().__init__(context)
        self._api_settings = get_settings()

    def supports(self, identifier: Identifier) -> bool:
        normalized = identifier.normalized()
        return bool(normalized.doi or normalized.pmid)

    def download(self, identifiers: Sequence[Identifier]) -> List[DownloadResult]:
        supported: List[tuple[Identifier, dict[str, str]]] = []
        for identifier in identifiers:
            if not self.supports(identifier):
                continue
            normalized = identifier.normalized()
            record: dict[str, str] = {}
            if normalized.doi:
                record["doi"] = normalized.doi
            if normalized.pmid:
                record["pmid"] = normalized.pmid
            supported.append((identifier, record))
        if not supported:
            return []

        results: List[DownloadResult] = []
        article_map: Dict[str, ArticleContent] = {}

        record_lookup: Dict[tuple[str, str], Identifier] = {}
        batch_records = []
        for identifier, record in supported:
            key = (
                record.get("doi", "") or "",
                record.get("pmid", "") or "",
            )
            record_lookup[key] = identifier
            batch_records.append(record)

        try:
            articles = self._download_articles(batch_records)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Elsevier batch download failed; no articles retrieved.",
                extra={"error": str(exc)},
            )
            articles = []

        for article in articles:
            lookup = article.metadata.get("identifier_lookup") or {}
            key = (
                lookup.get("doi", "") or "",
                lookup.get("pmid", "") or "",
            )
            identifier = record_lookup.get(key)
            if identifier:
                article_map[identifier.hash_id] = article

        total_supported = len(supported)
        missing_coordinates: List[str] = []
        download_failures: List[str] = []
        coordinates_found_count = 0
        for identifier, _record in supported:
            article = article_map.get(identifier.hash_id)
            if not article:
                download_failures.append(self._identifier_label(identifier))
                continue
            try:
                result = self._process_article(identifier, article)
                results.append(result)
                if result.extra_metadata.get("coordinates_found"):
                    coordinates_found_count += 1
                else:
                    missing_coordinates.append(
                        result.extra_metadata.get("doi") or self._identifier_label(identifier)
                    )
            except DownloadError as exc:
                logger.warning(
                    "Elsevier extraction failed",
                    extra={"hash_id": identifier.hash_id, "error": str(exc)},
                )
        logger.info(
            "Elsevier extractor summary: downloaded %d/%d (failures=%d), coordinates found=%d, no coordinates=%d",
            len(results),
            total_supported,
            len(download_failures),
            coordinates_found_count,
            len(missing_coordinates),
        )
        if download_failures:
            logger.info(
                "Elsevier extractor missing articles examples: %s",
                download_failures[:5],
            )
        if missing_coordinates:
            logger.info(
                "Elsevier extractor no-coordinate examples: %s",
                missing_coordinates[:5],
            )
        return results

    def _download_articles(self, records: List[dict[str, str]]):
        cache_root = self.context.storage.cache_root
        cache = FileCache(cache_root)
        cfg = self._api_settings

        async def _runner():
            async with ScienceDirectClient(cfg) as client:
                return await download_articles(
                    records,
                    client=client,
                    cache=cache,
                    cache_namespace="articles",
                )

        with self._quiet_httpx():
            return asyncio.run(_runner())

    def _download_article_record(self, record: dict[str, str]) -> Optional[ArticleContent]:
        with self._quiet_httpx():
            try:
                articles = self._download_articles([record])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    logger.warning(
                        "Elsevier API error for record; skipping.",
                        extra={"record": record, "status": exc.response.status_code},
                    )
                return None
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Elsevier download error for record; skipping.",
                    extra={"record": record, "error": str(exc)},
                )
                return None
        return articles[0] if articles else None

    def _process_article(self, identifier: Identifier, article) -> DownloadResult:
        paths = self.context.storage.paths_for(identifier)
        source_dir = paths.source_for(self.name)
        processed_dir = paths.processed_for(self.name)
        source_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        article_path = source_dir / "article.xml"
        article_path.write_bytes(article.payload)

        metadata_payload = self._build_metadata_payload(article, identifier)
        processed_dir.joinpath("metadata.json").write_text(
            json.dumps(metadata_payload, indent=2),
            encoding="utf-8",
        )

        analyses = extract_coordinates([article])
        studies = analyses.get("studyset", {}).get("studies", [])
        coordinates_path = processed_dir / "coordinates.csv"
        analyses_path = processed_dir / "analyses.json"
        analyses_path.write_text(json.dumps(analyses, indent=2), encoding="utf-8")

        coordinate_rows = self._coordinate_rows(studies)
        coordinates_found = bool(coordinate_rows)
        if coordinate_rows:
            pd.DataFrame(coordinate_rows).to_csv(coordinates_path, index=False)
        else:
            empty_columns = [
                "doi",
                "table_id",
                "table_label",
                "analysis_name",
                "x",
                "y",
                "z",
                "space",
            ]
            pd.DataFrame(columns=empty_columns).to_csv(coordinates_path, index=False)

        coordinate_table_ids = {row["table_id"] for row in coordinate_rows if row.get("table_id")}
        self._persist_tables(article.payload, studies, source_dir, coordinate_table_ids)

        artifacts = [
            DownloadArtifact(
                path=article_path,
                kind="article_xml",
                media_type="application/xml",
            ),
            DownloadArtifact(
                path=coordinates_path,
                kind="coordinates_csv",
                media_type="text/csv",
            ),
            DownloadArtifact(
                path=processed_dir / "metadata.json",
                kind="metadata_json",
                media_type="application/json",
            ),
            DownloadArtifact(
                path=analyses_path,
                kind="analyses_json",
                media_type="application/json",
            ),
        ]

        return DownloadResult(
            identifier=identifier,
            source=self.name,
            artifacts=artifacts,
            open_access=False,
            extra_metadata={
                "doi": metadata_payload.get("doi"),
                "pmid": metadata_payload.get("pmid"),
                "coordinates_found": coordinates_found,
            },
        )

    @staticmethod
    def _identifier_label(identifier: Identifier) -> str:
        normalized = identifier.normalized()
        return (
            normalized.doi
            or normalized.pmid
            or normalized.pmcid
            or (normalized.other_ids[0] if normalized.other_ids else None)
            or identifier.hash_id
        )

    @contextmanager
    def _quiet_httpx(self):
        httpx_logger = logging.getLogger("httpx")
        previous_level = httpx_logger.level
        httpx_logger.setLevel(logging.WARNING)
        try:
            yield
        finally:
            httpx_logger.setLevel(previous_level)

    @staticmethod
    def _coordinate_rows(studies: list[dict]) -> List[dict]:
        rows: List[dict] = []
        for study in studies:
            doi = study.get("doi")
            for index, analysis in enumerate(study.get("analyses", []), start=1):
                analysis_name = analysis.get("name") or f"Analysis {index}"
                metadata = analysis.get("metadata", {}) or {}
                table_id = metadata.get("table_id") or f"analysis_{index:03d}"
                table_label = metadata.get("table_label")
                for point in analysis.get("points", []):
                    coords = point.get("coordinates") or [None, None, None]
                    rows.append(
                        {
                            "doi": doi,
                            "table_id": table_id,
                            "table_label": table_label,
                            "analysis_name": analysis_name,
                            "x": coords[0],
                            "y": coords[1],
                            "z": coords[2],
                            "space": point.get("space"),
                        }
                    )
        return rows

    def _persist_tables(
        self,
        payload: bytes,
        studies: list[dict],
        source_dir: Path,
        coordinate_table_ids: set[str],
    ) -> None:
        tables_dir = source_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = source_dir / "tables_manifest.json"

        analyses_tables: list[dict] = []
        for study in studies:
            analyses_tables.extend(study.get("analyses", []))

        manifest: List[dict] = []
        for idx, analysis in enumerate(analyses_tables, start=1):
            metadata = analysis.get("metadata", {}) or {}
            table_id = metadata.get("table_id") or f"analysis_{idx:03d}"
            table_label = metadata.get("table_label") or analysis.get("name")
            raw_xml = metadata.get("raw_table_xml")
            file_stem = f"table_{idx:03d}"
            data_file = None
            if raw_xml:
                data_file = f"{file_stem}.xml"
                tables_dir.joinpath(data_file).write_text(raw_xml, encoding="utf-8")
            info = {
                "table_id": table_id,
                "table_label": table_label,
                "analysis_name": analysis.get("name"),
                "table_data_file": data_file,
                "is_coordinate_table": table_id in coordinate_table_ids,
            }
            manifest.append(info)
            tables_dir.joinpath(f"{file_stem}_info.json").write_text(
                json.dumps(info, indent=2),
                encoding="utf-8",
            )
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def _build_metadata_payload(self, article, identifier: Identifier) -> dict:
        root = etree.fromstring(article.payload)
        ns = {
            "dc": "http://purl.org/dc/elements/1.1/",
            "ce": "http://www.elsevier.com/xml/common/dtd",
            "prism": "http://prismstandard.org/namespaces/basic/2.0/",
        }

        def first_text(paths: Iterable[str]) -> str | None:
            for path in paths:
                values = root.xpath(path, namespaces=ns)
                if not values:
                    continue
                texts = []
                for value in values:
                    if isinstance(value, str):
                        texts.append(value.strip())
                    else:
                        texts.append(" ".join(value.itertext()).strip())
                texts = [text for text in texts if text]
                if texts:
                    return texts[0]
            return None

        title = first_text(["//dc:title"])
        journal = first_text(["//prism:publicationName"])
        cover_date = first_text(
            [
                "//prism:coverDate",
                "//prism:coverDisplayDate",
                "//prism:publicationDate",
            ]
        )
        year = None
        if cover_date:
            digits = "".join(ch for ch in cover_date if ch.isdigit())
            if len(digits) >= 4:
                year = int(digits[:4])

        author_nodes = root.xpath("//ce:author", namespaces=ns)
        authors: List[str] = []
        for node in author_nodes:
            given = " ".join(node.xpath("./ce:given-name/text()", namespaces=ns)).strip()
            surname = " ".join(node.xpath("./ce:surname/text()", namespaces=ns)).strip()
            parts = [part for part in (given, surname) if part]
            if parts:
                authors.append(" ".join(parts))

        abstract = first_text(["//dc:description", "//ce:abstract"])
        keywords = root.xpath("//ce:index-terms//ce:term/text()", namespaces=ns)

        metadata = {
            "doi": article.doi,
            "pmid": identifier.pmid,
            "title": title or "Untitled",
            "authors": "; ".join(authors),
            "journal": journal,
            "publication_year": year,
            "abstract": abstract,
            "keywords": "; ".join(keyword.strip() for keyword in keywords if keyword.strip()),
            "identifier_type": article.metadata.get("identifier_type"),
            "identifier": article.metadata.get("identifier"),
        }
        metadata["source_metadata"] = article.metadata
        return metadata
