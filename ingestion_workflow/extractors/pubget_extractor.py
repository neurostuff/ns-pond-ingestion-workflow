"""Extractor implementation leveraging Pubget to fetch PMC articles."""
from __future__ import annotations

import json
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Sequence

import requests
import pandas as pd
from lxml import etree
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from ingestion_workflow.extractors.base import DownloadError, Extractor
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier

from pubget import ExitCode  # type: ignore  # noqa: E402
from pubget._articles import extract_articles  # type: ignore  # noqa: E402
from pubget._data_extraction import extract_data_to_csv  # type: ignore  # noqa: E402
from pubget._download import download_pmcids  # type: ignore  # noqa: E402
from pubget._utils import article_bucket_from_pmcid  # type: ignore  # noqa: E402

logger = logging.getLogger(__name__)


ID_CONVERTER_URL = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
ID_CONVERTER_MAX_BATCH = 100


@dataclass
class PmcidResolution:
    pmcid: int
    pmcid_str: str
    open_access: bool


@dataclass
class ArticlesetSource:
    """Description of an articleset source file and optional metadata."""

    xml_path: Path
    info_file: Optional[Path] = None


@dataclass
class ArticlesetArtifacts:
    """Holds extracted artifacts for a single articleset download."""

    temp_dir: TemporaryDirectory
    articles_dir: Path
    extracted_dir: Path

    def cleanup(self) -> None:
        self.temp_dir.cleanup()


class PubMedIdConverter:
    """Resolve PMCID from various identifiers using NCBI conversion service."""

    def __init__(self, email: Optional[str], api_key: Optional[str]) -> None:
        self.email = email
        self.api_key = api_key
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "IngestionWorkflow/1.0 (+mailto:jamesdkent21@gmail.com)",
                "Accept": "application/json",
            }
        )
        self._timeout = 30.0

    def resolve(self, identifier: Identifier) -> Optional[PmcidResolution]:
        return self.resolve_many([identifier]).get(identifier.hash_id)

    def resolve_many(self, identifiers: Sequence[Identifier]) -> Dict[str, PmcidResolution]:
        results: Dict[str, PmcidResolution] = {}
        unresolved: Dict[str, Identifier] = {}

        for identifier in identifiers:
            normalized = identifier.normalized()
            if normalized.pmcid:
                pmcid_int = self._parse_pmcid(normalized.pmcid)
                if pmcid_int is not None:
                    results[identifier.hash_id] = PmcidResolution(
                        pmcid=pmcid_int,
                        pmcid_str=f"PMC{pmcid_int}",
                        open_access=True,
                    )
                    continue
            unresolved[identifier.hash_id] = normalized

        if not unresolved:
            return results

        lookup_orders = [
            lambda ident: [ident.pmid] if ident.pmid else [],
            lambda ident: [ident.doi] if ident.doi else [],
            lambda ident: list(ident.other_ids),
        ]

        remaining = dict(unresolved)
        for getter in lookup_orders:
            query_map: Dict[str, List[str]] = {}
            query_values: List[str] = []
            for hash_id, ident in remaining.items():
                candidates = [value for value in getter(ident) if value]
                if not candidates:
                    continue
                value = candidates[0]
                query_values.append(value)
                query_map.setdefault(value, []).append(hash_id)
            if not query_values:
                continue
            responses = self._batch_lookup(query_values)
            resolved_hashes: List[str] = []
            for query_value, resolution in responses.items():
                if not resolution:
                    continue
                for hash_id in query_map.get(query_value, []):
                    results[hash_id] = resolution
                    resolved_hashes.append(hash_id)
            for hash_id in resolved_hashes:
                remaining.pop(hash_id, None)
            if not remaining:
                break
        return results

    def _batch_lookup(self, values: Sequence[str]) -> Dict[str, Optional[PmcidResolution]]:
        responses: Dict[str, Optional[PmcidResolution]] = {}
        if not values:
            return responses
        for start in range(0, len(values), ID_CONVERTER_MAX_BATCH):
            chunk = values[start:start + ID_CONVERTER_MAX_BATCH]
            try:
                chunk_response = self._query_chunk(chunk)
            except (requests.RequestException, RetryError) as exc:
                logger.warning("PMC idconv request failed", extra={"error": str(exc)})
                chunk_response = {}
            responses.update(chunk_response)
        return responses

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        reraise=True,
    )
    def _perform_request(self, ids: Sequence[str]) -> dict:
        params = {
            "format": "json",
            "ids": ",".join(ids),
        }
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        params["tool"] = "ingestion-workflow"
        ordered_params = dict(sorted(params.items()))
        response = self._session.get(ID_CONVERTER_URL, params=ordered_params, timeout=self._timeout)
        params = dict(sorted(params.items()))
        response = self._session.get(ID_CONVERTER_URL, params=params, timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def _query_chunk(self, ids: Sequence[str]) -> Dict[str, Optional[PmcidResolution]]:
        result: Dict[str, Optional[PmcidResolution]] = {value: None for value in ids}
        if not ids:
            return result
        payload = self._perform_request(ids)
        records = payload.get("records", [])
        for record in records:
            query_value = record.get("requested-id") or record.get("id") or record.get("doi")
            pmcid_str = record.get("pmcid")
            if not pmcid_str:
                continue
            pmcid_int = self._parse_pmcid(pmcid_str)
            if pmcid_int is None:
                continue
            oa = record.get("oa", "").lower() if isinstance(record.get("oa"), str) else ""
            is_open_access = bool(oa == "true" or record.get("status") == "ok")
            resolution = PmcidResolution(pmcid_int, pmcid_str, is_open_access)
            if query_value:
                result[query_value] = resolution
            else:
                result[pmcid_str] = resolution
        return result

    @staticmethod
    def _parse_pmcid(value: str) -> Optional[int]:
        cleaned = value.replace("PMC", "").strip()
        return int(cleaned) if cleaned.isdigit() else None


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("_") or "table"


class PubgetExtractor(Extractor):
    """Extractor that downloads PMC articles and filters coordinate tables."""

    name = "pubget"

    def __init__(self, context):
        super().__init__(context)
        self._converter = PubMedIdConverter(
            email=context.settings.pubmed_email,
            api_key=context.settings.pubmed_api_key,
        )

    def supports(self, identifier: Identifier) -> bool:
        normalized = identifier.normalized()
        return bool(
            normalized.pmcid
            or normalized.pmid
            or normalized.doi
            or normalized.other_ids
        )

    def download(self, identifiers: Sequence[Identifier]) -> List[DownloadResult]:
        supported = [identifier for identifier in identifiers if self.supports(identifier)]
        if not supported:
            return []

        resolutions_map = self._converter.resolve_many(supported)

        resolved: List[tuple[Identifier, PmcidResolution]] = []
        unresolved_details: List[tuple[str, str]] = []
        for identifier in supported:
            resolution = resolutions_map.get(identifier.hash_id)
            if resolution is None:
                unresolved_details.append(
                    (identifier.hash_id, self._identifier_label(identifier))
                )
                continue
            resolved.append((identifier, resolution))
        if unresolved_details:
            sample = [label for _, label in unresolved_details[:10]]
            logger.warning(
                "Unable to resolve PMCID for %d identifiers; passing to next extractor. Examples: %s",
                len(unresolved_details),
                sample,
                extra={
                    "hash_ids": [hash_id for hash_id, _ in unresolved_details],
                    "identifier_labels": sample,
                },
            )
        if not resolved:
            return []

        cached_sources: Dict[int, ArticlesetSource] = {}
        missing_pmcids: List[int] = []
        for _, resolution in resolved:
            source = self._locate_cached_articleset(resolution.pmcid)
            if source:
                logger.info(
                    "Using cached Pubget download",
                    extra={"pmcid": resolution.pmcid},
                )
                cached_sources[resolution.pmcid] = source
            else:
                missing_pmcids.append(resolution.pmcid)

        temp_dirs: List[TemporaryDirectory] = []
        downloaded_sources: Dict[int, ArticlesetSource] = {}
        missing_downloads: set[int] = set()
        if missing_pmcids:
            batch_sources, batch_tmp, batch_missing = self._download_articles_batch(missing_pmcids)
            downloaded_sources.update(batch_sources)
            missing_downloads = batch_missing
            if batch_tmp:
                temp_dirs.append(batch_tmp)

        results: List[DownloadResult] = []
        prepared_cache: Dict[Path, ArticlesetArtifacts] = {}
        try:
            for identifier, resolution in resolved:
                source = cached_sources.get(resolution.pmcid) or downloaded_sources.get(
                    resolution.pmcid
                )
                if not source:
                    if resolution.pmcid in missing_downloads:
                        continue
                    logger.warning(
                        "Missing articleset source after download",
                        extra={"pmcid": resolution.pmcid},
                    )
                    continue
                cache_key = source.xml_path.resolve()
                artifacts = prepared_cache.get(cache_key)
                if artifacts is None:
                    artifacts = self._prepare_articleset(source)
                    prepared_cache[cache_key] = artifacts
                try:
                    results.append(
                        self._build_result_from_artifacts(
                            identifier,
                            resolution,
                            artifacts.articles_dir,
                            artifacts.extracted_dir,
                        )
                    )
                except DownloadError as exc:
                    logger.warning(
                        "Pubget download failed",
                        extra={"hash_id": identifier.hash_id, "error": str(exc)},
                    )
        finally:
            for artifacts in prepared_cache.values():
                artifacts.cleanup()
            for tmp_dir in temp_dirs:
                tmp_dir.cleanup()

        return results

    def _prepare_articleset(self, source: ArticlesetSource) -> ArticlesetArtifacts:
        temp_dir = TemporaryDirectory(dir=self.context.storage.cache_root)
        tmp_path = Path(temp_dir.name)
        logger.debug(
            "Preparing articleset working directory",
            extra={"path": str(tmp_path), "xml": str(source.xml_path)},
        )
        articlesets_dir = self._materialize_articleset(source, tmp_path / "articlesets")
        articles_dir = self._extract_articles(articlesets_dir)
        extracted_dir = self._extract_data(articles_dir)
        return ArticlesetArtifacts(temp_dir=temp_dir, articles_dir=articles_dir, extracted_dir=extracted_dir)

    def _build_result_from_artifacts(
        self,
        identifier: Identifier,
        resolution: PmcidResolution,
        articles_dir: Path,
        extracted_dir: Path,
    ) -> DownloadResult:
        storage_paths = self.context.storage.paths_for(identifier)
        source_dir = storage_paths.source_for(self.name)
        processed_dir = storage_paths.processed_for(self.name)
        source_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        coordinates_found = False
        coordinates_path = extracted_dir / "coordinates.csv"
        if not coordinates_path.exists():
            raise DownloadError("Coordinate extraction output missing.")
        coordinates_df = pd.read_csv(coordinates_path)
        coordinates_found = not coordinates_df.empty
        if not coordinates_found:
            logger.info(
                "No coordinate tables detected for article; continuing per configuration.",
                extra={"pmcid": resolution.pmcid},
            )
            coordinate_table_ids = set()
        else:
            coordinate_table_ids = {
                str(value) for value in coordinates_df["table_id"].dropna().astype(str)
            }

        tables_df = self._load_tables(extracted_dir)
        metadata_record = self._load_metadata(extracted_dir)

        # Persist processed artifacts
        coordinates_output = processed_dir.joinpath("coordinates.csv")
        coordinates_df.to_csv(coordinates_output, index=False)
        if tables_df is not None:
            tables_df["has_coordinates"] = tables_df["table_id"].astype(str).isin(
                coordinate_table_ids
            )
            tables_df.to_csv(processed_dir.joinpath("tables.csv"), index=False)
        if metadata_record:
            processed_dir.joinpath("metadata.json").write_text(
                json.dumps(metadata_record, indent=2), encoding="utf-8"
            )

        article_dir = self._article_directory(articles_dir, resolution.pmcid)
        artifacts = self._persist_source_artifacts(
            article_dir,
            source_dir,
            coordinate_table_ids,
        )

        coordinates_file = processed_dir.joinpath("coordinates.csv")
        result_artifacts = [
            DownloadArtifact(
                path=source_dir.joinpath("article.xml"),
                kind="article_xml",
                media_type="application/xml",
            ),
            DownloadArtifact(
                path=coordinates_file,
                kind="coordinates_csv",
                media_type="text/csv",
            ),
        ]
        if processed_dir.joinpath("tables.csv").exists():
            result_artifacts.append(
                DownloadArtifact(
                    path=processed_dir.joinpath("tables.csv"),
                    kind="tables_csv",
                    media_type="text/csv",
                )
            )
        result_artifacts.extend(artifacts)

        return DownloadResult(
            identifier=identifier,
            source=self.name,
            artifacts=result_artifacts,
            open_access=resolution.open_access,
            extra_metadata={
                "pmcid": resolution.pmcid_str,
                "coordinates_found": coordinates_found,
            },
        )

    def _extract_articles(self, articlesets_dir: Path) -> Path:
        logger.info("Extracting article XML and tables")
        articles_dir, exit_code = extract_articles(articlesets_dir)
        if exit_code not in (ExitCode.COMPLETED, ExitCode.INCOMPLETE):
            raise DownloadError(f"Article extraction failed with exit code {exit_code}")
        return articles_dir

    def _extract_data(self, articles_dir: Path) -> Path:
        logger.info("Extracting metadata and coordinates")
        extracted_dir, exit_code = extract_data_to_csv(
            articles_dir,
            articles_with_coords_only=True,
            n_jobs=1,
        )
        if exit_code not in (ExitCode.COMPLETED, ExitCode.INCOMPLETE):
            raise DownloadError(f"Data extraction failed with exit code {exit_code}")
        return extracted_dir

    def _locate_cached_articleset(self, pmcid: int) -> Optional[ArticlesetSource]:
        cache_root = self.context.settings.pubget_cache_root
        if cache_root is None or not cache_root.exists():
            return None
        for articlesets_dir in cache_root.rglob("articlesets"):
            xml_path = self._find_pmcid_in_articlesets(articlesets_dir, pmcid)
            if xml_path is None:
                continue
            info_file = None
            for candidate in (
                articlesets_dir.joinpath("info.json"),
                articlesets_dir.parent.joinpath("info.json"),
            ):
                if candidate.exists():
                    info_file = candidate
                    break
            return ArticlesetSource(xml_path=xml_path, info_file=info_file)
        return None

    def _find_pmcid_in_articlesets(self, articlesets_dir: Path, pmcid: int) -> Optional[Path]:
        for xml_path in sorted(articlesets_dir.glob("articleset_*.xml")):
            if self._xml_contains_pmcid(xml_path, pmcid):
                return xml_path
        return None

    def _download_articles_batch(
        self, pmcids: Sequence[int]
    ) -> tuple[Dict[int, ArticlesetSource], Optional[TemporaryDirectory], set[int]]:
        if not pmcids:
            return {}, None, set()
        batch_tmp = TemporaryDirectory(dir=self.context.storage.cache_root)
        try:
            articlesets_dir, exit_code = download_pmcids(
                list(pmcids),
                data_dir=Path(batch_tmp.name),
                api_key=self.context.settings.pubmed_api_key,
            )
            if exit_code not in (ExitCode.COMPLETED, ExitCode.INCOMPLETE):
                raise DownloadError(f"Pubget download failed with exit code {exit_code}")
            articlesets_path = Path(articlesets_dir)
            info_file = articlesets_path.joinpath("info.json") if articlesets_path else None
            sources: Dict[int, ArticlesetSource] = {}
            missing_in_batch: List[int] = []
            for pmcid in pmcids:
                xml_path = self._find_pmcid_in_articlesets(articlesets_path, pmcid)
                if not xml_path:
                    missing_in_batch.append(pmcid)
                    continue
                sources[pmcid] = ArticlesetSource(
                    xml_path=xml_path,
                    info_file=info_file if info_file and info_file.exists() else None,
                )
            missing_set = set(missing_in_batch)
            if missing_in_batch:
                logger.warning(
                    "Downloaded batch missing %d PMCIDs",
                    len(missing_in_batch),
                    extra={
                        "path": str(articlesets_path),
                        "pmcids": missing_in_batch[:10],
                    },
                )
            if not sources:
                batch_tmp.cleanup()
                return {}, None, missing_set
            return sources, batch_tmp, missing_set
        except Exception:
            batch_tmp.cleanup()
            raise

    def _materialize_articleset(self, source: ArticlesetSource, destination: Path) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        if source.info_file and source.info_file.exists():
            shutil.copy2(source.info_file, destination / "info.json")
        shutil.copy2(source.xml_path, destination / source.xml_path.name)
        self._sanitize_articlesets(destination)
        return destination

    @staticmethod
    def _xml_contains_pmcid(xml_path: Path, pmcid: int) -> bool:
        try:
            tree = etree.parse(str(xml_path))
        except (OSError, etree.XMLSyntaxError):
            return False
        target = str(pmcid)
        for node in tree.iterfind(".//article-id[@pub-id-type='pmcid']"):
            text = (node.text or "").strip()
            if not text:
                continue
            normalized = text.replace("PMC", "").strip()
            if normalized == target:
                return True
        return False

    def _sanitize_articlesets(self, articlesets_dir: Path) -> None:
        """
        Normalize article identifiers to ensure compatibility with pubget's utilities.

        Recent PMC articles use `pub-id-type="pmcid"` with text prefixed by "PMC".
        The historical pubget code expects `pub-id-type="pmc"` with digit-only text.
        This routine rewrites those entries in-place before downstream processing.
        """
        for xml_path in Path(articlesets_dir).glob("articleset_*.xml"):
            try:
                tree = etree.parse(str(xml_path))
            except Exception as exc:  # pragma: no cover - file parse errors are logged
                logger.warning("Failed to parse articleset", extra={"path": str(xml_path), "error": str(exc)})
                continue
            root = tree.getroot()
            updated = False
            for article in root.iterfind("article"):
                for article_id in article.findall("front/article-meta/article-id"):
                    id_type = article_id.get("pub-id-type")
                    if id_type == "pmcid":
                        text = article_id.text or ""
                        if text.upper().startswith("PMC"):
                            article_id.text = text[3:]
                        article_id.set("pub-id-type", "pmc")
                        updated = True
            if updated:
                tree.write(
                    str(xml_path),
                    encoding="utf-8",
                    xml_declaration=True,
                    doctype=tree.docinfo.doctype,
                )

    @staticmethod
    def _load_tables(extracted_dir: Path) -> Optional[pd.DataFrame]:
        tables_path = extracted_dir / "tables.csv"
        if not tables_path.exists():
            return None
        tables_df = pd.read_csv(tables_path)
        if tables_df.empty:
            return None
        return tables_df

    @staticmethod
    def _load_metadata(extracted_dir: Path) -> Optional[Dict[str, object]]:
        metadata_path = extracted_dir / "metadata.csv"
        if not metadata_path.exists():
            return None
        metadata_df = pd.read_csv(metadata_path)
        if metadata_df.empty:
            return None
        record = metadata_df.iloc[0].dropna().to_dict()
        return record

    @staticmethod
    def _article_directory(articles_dir: Path, pmcid: int) -> Path:
        bucket = article_bucket_from_pmcid(pmcid)
        article_dir = articles_dir.joinpath(bucket, f"pmcid_{pmcid}")
        if not article_dir.exists():
            raise DownloadError(f"Article directory {article_dir} missing.")
        return article_dir

    def _persist_source_artifacts(
        self,
        article_dir: Path,
        source_dir: Path,
        coordinate_table_ids: Iterable[str],
    ) -> List[DownloadArtifact]:
        artifacts: List[DownloadArtifact] = []
        article_xml_path = article_dir.joinpath("article.xml")
        if not article_xml_path.exists():
            raise DownloadError("Article XML not found after extraction.")
        shutil.copy2(article_xml_path, source_dir.joinpath("article.xml"))

        tables_source_dir = source_dir.joinpath("tables")
        tables_source_dir.mkdir(parents=True, exist_ok=True)
        tables_xml_path = article_dir.joinpath("tables", "tables.xml")
        tables_tree = None
        if tables_xml_path.exists():
            shutil.copy2(tables_xml_path, tables_source_dir.joinpath("tables.xml"))
            tables_tree = etree.parse(str(tables_xml_path))

        coordinate_ids_set = {str(tid) for tid in coordinate_table_ids}
        manifest: List[Dict[str, object]] = []

        for table_info_json in sorted(article_dir.joinpath("tables").glob("table_*_info.json")):
            info = json.loads(table_info_json.read_text("utf-8"))
            table_stub = table_info_json.stem.replace("_info", "")
            table_id_value = info.get("table_id")
            table_id = str(table_id_value or table_stub)
            has_coordinates = table_id in coordinate_ids_set
            if not has_coordinates:
                continue
            table_name = table_stub
            table_dest_prefix = tables_source_dir.joinpath(table_name)

            shutil.copy2(
                table_info_json,
                tables_source_dir.joinpath(f"{table_name}_info.json"),
            )
            data_file = info.get("table_data_file")
            if data_file:
                table_csv_src = table_info_json.parent.joinpath(data_file)
                if table_csv_src.exists():
                    shutil.copy2(table_csv_src, table_dest_prefix.with_suffix(".csv"))

            xml_snippet = None
            if tables_tree is not None:
                if table_id_value:
                    xml_snippet = self._extract_table_xml(tables_tree, str(table_id_value))
                else:
                    xml_snippet = None
                if xml_snippet is None:
                    index = self._table_index_from_name(table_name)
                    xml_snippet = self._extract_table_by_index(tables_tree, index)
            if xml_snippet is not None:
                xml_name = _sanitize_filename(table_id) if table_id_value else _sanitize_filename(table_name)
                xml_filename = tables_source_dir.joinpath(f"{xml_name}.xml")
                xml_filename.write_bytes(
                    etree.tostring(xml_snippet, encoding="utf-8", pretty_print=True)
                )

            manifest.append(
                {
                    "table_id": table_id,
                    "table_label": info.get("table_label"),
                    "table_caption": info.get("table_caption"),
                    "table_foot": info.get("table_foot"),
                    "table_data_file": info.get("table_data_file"),
                }
            )

        if manifest:
            source_dir.joinpath("tables_manifest.json").write_text(
                json.dumps(manifest, indent=2), encoding="utf-8"
            )

        for table_id in coordinate_ids_set:
            safe_name = _sanitize_filename(table_id)
            xml_path = tables_source_dir.joinpath(f"{safe_name}.xml")
            if xml_path.exists():
                artifacts.append(
                    DownloadArtifact(
                        path=xml_path,
                        kind="table_xml",
                        media_type="application/xml",
                    )
                )
        return artifacts

    @staticmethod
    def _identifier_label(identifier: Identifier) -> str:
        if identifier.pmcid:
            return identifier.pmcid
        if identifier.pmid:
            return identifier.pmid
        if identifier.doi:
            return identifier.doi
        if identifier.other_ids:
            return identifier.other_ids[0]
        return identifier.hash_id

    @staticmethod
    def _extract_table_xml(tables_tree: etree._ElementTree, table_id: str) -> Optional[etree._Element]:
        xpath = f".//extracted-table[table-id='{table_id}']"
        found = tables_tree.find(xpath)
        if found is None:
            logger.debug("Table XML not found", extra={"table_id": table_id})
            return None
        return found

    @staticmethod
    def _extract_table_by_index(
        tables_tree: etree._ElementTree, index: Optional[int]
    ) -> Optional[etree._Element]:
        if index is None or index < 0:
            return None
        tables = tables_tree.findall(".//extracted-table")
        if index >= len(tables):
            return None
        return tables[index]

    @staticmethod
    def _table_index_from_name(table_name: str) -> Optional[int]:
        match = re.search(r"_(\d+)$", table_name)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None
