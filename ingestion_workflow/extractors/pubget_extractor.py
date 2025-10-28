"""Extractor implementation leveraging Pubget to fetch PMC articles."""
from __future__ import annotations

import json
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional

import httpx
import pandas as pd
from lxml import etree

from ingestion_workflow.extractors.base import DownloadError, Extractor
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier

from pubget import ExitCode  # type: ignore  # noqa: E402
from pubget._articles import extract_articles  # type: ignore  # noqa: E402
from pubget._data_extraction import extract_data_to_csv  # type: ignore  # noqa: E402
from pubget._download import download_pmcids  # type: ignore  # noqa: E402
from pubget._utils import article_bucket_from_pmcid  # type: ignore  # noqa: E402

logger = logging.getLogger(__name__)


ID_CONVERTER_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"


@dataclass
class PmcidResolution:
    pmcid: int
    pmcid_str: str
    open_access: bool


class PubMedIdConverter:
    """Resolve PMCID from various identifiers using NCBI conversion service."""

    def __init__(self, email: Optional[str], api_key: Optional[str]) -> None:
        self.email = email
        self.api_key = api_key
        self._client = httpx.Client(timeout=30.0, follow_redirects=True)

    def resolve(self, identifier: Identifier) -> Optional[PmcidResolution]:
        query_ids = []
        if identifier.pmcid:
            pmcid_str = str(identifier.pmcid)
            pmcid_int = self._parse_pmcid(pmcid_str)
            if pmcid_int is not None:
                return PmcidResolution(pmcid_int, f"PMC{pmcid_int}", open_access=True)
        if identifier.pmid:
            query_ids.append(str(identifier.pmid))
        if identifier.doi:
            query_ids.append(identifier.doi)
        for other in identifier.other_ids:
            query_ids.append(other)
        for value in query_ids:
            resolution = self._query_id_converter(value)
            if resolution:
                return resolution
        return None

    def _query_id_converter(self, value: str) -> Optional[PmcidResolution]:
        params = {"format": "json", "ids": value}
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        response = self._client.get(ID_CONVERTER_URL, params=params)
        response.raise_for_status()
        payload = response.json()
        records = payload.get("records", [])
        if not records:
            return None
        record = records[0]
        pmcid_str = record.get("pmcid")
        if not pmcid_str:
            return None
        pmcid_int = self._parse_pmcid(pmcid_str)
        if pmcid_int is None:
            return None
        oa = record.get("oa", "").lower() if isinstance(record.get("oa"), str) else ""
        is_open_access = bool(oa == "true" or record.get("status") == "ok")
        return PmcidResolution(pmcid_int, pmcid_str, is_open_access)

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

    def download(self, identifier: Identifier) -> DownloadResult:
        resolution = self._converter.resolve(identifier.normalized())
        if resolution is None:
            raise DownloadError("Unable to resolve PMCID for identifier.")
        logger.info(
            "Resolved identifier to PMCID",
            extra={"hash_id": identifier.hash_id, "pmcid": resolution.pmcid_str},
        )

        storage_paths = self.context.storage.paths_for(identifier)
        source_dir = storage_paths.source_for(self.name)
        processed_dir = storage_paths.processed_for(self.name)
        source_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        with TemporaryDirectory(dir=self.context.storage.cache_root) as tmp_dir:
            tmp_path = Path(tmp_dir)
            logger.debug("Temporary working directory created", extra={"path": str(tmp_path)})
            articlesets_dir = self._download_articles(resolution.pmcid, tmp_path)
            articles_dir = self._extract_articles(articlesets_dir)
            extracted_dir = self._extract_data(articles_dir)

            coordinates_path = extracted_dir / "coordinates.csv"
            if not coordinates_path.exists():
                raise DownloadError("Coordinate extraction output missing.")
            coordinates_df = pd.read_csv(coordinates_path)
            if coordinates_df.empty:
                raise DownloadError("No coordinate tables detected for article.")

            tables_df = self._load_tables(extracted_dir)
            metadata_record = self._load_metadata(extracted_dir)

            coord_table_ids = {
                str(value) for value in coordinates_df["table_id"].dropna().astype(str)
            }

            # Persist processed artifacts
            coordinates_output = processed_dir.joinpath("coordinates.csv")
            coordinates_df.to_csv(coordinates_output, index=False)
            if tables_df is not None:
                tables_df["has_coordinates"] = tables_df["table_id"].astype(str).isin(coord_table_ids)
                tables_df.to_csv(processed_dir.joinpath("tables.csv"), index=False)
            if metadata_record:
                processed_dir.joinpath("metadata.json").write_text(
                    json.dumps(metadata_record, indent=2), encoding="utf-8"
                )

            article_dir = self._article_directory(articles_dir, resolution.pmcid)
            artifacts = self._persist_source_artifacts(
                article_dir,
                source_dir,
                coord_table_ids,
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
            extra_metadata={"pmcid": resolution.pmcid_str},
        )

    def _download_articles(self, pmcid: int, tmp_path: Path) -> Path:
        logger.info("Downloading article via Pubget", extra={"pmcid": pmcid})
        articlesets_dir, exit_code = download_pmcids(
            [pmcid],
            data_dir=tmp_path,
            api_key=self.context.settings.pubmed_api_key,
        )
        if exit_code not in (ExitCode.COMPLETED, ExitCode.INCOMPLETE):
            raise DownloadError(f"Pubget download failed with exit code {exit_code}")
        self._sanitize_articlesets(Path(articlesets_dir))
        return articlesets_dir

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
