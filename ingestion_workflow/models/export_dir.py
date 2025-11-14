"""Dataclasses mirroring exported article directory layouts for any extractor."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

from pyarty import Dir, File, bundle, twig

from .analysis import AnalysisCollection, CreateAnalysesResult
from .download import DownloadSource
from .extract import ArticleExtractionBundle, ExtractedContent, ExtractedTable
from .ids import Identifier
from .metadata import ArticleMetadata


# --------------------------------------------------------------------------- #
# Generic file primitives
# --------------------------------------------------------------------------- #
@dataclass
class TextFile:
    """Plain-text file stored relative to an article root."""

    rel_path: Path
    content: str = ""

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        path = root / self.rel_path
        if path.exists() and not overwrite:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.content, encoding="utf-8")

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "TextFile":
        path = root / rel_path
        return cls(rel_path=rel_path, content=path.read_text(encoding="utf-8"))


@dataclass
class BinaryFile:
    """Binary file stored relative to an article root."""

    rel_path: Path
    data: bytes = b""

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        path = root / self.rel_path
        if path.exists() and not overwrite:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.data)

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "BinaryFile":
        path = root / rel_path
        return cls(rel_path=rel_path, data=path.read_bytes())


@dataclass
class JsonFile:
    """JSON file persisted relative to the article root."""

    rel_path: Path
    data: Any = field(default_factory=dict)

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        path = root / self.rel_path
        if path.exists() and not overwrite:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.data, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "JsonFile":
        path = root / rel_path
        return cls(rel_path=rel_path, data=json.loads(path.read_text(encoding="utf-8")))


@dataclass
class JsonLinesFile:
    """JSON Lines file that stores records line-by-line."""

    rel_path: Path
    records: list[Any] = field(default_factory=list)

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        path = root / self.rel_path
        if path.exists() and not overwrite:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = "\n".join(json.dumps(record) for record in self.records)
        payload = f"{serialized}\n" if serialized else ""
        path.write_text(payload, encoding="utf-8")

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "JsonLinesFile":
        path = root / rel_path
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return cls(rel_path=rel_path, records=[json.loads(line) for line in lines])


def _sanitize_table_id(table_id: str | None, index: int) -> str:
    """Sanitize identifiers shared across processed exports."""
    if table_id:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", table_id).strip("-")
        if normalized:
            return normalized.lower()
    return f"table-{index + 1}"


def _unique_stem(base: str, used: set[str], index: int) -> str:
    """Ensure filenames do not collide."""
    stem = base
    if stem in used:
        stem = f"{stem}-{index}"
    while stem in used:
        stem = f"{stem}-{index + 1}"
    used.add(stem)
    return stem


# --------------------------------------------------------------------------- #
# Typed JSON helpers
# --------------------------------------------------------------------------- #
@dataclass
class IdentifierFile:
    """Wrapper around identifiers.json contents."""

    rel_path: Path = Path("identifiers.json")
    identifier: Identifier | None = None

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        if self.identifier is None:
            raise ValueError("identifier is required before saving identifiers.json")
        payload = self.identifier.to_dict()
        JsonFile(rel_path=self.rel_path, data=payload).save(root, overwrite=overwrite)

    @classmethod
    def load(cls, root: Path, rel_path: Path = Path("identifiers.json")) -> "IdentifierFile":
        data = JsonFile.load(root, rel_path).data
        return cls(rel_path=rel_path, identifier=Identifier.from_dict(data))


@dataclass
class ArticleDataFile:
    """Persisted ExtractedContent (article_data.json)."""

    rel_path: Path
    content: ExtractedContent

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        JsonFile(rel_path=self.rel_path, data=self.content.to_dict()).save(root, overwrite=overwrite)

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "ArticleDataFile":
        data = JsonFile.load(root, rel_path).data
        return cls(rel_path=rel_path, content=ExtractedContent.from_dict(data))


@dataclass
class ArticleMetadataFile:
    """Persisted ArticleMetadata (article_metadata.json)."""

    rel_path: Path
    metadata: ArticleMetadata

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        JsonFile(rel_path=self.rel_path, data=self.metadata.to_dict()).save(root, overwrite=overwrite)

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "ArticleMetadataFile":
        data = JsonFile.load(root, rel_path).data
        return cls(rel_path=rel_path, metadata=ArticleMetadata.from_dict(data))


@dataclass
class TablesIndexFile:
    """tables.json manifest of ExtractedTable entries."""

    rel_path: Path
    tables: list[ExtractedTable] = field(default_factory=list)

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        payload = [table.to_dict() for table in self.tables]
        JsonFile(rel_path=self.rel_path, data=payload).save(root, overwrite=overwrite)

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "TablesIndexFile":
        data = JsonFile.load(root, rel_path).data or []
        tables = [ExtractedTable.from_dict(item) for item in data]
        return cls(rel_path=rel_path, tables=tables)


@dataclass
class AnalysisFile:
    """Single analysis_collection JSON export."""

    rel_path: Path
    collection: AnalysisCollection

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        JsonFile(rel_path=self.rel_path, data=self.collection.to_dict()).save(root, overwrite=overwrite)

    @classmethod
    def load(cls, root: Path, rel_path: Path) -> "AnalysisFile":
        data = JsonFile.load(root, rel_path).data
        return cls(rel_path=rel_path, collection=AnalysisCollection.from_dict(data))


# --------------------------------------------------------------------------- #
# Processed/source directory mirrors
# --------------------------------------------------------------------------- #
def _relative_under(base: Path, rel_path: Path, name: str) -> Path:
    """Ensure a rel_path lives under a base directory."""
    try:
        rel_path.relative_to(base)
        return rel_path
    except ValueError:
        return base / name


@dataclass
class ProcessedExtractorTree:
    """Processed/<source> tree for any download source."""

    source: DownloadSource | str
    bundle: ArticleExtractionBundle
    tables_index: list[ExtractedTable] = field(default_factory=list)
    table_files: dict[str, TextFile] = field(default_factory=dict)
    analyses: dict[str, AnalysisFile] = field(default_factory=dict)

    @property
    def base_dir(self) -> Path:
        source_name = self.source.value if isinstance(self.source, DownloadSource) else self.source
        return Path("processed") / source_name

    @property
    def source_name(self) -> str:
        return self.source.value if isinstance(self.source, DownloadSource) else self.source

    @classmethod
    def load(cls, root: Path, source_name: str) -> "ProcessedExtractorTree":
        base_dir = Path("processed") / source_name
        article_data = ArticleDataFile.load(root, base_dir / "article_data.json")
        article_metadata = ArticleMetadataFile.load(root, base_dir / "article_metadata.json")
        bundle = ArticleExtractionBundle(
            article_data=article_data.content,
            article_metadata=article_metadata.metadata,
        )

        tables_index: list[ExtractedTable] = []
        tables_index_path = root / base_dir / "tables.json"
        if tables_index_path.exists():
            tables_index = TablesIndexFile.load(root, base_dir / "tables.json").tables
            bundle.article_data.tables = list(tables_index)

        tables_dir = root / base_dir / "tables"
        table_files: dict[str, TextFile] = {}
        if tables_dir.exists():
            for file_path in sorted(p for p in tables_dir.iterdir() if p.is_file()):
                rel = file_path.relative_to(root)
                table_files[file_path.name] = TextFile.load(root, rel)

        analyses_dir = root / base_dir / "analyses"
        analysis_files: dict[str, AnalysisFile] = {}
        if analyses_dir.exists():
            for file_path in sorted(analyses_dir.glob("*.jsonl")):
                rel = file_path.relative_to(root)
                analysis_files[file_path.name] = AnalysisFile.load(root, rel)

        source_enum: DownloadSource | str
        try:
            source_enum = DownloadSource(source_name)
        except ValueError:
            source_enum = source_name

        return cls(
            source=source_enum,
            bundle=bundle,
            tables_index=tables_index,
            table_files=table_files,
            analyses=analysis_files,
        )

    @classmethod
    def from_bundle(
        cls,
        bundle: ArticleExtractionBundle,
        analyses: Sequence[CreateAnalysesResult] | None = None,
    ) -> "ProcessedExtractorTree":
        source_name = bundle.article_data.source.value
        tables_index = list(bundle.article_data.tables)
        table_files: dict[str, TextFile] = {}
        for index, table in enumerate(bundle.article_data.tables):
            raw_path = table.raw_content_path
            if not raw_path or not raw_path.exists():
                continue
            suffix = Path(raw_path).suffix or ".html"
            sanitized = _sanitize_table_id(table.table_id, index)
            filename = f"{sanitized}{suffix}"
            rel = Path("processed") / source_name / "tables" / filename
            table_files[filename] = TextFile(
                rel_path=rel,
                content=raw_path.read_text(encoding="utf-8"),
            )

        analysis_files: dict[str, AnalysisFile] = {}
        if analyses:
            used_names: set[str] = set()
            filtered = [
                result
                for result in analyses
                if result.article_slug == bundle.article_data.slug
            ]
            for index, result in enumerate(filtered):
                base = result.sanitized_table_id or result.table_id
                sanitized = _sanitize_table_id(base, index)
                stem = _unique_stem(sanitized, used_names, index)
                rel = Path("processed") / source_name / "analyses" / f"{stem}.jsonl"
                analysis_files[rel.name] = AnalysisFile(
                    rel_path=rel,
                    collection=result.analysis_collection,
                )

        return cls(
            source=bundle.article_data.source,
            bundle=bundle,
            tables_index=tables_index,
            table_files=table_files,
            analyses=analysis_files,
        )

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        base_dir = self.base_dir
        (root / base_dir).mkdir(parents=True, exist_ok=True)

        ArticleDataFile(
            rel_path=base_dir / "article_data.json", content=self.bundle.article_data
        ).save(root, overwrite=overwrite)
        ArticleMetadataFile(
            rel_path=base_dir / "article_metadata.json", metadata=self.bundle.article_metadata
        ).save(root, overwrite=overwrite)

        tables_payload = self.tables_index or list(self.bundle.article_data.tables)
        if tables_payload:
            TablesIndexFile(rel_path=base_dir / "tables.json", tables=tables_payload).save(
                root, overwrite=overwrite
            )

        tables_dir = base_dir / "tables"
        for name, text_file in self.table_files.items():
            rel_path = _relative_under(tables_dir, text_file.rel_path, name)
            text_file.rel_path = rel_path
            text_file.save(root, overwrite=overwrite)

        if self.analyses:
            analyses_dir = base_dir / "analyses"
            for name, file in self.analyses.items():
                rel_path = _relative_under(analyses_dir, file.rel_path, name)
                file.rel_path = rel_path
                file.save(root, overwrite=overwrite)

    def to_pyarty_bundle(self) -> "_ProcessedSourceBundle":
        tables_manifest = self.tables_index or list(self.bundle.article_data.tables)
        tables_payload: list[dict[str, Any]] | None = None
        if tables_manifest:
            tables_payload = [table.to_dict() for table in tables_manifest]

        table_files = [
            _TableFileBundle(
                filename=Path(text_file.rel_path).name,
                payload=text_file.content.encode("utf-8"),
            )
            for _, text_file in sorted(self.table_files.items())
        ]

        analysis_files = [
            _AnalysisFileBundle(
                filename=Path(analysis.rel_path).name,
                collection=json.dumps(analysis.collection.to_dict(), indent=2).encode("utf-8"),
            )
            for _, analysis in sorted(self.analyses.items())
        ]

        return _ProcessedSourceBundle(
            source_name=self.source_name,
            article_data=self.bundle.article_data.to_dict(),
            article_metadata=self.bundle.article_metadata.to_dict(),
            tables_index=tables_payload,
            tables=table_files,
            analyses=analysis_files,
        )


@dataclass
class ExtractorSourceTree:
    """Source/<source> tree for any extracted bundle."""

    source: DownloadSource | str
    files: dict[str, BinaryFile] = field(default_factory=dict)

    @property
    def base_dir(self) -> Path:
        source_name = self.source.value if isinstance(self.source, DownloadSource) else self.source
        return Path("source") / source_name

    @property
    def source_name(self) -> str:
        return self.source.value if isinstance(self.source, DownloadSource) else self.source

    @classmethod
    def load(cls, root: Path, source_name: str) -> "ExtractorSourceTree":
        base_dir = Path("source") / source_name
        base_path = root / base_dir
        files: dict[str, BinaryFile] = {}
        if base_path.exists():
            for file_path in sorted(p for p in base_path.iterdir() if p.is_file()):
                rel = file_path.relative_to(root)
                files[file_path.name] = BinaryFile.load(root, rel)

        source_enum: DownloadSource | str
        try:
            source_enum = DownloadSource(source_name)
        except ValueError:
            source_enum = source_name

        return cls(source=source_enum, files=files)

    @classmethod
    def from_bundle(cls, bundle: ArticleExtractionBundle) -> "ExtractorSourceTree":
        source = bundle.article_data.source
        source_name = source.value
        files: dict[str, BinaryFile] = {}
        full_text_path = bundle.article_data.full_text_path
        if full_text_path and full_text_path.exists():
            rel = Path("source") / source_name / full_text_path.name
            files[rel.name] = BinaryFile(rel_path=rel, data=full_text_path.read_bytes())
        return cls(source=source, files=files)

    def save(self, root: Path, *, overwrite: bool = True) -> None:
        base_dir = self.base_dir
        (root / base_dir).mkdir(parents=True, exist_ok=True)
        for name, file in self.files.items():
            rel_path = _relative_under(base_dir, file.rel_path, name)
            file.rel_path = rel_path
            file.save(root, overwrite=overwrite)

    def to_pyarty_bundle(self) -> "_SourceBundle":
        entries = [
            _BinaryFileBundle(
                filename=Path(file.rel_path).name,
                payload=file.data,
            )
            for _, file in sorted(self.files.items())
        ]
        return _SourceBundle(source_name=self.source_name, files=entries)


# --------------------------------------------------------------------------- #
# High-level entry point
# --------------------------------------------------------------------------- #
@dataclass
class ArticleDirectory:
    """Represents the entire exported folder for a single article."""

    root_name: str
    identifier: Identifier
    processed: dict[str, ProcessedExtractorTree] = field(default_factory=dict)
    sources: dict[str, ExtractorSourceTree] = field(default_factory=dict)

    @property
    def root_path(self) -> Path:
        return Path(self.root_name)

    @classmethod
    def load(cls, base_root: Path, root_name: str) -> "ArticleDirectory":
        root = base_root / root_name
        identifier_file = IdentifierFile.load(root)
        if identifier_file.identifier is None:
            raise ValueError(f"identifiers.json missing identifier data under {root}")

        processed: dict[str, ProcessedExtractorTree] = {}
        processed_root = root / "processed"
        if processed_root.exists():
            for directory in sorted(p for p in processed_root.iterdir() if p.is_dir()):
                processed[directory.name] = ProcessedExtractorTree.load(root, directory.name)

        sources: dict[str, ExtractorSourceTree] = {}
        source_root = root / "source"
        if source_root.exists():
            for directory in sorted(p for p in source_root.iterdir() if p.is_dir()):
                sources[directory.name] = ExtractorSourceTree.load(root, directory.name)

        return cls(
            root_name=root_name,
            identifier=identifier_file.identifier,
            processed=processed,
            sources=sources,
        )

    @classmethod
    def from_bundle(
        cls,
        bundle: ArticleExtractionBundle,
        analyses: Sequence[CreateAnalysesResult] | None = None,
    ) -> "ArticleDirectory":
        identifier = bundle.article_data.identifier
        if identifier is None:
            raise ValueError("Cannot build ArticleDirectory without an identifier")
        source_name = bundle.article_data.source.value
        processed_tree = ProcessedExtractorTree.from_bundle(bundle, analyses or [])
        source_tree = ExtractorSourceTree.from_bundle(bundle)
        sources = {source_name: source_tree} if source_tree.files else {}
        return cls(
            root_name=identifier.slug,
            identifier=identifier,
            processed={source_name: processed_tree},
            sources=sources,
        )

    def _to_pyarty_bundle(self) -> "_ArticleExportBundle":
        processed_entries = [
            tree.to_pyarty_bundle()
            for _, tree in sorted(self.processed.items(), key=lambda item: item[0])
        ]
        source_entries = [
            tree.to_pyarty_bundle()
            for _, tree in sorted(self.sources.items(), key=lambda item: item[0])
        ]
        identifier_payload = self.identifier.to_dict()
        return _ArticleExportBundle(
            identifier=identifier_payload,
            processed_sources=processed_entries,
            sources=source_entries,
        )

    def save(self, base_root: Path, *, overwrite: bool = True) -> None:
        root = base_root / self.root_name
        if root.exists():
            if not overwrite:
                return
            shutil.rmtree(root)

        bundle = self._to_pyarty_bundle()
        bundle.write(root, overwrite=True)


def _constant_name(value: str) -> Callable[[str, Any, Any, int | None], str]:
    def _name(field_name: str, child: Any, owner: Any, index: int | None = None) -> str:
        return value

    return _name


@bundle
class _TableFileBundle:
    filename: str
    payload: File[bytes] = twig(name="{filename}")


@bundle
class _AnalysisFileBundle:
    filename: str
    collection: File[bytes] = twig(name="{filename}")


@bundle
class _ProcessedSourceBundle:
    source_name: str
    article_data: File[dict[str, Any]] = twig(name=_constant_name("article_data"))
    article_metadata: File[dict[str, Any]] = twig(name=_constant_name("article_metadata"))
    tables_index: File[list[dict[str, Any]]] = twig(
        name=_constant_name("tables"),
        extension="json",
        default=None,
    )
    tables: Dir[list[_TableFileBundle]] = twig(
        name=_constant_name("tables"),
        default_factory=list,
    )
    analyses: Dir[list[_AnalysisFileBundle]] = twig(
        name=_constant_name("analyses"),
        default_factory=list,
    )


@bundle
class _BinaryFileBundle:
    filename: str
    payload: File[bytes] = twig(name="{filename}")


@bundle
class _SourceBundle:
    source_name: str
    files: Dir[list[_BinaryFileBundle]] = twig(
        name=_constant_name(""),
        default_factory=list,
    )


@bundle
class _ArticleExportBundle:
    identifier: File[dict[str, Any]] = twig(name=_constant_name("identifiers"))
    processed_sources: Dir[list[_ProcessedSourceBundle]] = twig(
        prefix="processed",
        name=("{source_name}", "field"),
    )
    sources: Dir[list[_SourceBundle]] = twig(
        prefix="source",
        name=("{source_name}", "field"),
    )


__all__ = [
    "AnalysisFile",
    "ArticleDataFile",
    "ArticleDirectory",
    "ArticleMetadataFile",
    "BinaryFile",
    "ExtractorSourceTree",
    "IdentifierFile",
    "JsonFile",
    "JsonLinesFile",
    "ProcessedExtractorTree",
    "TablesIndexFile",
    "TextFile",
]
