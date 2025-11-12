from collections.abc import MutableMapping
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

import json
import re

DOI_URL = re.compile(r'(?i)https?://[^/\s]+/(10\.\d{4,9}/[^\s"\'<>()]+)')


def _normalize_identifier(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    return value or None


def _normalize_pmid(value: Optional[str]) -> Optional[str]:
    value = _normalize_identifier(value)
    if value and value.startswith("https://pubmed.ncbi.nlm.nih.gov/"):
        prefix = "https://pubmed.ncbi.nlm.nih.gov/"
        value = value.replace(prefix, "").rstrip("/")
    return value or None


def _normalize_pmcid(value: Optional[str]) -> Optional[str]:
    value = _normalize_identifier(value)
    if value and not value.upper().startswith("PMC"):
        value = "PMC" + value
    return value or None


def _normalize_doi(value: Optional[str]) -> Optional[str]:
    value = _normalize_identifier(value)
    if value and value.startswith("http"):
        value = DOI_URL.sub(r"\1", value)
    if value and value.lower().startswith("doi:"):
        value = value[4:]
    return value or None


@dataclass
class Identifier(MutableMapping[str, Optional[str]]):
    neurostore: Optional[str] = None
    pmid: Optional[str] = None
    doi: Optional[str] = None
    pmcid: Optional[str] = None
    other_ids: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        """Ensure identifiers are normalized immediately after creation."""
        self.normalize()

    def hash_identifiers(self) -> str:
        """Create a hashable representation of the identifiers."""
        id_parts = [
            self.pmid or "",
            self.doi or "",
            self.pmcid or "",
        ]
        # replace slashes to avoid path issues
        return "|".join(id_parts).replace("/", "_")

    @property
    def hash_id(self) -> str:
        """Return the current hash of primary identifiers."""
        return self.hash_identifiers()

    def normalize(self) -> None:
        """Normalize identifier fields."""
        self.pmid = _normalize_pmid(self.pmid) if self.pmid else None
        self.doi = _normalize_doi(self.doi) if self.doi else None
        self.pmcid = _normalize_pmcid(self.pmcid) if self.pmcid else None
        if self.other_ids is not None:
            normalized = {
                k: _normalize_identifier(v) for k, v in self.other_ids.items()
            }
            self.other_ids = {
                key: value
                for key, value in normalized.items()
                if value is not None
            }
            if not self.other_ids:
                self.other_ids = None

    # -- MutableMapping protocol ----------------------------------------------

    _PRIMARY_KEYS = {"neurostore", "pmid", "doi", "pmcid"}

    def __getitem__(self, key: str) -> Optional[str]:
        normalized_key = self._normalize_mapping_key(key)
        if normalized_key in self._PRIMARY_KEYS:
            return getattr(self, normalized_key)
        if self.other_ids and normalized_key in self.other_ids:
            return self.other_ids[normalized_key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Optional[str]) -> None:
        normalized_key = self._normalize_mapping_key(key)
        if normalized_key in self._PRIMARY_KEYS:
            setattr(self, normalized_key, value)
            self.normalize()
            return

        if value is None:
            if self.other_ids is not None:
                self.other_ids.pop(normalized_key, None)
                if not self.other_ids:
                    self.other_ids = None
            return

        if self.other_ids is None:
            self.other_ids = {}
        self.other_ids[normalized_key] = str(value)
        self.normalize()

    def __delitem__(self, key: str) -> None:
        normalized_key = self._normalize_mapping_key(key)
        if normalized_key in self._PRIMARY_KEYS:
            setattr(self, normalized_key, None)
            return
        if self.other_ids and normalized_key in self.other_ids:
            self.other_ids.pop(normalized_key, None)
            if not self.other_ids:
                self.other_ids = None
            return
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        for primary in self._PRIMARY_KEYS:
            value = getattr(self, primary)
            if value is not None:
                yield primary
        if self.other_ids:
            for key in self.other_ids:
                yield key

    def __len__(self) -> int:
        count = sum(
            1 for key in self._PRIMARY_KEYS if getattr(self, key) is not None
        )
        if self.other_ids:
            count += len(self.other_ids)
        return count

    def _normalize_mapping_key(self, key: str) -> str:
        if key is None:
            raise KeyError(key)
        key_str = str(key).strip()
        if not key_str:
            raise KeyError(key)
        return key_str.lower()


@dataclass
class Identifiers:
    identifiers: list[Identifier] = field(default_factory=list)
    _index_keys: set[str] = field(default_factory=set, init=False, repr=False)
    _indices: dict[str, dict[str, Identifier]] = field(
        default_factory=dict, init=False, repr=False
    )

    def set_index(self, *keys: str) -> None:
        """Configure one or more identifier fields for fast lookups."""
        if len(keys) == 1 and isinstance(keys[0], (list, tuple, set)):
            keys = tuple(keys[0])  # type: ignore[assignment]

        if not keys:
            self._index_keys = set()
            self._indices = {}
            return

        valid_keys = {"pmid", "pmcid", "doi", "neurostore"}
        normalized_keys = set()
        for key in keys:
            if key not in valid_keys:
                raise ValueError(
                    "Index keys must be drawn from 'pmid', 'pmcid', 'doi', "
                    "or 'neurostore'."
                )
            normalized_keys.add(key)

        self._index_keys = normalized_keys
        self._indices = {key: {} for key in self._index_keys}
        for identifier in self.identifiers:
            self._add_to_indices(identifier)

    def lookup(
        self, value: str, key: Optional[str] = None
    ) -> Optional[Identifier]:
        """Return a matching identifier in constant time."""
        if key is not None and key not in {
            "pmid",
            "pmcid",
            "doi",
            "neurostore",
        }:
            raise ValueError(
                "Lookup key must be one of 'pmid', 'pmcid', 'doi', or "
                "'neurostore'."
            )

        target_key = key
        if target_key is None and len(self._index_keys) == 1:
            target_key = next(iter(self._index_keys))

        if target_key is None:
            raise ValueError(
                "No index configured. Set indices first or provide key."
            )

        if target_key not in self._index_keys:
            new_keys = set(self._index_keys)
            new_keys.add(target_key)
            self.set_index(*new_keys)

        normalized = self._normalize_for_key(target_key, value)
        if normalized is None:
            return None
        return self._indices[target_key].get(normalized)

    def _normalize_for_key(
        self, key: str, value: Optional[str]
    ) -> Optional[str]:
        if value is None:
            return None
        value_str = str(value)
        if key == "pmid":
            return _normalize_pmid(value_str)
        if key == "pmcid":
            return _normalize_pmcid(value_str)
        if key == "doi":
            return _normalize_doi(value_str)
        if key == "neurostore":
            return _normalize_identifier(value_str)
        raise ValueError(f"Unsupported key: {key}")

    def _add_to_indices(self, identifier: Identifier) -> None:
        if not self._index_keys:
            return
        for key in self._index_keys:
            value = getattr(identifier, key)
            if value:
                normalized = self._normalize_for_key(key, value)
                if normalized:
                    self._indices[key][normalized] = identifier

    def _remove_from_indices(self, identifier: Identifier) -> None:
        if not self._index_keys:
            return
        for key in self._index_keys:
            value = getattr(identifier, key)
            if value:
                normalized = self._normalize_for_key(key, value)
                if normalized:
                    self._indices[key].pop(normalized, None)

    def _rebuild_indices(self) -> None:
        if not self._index_keys:
            return
        self._indices = {key: {} for key in self._index_keys}
        for identifier in self.identifiers:
            self._add_to_indices(identifier)

    def deduplicate(self) -> None:
        """Remove duplicate identifiers based on their hash."""
        unique_hashes = set()
        unique_identifiers = []
        for identifier in self.identifiers:
            hash_value = identifier.hash_identifiers()
            if hash_value not in unique_hashes:
                unique_hashes.add(hash_value)
                unique_identifiers.append(identifier)
        self.identifiers = unique_identifiers
        self._rebuild_indices()


    def save(self, file_path: Path | str) -> None:
        """
        Save identifiers to a JSONL file.

        Parameters
        ----------
        file_path : Path or str
            Path to the JSONL file to write
        """
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open("w", encoding="utf-8") as f:
            for identifier in self.identifiers:
                json.dump(asdict(identifier), f)
                f.write("\n")

    @classmethod
    def load(cls, file_path: Path | str) -> "Identifiers":
        """
        Load identifiers from a JSONL file.

        Parameters
        ----------
        file_path : Path or str
            Path to the JSONL file to read

        Returns
        -------
        Identifiers
            Loaded identifiers
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return cls()

        identifiers = []
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    data = json.loads(line)
                    identifiers.append(Identifier(**data))

        return cls(identifiers=identifiers)

    # List-like interface methods
    def __len__(self) -> int:
        """Return the number of identifiers."""
        return len(self.identifiers)

    def __getitem__(
        self, index: int | slice | str
    ) -> Identifier | list[Identifier]:
        """Get identifier(s) by index, slice, or indexed id."""
        if isinstance(index, str):
            result = self.lookup(index)
            if result is None:
                raise KeyError(index)
            return result
        return self.identifiers[index]

    def __setitem__(
        self, index: int | slice, value: Identifier | list[Identifier]
    ) -> None:
        """Set identifier(s) at index, keeping the index in sync."""
        replaced = self.identifiers[index]

        if isinstance(replaced, list):
            for item in replaced:
                self._remove_from_indices(item)
        else:
            self._remove_from_indices(replaced)

        self.identifiers[index] = value

        if isinstance(value, list):
            for item in value:
                self._add_to_indices(item)
        else:
            self._add_to_indices(value)

    def __delitem__(self, index: int | slice) -> None:
        """Delete identifier(s) at index or slice."""
        to_remove = self.identifiers[index]
        if isinstance(to_remove, list):
            for item in to_remove:
                self._remove_from_indices(item)
        else:
            self._remove_from_indices(to_remove)
        del self.identifiers[index]

    def __iter__(self) -> Iterator[Identifier]:
        """Iterate over identifiers."""
        return iter(self.identifiers)

    def __contains__(self, item: Identifier) -> bool:
        """Check if identifier is in the list."""
        return item in self.identifiers

    def append(self, item: Identifier) -> None:
        """Append an identifier to the list."""
        self.identifiers.append(item)
        self._add_to_indices(item)

    def extend(self, items: list[Identifier]) -> None:
        """Extend the list with multiple identifiers."""
        self.identifiers.extend(items)
        for item in items:
            self._add_to_indices(item)

    def insert(self, index: int, item: Identifier) -> None:
        """Insert an identifier at the specified index."""
        self.identifiers.insert(index, item)
        self._add_to_indices(item)

    def remove(self, item: Identifier) -> None:
        """Remove the first occurrence of an identifier."""
        self.identifiers.remove(item)
        self._remove_from_indices(item)

    def pop(self, index: int = -1) -> Identifier:
        """Remove and return identifier at index (default last)."""
        item = self.identifiers.pop(index)
        self._remove_from_indices(item)
        return item

    def clear(self) -> None:
        """Remove all identifiers."""
        self.identifiers.clear()
        self._indices.clear()
        self._index_keys.clear()

    def index(self, item: Identifier, start: int = 0, stop: int = None) -> int:
        """Return index of first occurrence of identifier."""
        if stop is None:
            return self.identifiers.index(item, start)
        return self.identifiers.index(item, start, stop)

    def count(self, item: Identifier) -> int:
        """Return number of occurrences of identifier."""
        return self.identifiers.count(item)

    def reverse(self) -> None:
        """Reverse the list in place."""
        self.identifiers.reverse()

    def sort(self, key=None, reverse: bool = False) -> None:
        """Sort the list in place."""
        self.identifiers.sort(key=key, reverse=reverse)


@dataclass
class IdentifierExpansion:
    """Pipeline payload describing identifier lookup results."""

    seed_identifier: Identifier
    identifiers: Identifiers = field(default_factory=Identifiers)
    sources: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seed_identifier": self.seed_identifier.__dict__.copy(),
            "identifiers": [
                identifier.__dict__.copy()
                for identifier in self.identifiers.identifiers
            ],
            "sources": list(self.sources),
        }

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, Any]
    ) -> "IdentifierExpansion":
        seed_payload = payload.get("seed_identifier", {})
        seed_identifier = Identifier(
            **seed_payload
        )  # type: ignore[arg-type]
        identifiers_payload = payload.get("identifiers", [])
        identifier_list = [
            Identifier(**item) for item in identifiers_payload
        ]
        return cls(
            seed_identifier=seed_identifier,
            identifiers=Identifiers(identifier_list),
            sources=[str(source) for source in payload.get("sources", [])],
        )
