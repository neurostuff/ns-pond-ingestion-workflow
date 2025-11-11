from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, Iterator

import json

@dataclass
class Identifier:
    pmid: str
    doi: str
    pmcid: str
    other_ids: Optional[dict[str, str]]

    def hash_identifiers(self) -> str:
        """Create a hashable representation of the identifiers."""
        id_parts = [
            f"pmid:{self.pmid}",
            f"doi:{self.doi}",
            f"{self.pmcid}",
        ]
        return "|".join(id_parts)


@dataclass
class Identifiers:
    identifiers: list[Identifier] = field(default_factory=list)

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

        with file_path.open('w', encoding='utf-8') as f:
            for identifier in self.identifiers:
                json.dump(asdict(identifier), f)
                f.write('\n')

    @classmethod
    def load(cls, file_path: Path | str) -> 'Identifiers':
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
        with file_path.open('r', encoding='utf-8') as f:
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

    def __getitem__(self, index: int | slice) -> Identifier | list[Identifier]:
        """Get identifier(s) by index or slice."""
        return self.identifiers[index]

    def __setitem__(self, index: int, value: Identifier) -> None:
        """Set identifier at index."""
        self.identifiers[index] = value

    def __delitem__(self, index: int | slice) -> None:
        """Delete identifier(s) at index or slice."""
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

    def extend(self, items: list[Identifier]) -> None:
        """Extend the list with multiple identifiers."""
        self.identifiers.extend(items)

    def insert(self, index: int, item: Identifier) -> None:
        """Insert an identifier at the specified index."""
        self.identifiers.insert(index, item)

    def remove(self, item: Identifier) -> None:
        """Remove the first occurrence of an identifier."""
        self.identifiers.remove(item)

    def pop(self, index: int = -1) -> Identifier:
        """Remove and return identifier at index (default last)."""
        return self.identifiers.pop(index)

    def clear(self) -> None:
        """Remove all identifiers."""
        self.identifiers.clear()

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

   