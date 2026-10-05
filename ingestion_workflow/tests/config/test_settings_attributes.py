"""Every setting an extractor or stage reads must exist on Settings.

`force_redownload` was removed from Settings with the catalog refactor while
the ACE extractor still read it, so every ACE scrape failed with
AttributeError: 363 articles in one run, recorded as failed downloads.
"""
import ast
from pathlib import Path

from ingestion_workflow.config import Settings

ROOT = Path(__file__).resolve().parents[2]


def _reads(path):
    for node in ast.walk(ast.parse(path.read_text())):
        if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
                and node.value.attr == "settings" and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "self"):
            yield node.attr, node.lineno


def test_every_setting_read_through_self_settings_exists():
    known = set(Settings.model_fields) | {n for n in dir(Settings) if not n.startswith("_")}
    missing = []
    for folder in ("extractors", "pipeline", "services", "clients"):
        for path in sorted((ROOT / folder).rglob("*.py")):
            for name, line in _reads(path):
                if name not in known:
                    missing.append(f"{path.relative_to(ROOT)}:{line} settings.{name}")
    assert not missing, missing
