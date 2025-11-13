#!/usr/bin/env python3
"""Run the ingestion pipeline against a playground manifest."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
# Consolidate path setup before importing project modules.
if sys.version_info < (3, 9):
    raise SystemExit("Please run this script with Python 3.9 or newer.")

from typing import List

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion_workflow.config import load_settings
from ingestion_workflow.workflow.orchastrator import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute the ingestion pipeline using a playground manifest."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("playground/manifests/manifest_n1000.jsonl"),
        help="Path to the manifest JSONL file (default: playground/manifests/manifest_n10.jsonl).",
    )
    parser.add_argument(
        "--playground-root",
        type=Path,
        default=Path("playground"),
        help="Root directory for playground outputs (default: playground/).",
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        default=["gather", "download", "extract", "create_analyses"],
        help="Pipeline stages to run (default: gather download extract create_analyses).",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        default=True,
        help="Enable exporting bundles (default: enabled).",
    )
    parser.add_argument(
        "--no-export",
        dest="export",
        action="store_false",
        help="Disable exporting bundles.",
    )
    parser.add_argument(
        "--overwrite-export",
        action="store_true",
        default=True,
        help="Overwrite exported files (default: enabled).",
    )
    parser.add_argument(
        "--no-overwrite-export",
        dest="overwrite_export",
        action="store_false",
        help="Disable overwriting exported files.",
    )
    return parser.parse_args()


def ensure_directories(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _prepare_manifest(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    valid = [line for line in lines if line.strip() and json.loads(line).get("pmcid")]
    if not valid:
        raise ValueError(
            f"No PMCIDs found in manifest {path}. Regenerate the manifest with PMC entries."
        )
    if len(valid) == len(lines):
        return path
    filtered_path = path.with_name(path.stem + "_pmcid.jsonl")
    filtered_path.write_text("\n".join(valid) + "\n", encoding="utf-8")
    print(
        f"Filtered manifest to entries with PMCIDs: {filtered_path} "
        f"({len(valid)} of {len(lines)} entries retained)"
    )
    return filtered_path


def _has_llm_credentials() -> bool:
    return any(
        os.environ.get(env_key)
        for env_key in ("LLM_API_KEY", "OPENAI_API_KEY")
    )


def main() -> None:
    args = parse_args()

    playground_root = args.playground_root.resolve()
    manifest_path = args.manifest.resolve()
    data_root = playground_root / "data"
    cache_root = playground_root / ".cache"
    ns_pond_root = playground_root / "ns-pond"

    ensure_directories(
        playground_root,
        manifest_path.parent,
        data_root,
        cache_root,
        ns_pond_root,
    )

    stages = [stage.lower() for stage in args.stages]
    if "create_analyses" in stages and not _has_llm_credentials():
        print(
            "No LLM API key detected; skipping create_analyses stage."
        )
        stages = [stage for stage in stages if stage != "create_analyses"]

    overrides = {
        "data_root": data_root,
        "cache_root": cache_root,
        "ns_pond_root": ns_pond_root,
        "stages": stages,
        "manifest_path": manifest_path,
        "use_cached_inputs": True,
        "export": args.export,
        "export_overwrite": args.overwrite_export,
        "max_workers": 4,
        "ace_max_workers": 4,
        "download_sources": ["pubget", "elsevier", "ace"],
        "n_llm_workers": 4,
    }

    settings = load_settings(overrides=overrides)
    print("Running pipeline with settings:")
    print(f"  data_root: {settings.data_root}")
    print(f"  cache_root: {settings.cache_root}")
    print(f"  ns_pond_root: {settings.ns_pond_root}")
    print(f"  manifest: {settings.manifest_path}")
    print(f"  stages: {settings.stages}")
    print(f"  export: {settings.export} (overwrite={settings.export_overwrite})")
    print("Launching pipeline...")

    run_pipeline(settings=settings)
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    load_dotenv()
    main()
