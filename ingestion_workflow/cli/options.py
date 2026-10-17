"""Option definitions that more than one command takes."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

ConfigOption = typer.Option(None, "--config", "-c", help="YAML settings file.")

CatalogOption = typer.Option(
    None, "--catalog",
    help="Catalog directory (holding catalog.sqlite); defaults to the config's.")

ProseOption = typer.Option(
    None, "--prose/--no-prose", help="Stage graph with or without the prose stages; "
    "defaults to whether the catalog holds any.")

BackupOption = typer.Option(
    None, "--backup", help="Where --apply copies the catalog database first; must not exist.")


def apply_option(help: str):
    return typer.Option(False, "--apply", help=help)


def dry_run_option(help: str):
    return typer.Option(False, "--dry-run", help=help)


def manifest_option(help: str):
    return typer.Option(None, "--manifest", "-m", exists=True, help=help)


def require_new_backup(apply: bool, backup: Optional[Path]) -> None:
    """Refuse --apply without a --backup path that is still free, before any work."""
    if apply and backup is None:
        raise typer.BadParameter("--apply needs --backup <path>", param_hint="--backup")
    if apply and backup.exists():
        raise typer.BadParameter(f"{backup} exists; give a new path", param_hint="--backup")
