"""
This module is responsible for calling the application
from the command line, setting the yaml config file,
etc.
"""

from __future__ import annotations
import typer

app = typer.Typer(
    name="Article Ingestion Workflow",
    help="Neuroimaging article ingestion workflow for Neurostore",
)


@app.command()
def run():
    pass


@app.command()
def search():
    pass


@app.command()
def download():
    pass


@app.command()
def extract():
    pass


@app.command()
def create_analyses():
    pass


@app.command()
def upload():
    pass


@app.command()
def sync():
    pass


def main() -> None:
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
