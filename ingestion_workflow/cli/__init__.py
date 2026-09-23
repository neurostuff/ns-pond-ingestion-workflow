"""Command line application entry point.

Only `app` is re-exported. Re-exporting `main` too would shadow the
`ingestion_workflow.cli.main` *module* with the function of the same name, so
`import ingestion_workflow.cli.main` would hand back the function instead.
"""

from .main import app

__all__ = ["app"]
