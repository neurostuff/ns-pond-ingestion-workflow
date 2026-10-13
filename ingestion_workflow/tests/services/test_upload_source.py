"""Which extractor's study versions `discover` counts as done.

`source` is not a label: neurostore holds one study version per source, and
`discover` thinks a base study is still work until a version with this source
exists.
"""

from __future__ import annotations

import inspect

from ingestion_workflow.config import Settings
from ingestion_workflow.services.discover import LLM_SOURCE, unprocessed_base_studies


def test_there_is_no_default_source():
    """It used to default to 'llm', which answered about another extractor's work
    when the setting was forgotten. There is no source that is safe to guess, so
    silence is refused rather than filled in."""
    assert Settings.model_fields["upload_source"].default is None
    assert LLM_SOURCE == "llm"      # discover's own default is unchanged


def test_discover_refuses_to_start_without_a_source():
    import pytest

    from ingestion_workflow.services.discover import UploadSourceNotSet, resolve_upload_source

    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings())
    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings(upload_source="   "))
    assert resolve_upload_source(Settings(upload_source="nuextract-v19")) == "nuextract-v19"


def test_discover_asks_about_a_source_it_is_given():
    """Otherwise pointing the pipeline at a new extractor would leave discover
    asking about the old one, and it would report no work to do."""
    params = inspect.signature(unprocessed_base_studies).parameters
    assert "source" in params
    assert params["source"].default == LLM_SOURCE


def test_the_cli_passes_the_configured_source_to_discover():
    """The parameter is only useful if the one real caller uses it."""
    from ingestion_workflow.cli import main

    src = inspect.getsource(main._discover_from_neurostore)
    assert "source=_upload_source(settings)" in src


def test_a_configured_source_survives_settings_construction():
    settings = Settings(upload_source="nuextract-v19")
    assert settings.upload_source == "nuextract-v19"


