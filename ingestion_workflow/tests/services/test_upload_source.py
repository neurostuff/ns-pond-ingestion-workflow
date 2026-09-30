"""Which extractor produced a study, and what that key decides.

`source` is not a label. neurostore holds one study version per source, so it
decides whether an upload updates a row or adds one, and whether `discover`
thinks a base study is still work.
"""

from __future__ import annotations

import inspect

from ingestion_workflow.config import Settings
from ingestion_workflow.services.discover import LLM_SOURCE, unprocessed_base_studies


def test_the_default_source_is_unchanged():
    """Every existing deployment writes 'llm'. Changing the default would
    silently re-process the whole corpus on upgrade."""
    assert Settings.model_fields["upload_source"].default == "llm"
    assert LLM_SOURCE == "llm"


def test_upload_takes_the_source_from_settings_not_a_constant():
    """It used to be hardcoded, which made the extractor unrecordable."""
    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._get_or_create_study)
    assert 'getattr(self.settings, "upload_source"' in src
    assert 'payload.source = payload.source or "llm"' not in src


def test_upload_matches_an_existing_version_by_source():
    """This is why a new extractor adds a version rather than overwriting:
    the lookup is keyed on source."""
    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._get_or_create_study)
    assert "version.source == payload.source" in src


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
    assert "source=settings.upload_source" in src


def test_a_configured_source_survives_settings_construction():
    settings = Settings(upload_source="nuextract-v19")
    assert settings.upload_source == "nuextract-v19"


def test_the_payload_default_does_not_shadow_the_setting():
    """`payload.source or settings.upload_source` only reaches the setting if
    the payload default is falsy. A default of "llm" is truthy and would win
    silently, which is how this was wrong the first time."""
    from ingestion_workflow.models.upload import StudyPayload

    assert StudyPayload().source is None


def test_an_explicit_payload_source_still_wins():
    """A caller that names the source means it."""
    from ingestion_workflow.models.upload import StudyPayload

    payload = StudyPayload(source="hand-curated")
    assert (payload.source or "llm") == "hand-curated"
