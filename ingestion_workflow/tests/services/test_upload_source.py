"""Which extractor produced a study, and what that key decides.

`source` is not a label. neurostore holds one study version per source, so it
decides whether an upload updates a row or adds one, and whether `discover`
thinks a base study is still work.
"""

from __future__ import annotations

import inspect

from ingestion_workflow.config import Settings
from ingestion_workflow.services.discover import LLM_SOURCE, unprocessed_base_studies


def test_there_is_no_default_source():
    """It used to default to 'llm'. Under the default 'update' behaviour that
    made a forgotten setting destructive: the study lookup resolved to another
    extractor's version and the reconciliation deleted its analyses to put
    these in their place. There is no source that is safe to guess, so silence
    is refused rather than filled in."""
    assert Settings.model_fields["upload_source"].default is None
    assert LLM_SOURCE == "llm"      # discover's own default is unchanged


def test_upload_refuses_to_start_without_a_source():
    import pytest

    from ingestion_workflow.pipeline.stages.upload import UploadStage
    from ingestion_workflow.services.upload import (
        UploadSourceNotSet,
        resolve_upload_source,
    )

    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings())
    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings(upload_source="   "))
    assert resolve_upload_source(Settings(upload_source="nuextract-v19")) == "nuextract-v19"

    # checked before the tunnel is opened, not in __init__ -- planning and
    # --dry-run change nothing and must still work without a source
    assert UploadStage(Settings()) is not None
    execute = inspect.getsource(UploadStage.execute)
    assert "resolve_upload_source(self.settings)" in execute
    assert execute.index("resolve_upload_source") < execute.index("self._withdrawn")
    assert execute.index("resolve_upload_source") < execute.index("self._execute")


def test_upload_takes_the_source_from_settings_not_a_constant():
    """It used to be hardcoded, which made the extractor unrecordable."""
    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._get_or_create_study)
    assert "resolve_upload_source(self.settings)" in src
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
    assert "source=_upload_source(settings)" in src


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


def test_the_source_is_part_of_the_upload_fingerprint():
    """A different source is a different study version in neurostore, not a
    relabelling of the same one. Leaving it out of the fingerprint made every
    already-uploaded article look fresh when the extractor changed."""
    import inspect

    from ingestion_workflow.pipeline.stages.upload import UploadStage

    assert "self.settings.upload_source," in inspect.getsource(UploadStage.fingerprint_for)
