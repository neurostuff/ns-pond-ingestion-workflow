import pytest

from ace.config import reset_config

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.ace_extractor import ACEExtractor
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifiers,
)


@pytest.mark.usefixtures("manifest_identifiers")
@pytest.mark.vcr()
def test_ace_downloads_html_articles(tmp_path, manifest_identifiers):
    subset = Identifiers(list(manifest_identifiers.identifiers[:10]))

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        ace_cache_root=tmp_path / "ace_cache",
    )

    extractor = ACEExtractor(settings=settings, download_mode="browser")

    try:
        results = extractor.download(subset)
    finally:
        reset_config("SAVE_ORIGINAL_HTML")

    assert len(results) == len(subset.identifiers)

    successes = [result for result in results if result.success]
    if not successes:
        failure_messages = [result.error_message or "" for result in results]
        pytest.fail(
            "No ACE downloads succeeded; failure details: "
            + " | ".join(failure_messages)
        )

    ace_root = settings.ace_cache_root

    for index, result in enumerate(results):
        identifier = subset.identifiers[index]
        assert result.identifier is identifier
        assert result.source is DownloadSource.ACE

        if result.success:
            assert result.files, "Successful downloads should persist files"
            downloaded_file = result.files[0]
            assert downloaded_file.file_type is FileType.HTML
            assert downloaded_file.file_path.exists()
            assert downloaded_file.file_path.suffix == ".html"
            assert downloaded_file.content_type == "text/html"
            assert ace_root in downloaded_file.file_path.parents
            assert result.error_message is None
        else:
            assert not result.files
            assert result.error_message is not None
