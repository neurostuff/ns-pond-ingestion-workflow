import json

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifiers,
)


@pytest.mark.usefixtures("manifest_identifiers")
@pytest.mark.vcr()
def test_elsevier_download_records_articles(tmp_path, manifest_identifiers):
    subset = Identifiers(list(manifest_identifiers.identifiers[:100]))

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )

    if not settings.elsevier_api_key:
        pytest.skip("Elsevier API key not configured for recording")

    extractor = ElsevierExtractor(settings=settings)

    results = extractor.download(subset)

    assert len(results) == len(subset.identifiers)
    successes = [result for result in results if result.success]
    assert successes, "Expected at least one successful Elsevier download"

    for index, result in enumerate(results):
        identifier = subset.identifiers[index]
        assert result.identifier is identifier
        assert result.source is DownloadSource.ELSEVIER

        if result.success:
            assert result.files, "Successful downloads should persist files"

            metadata_file = next(
                file
                for file in result.files
                if file.file_type is FileType.JSON
            )
            metadata = json.loads(
                metadata_file.file_path.read_text(encoding="utf-8")
            )

            expected_lookup_type = "doi" if identifier.doi else "pmid"
            expected_lookup_value = identifier.doi or identifier.pmid

            assert metadata["lookup_type"] == expected_lookup_type
            assert metadata["lookup_value"] == expected_lookup_value
            assert metadata["identifier_hash"] == identifier.hash_id
        else:
            assert not result.files
