import pytest
from ingestion_workflow.catalog import Catalog


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat
