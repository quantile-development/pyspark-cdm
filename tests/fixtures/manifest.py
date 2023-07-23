import pytest
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture
def manifest(corpus: CdmCorpusDefinition) -> Manifest:
    return Manifest(
        corpus=corpus,
        path="cdm:/Tables/Tables.manifest.cdm.json",
    )
