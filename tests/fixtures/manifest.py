import pytest
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture(scope="session")
def manifest(manifest_corpus: CdmCorpusDefinition) -> Manifest:
    return Manifest(
        corpus=manifest_corpus,
        path="cdm:/Tables/Tables.manifest.cdm.json",
    )


@pytest.fixture(scope="session")
def model(model_corpus: CdmCorpusDefinition) -> Manifest:
    return Manifest(
        corpus=model_corpus,
        path="local:/model.json",
    )
