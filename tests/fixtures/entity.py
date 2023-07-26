import pytest
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture
def manifest_entity(manifest_corpus: CdmCorpusDefinition):
    manifest = Manifest(
        corpus=manifest_corpus,
        path="cdm:/Tables/Common/Customer/Main/Main.manifest.cdm.json",
    )
    return list(manifest.entities)[0]


@pytest.fixture
def model_entity(model_corpus: CdmCorpusDefinition):
    manifest = Manifest(
        corpus=model_corpus,
        path="local:/model.json",
    )
    return list(manifest.entities)[0]
