import pytest
from pyspark_cdm import Entity, Manifest
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture
def entity(corpus: CdmCorpusDefinition):
    manifest = Manifest(
        corpus=corpus,
        path="cdm:/Tables/Common/Customer/Main/Main.manifest.cdm.json",
    )
    return manifest.entities[0]
