import pytest
from cdm.enums import CdmStatusLevel
from cdm.objectmodel import CdmCorpusDefinition, CdmManifestDefinition
from cdm.storage import LocalAdapter
from pyspark_cdm.utils import event_callback
from tests.consts import MANIFEST_SAMPLE_PATH


@pytest.fixture
def corpus():
    corpus = CdmCorpusDefinition()
    corpus.set_event_callback(event_callback, CdmStatusLevel.ERROR)
    corpus.storage.mount("cdm", LocalAdapter(root=str(MANIFEST_SAMPLE_PATH)))
    return corpus
