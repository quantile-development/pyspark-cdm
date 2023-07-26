import pytest
from cdm.enums import CdmStatusLevel
from cdm.objectmodel import CdmCorpusDefinition
from cdm.storage import LocalAdapter
from cdm.storage.adls import ADLSAdapter
from pyspark_cdm.utils import event_callback
from tests.consts import MANIFEST_SAMPLE_PATH, MODEL_SAMPLE_PATH


@pytest.fixture(scope="session")
def manifest_corpus():
    corpus = CdmCorpusDefinition()
    corpus.set_event_callback(event_callback, CdmStatusLevel.ERROR)
    corpus.storage.mount("cdm", LocalAdapter(root=str(MANIFEST_SAMPLE_PATH)))
    return corpus


@pytest.fixture(scope="session")
def model_corpus():
    corpus = CdmCorpusDefinition()
    corpus.set_event_callback(event_callback, CdmStatusLevel.ERROR)
    corpus.storage.mount("local", LocalAdapter(root=str(MODEL_SAMPLE_PATH)))
    corpus.storage.mount(
        "adls",
        ADLSAdapter(
            root="/dataverse",
            hostname="xyz.dfs.core.windows.net",
            tenant="72f988bf-86f1-41af-91ab-2d7cd011db47",
            resource="https://storage.azure.com",
        ),
    )
    return corpus
