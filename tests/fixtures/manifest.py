import pytest
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture(scope="session", params=["model", "manifest"])
def manifest(
    request, manifest_corpus: CdmCorpusDefinition, model_corpus: CdmCorpusDefinition
) -> Manifest:
    if request.param == "model":
        return Manifest(
            corpus=model_corpus,
            path="local:/model.json",
        )
    elif request.param == "manifest":
        return Manifest(
            corpus=manifest_corpus,
            path="cdm:/Tables/Tables.manifest.cdm.json",
        )

    raise ValueError(f"Invalid parameter value: {request.param}")


# @pytest.fixture(scope="session")
# def model(model_corpus: CdmCorpusDefinition) -> Manifest:
#     return Manifest(
#         corpus=model_corpus,
#         path="local:/model.json",
#     )
