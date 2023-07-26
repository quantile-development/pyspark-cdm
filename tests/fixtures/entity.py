import pytest
from pyspark_cdm import Manifest, Entity
from cdm.objectmodel import CdmCorpusDefinition


@pytest.fixture(scope="session", params=["model", "manifest"])
def entity(
    request, manifest_corpus: CdmCorpusDefinition, model_corpus: CdmCorpusDefinition
) -> Entity:
    if request.param == "model":
        manifest = Manifest(
            corpus=model_corpus,
            path="local:/model.json",
        )
        return next(manifest.entities)

    elif request.param == "manifest":
        manifest = Manifest(
            corpus=manifest_corpus,
            path="cdm:/Tables/Common/Customer/Main/Main.manifest.cdm.json",
        )
        return next(manifest.entities)

    raise ValueError(f"Invalid parameter value: {request.param}")


# @pytest.fixture
# def model_entity(model_corpus: CdmCorpusDefinition):
#     manifest = Manifest(
#         corpus=model_corpus,
#         path="local:/model.json",
#     )
#     return list(manifest.entities)[0]
