from pyspark_cdm import Manifest, Entity
from cdm.objectmodel import CdmManifestDefinition


def test_manifest_document(manifest: Manifest):
    """
    Make sure that the document property correctly returns a CdmManifestDefinition.
    This is the actual content of the manifest file.
    """
    assert type(manifest.document) == CdmManifestDefinition


def test_sub_manifest_path(manifest: Manifest):
    """
    Make sure the first sub-manifest path is correct.
    """
    common_sub_manifest = next(manifest.sub_manifests)
    assert common_sub_manifest.path == "cdm:/Tables/Common/Common.manifest.cdm.json"


def test_sub_manifests(manifest: Manifest):
    """
    Make sure that the sub_manifests property correctly returns a generator of Manifests.
    And that there are at least 3 sub-manifests.
    """
    assert len(list(manifest.sub_manifests)) >= 3


def test_manifest_entities(manifest: Manifest):
    """
    Make sure that the entities property correctly returns a list of Entities.
    And that there are at least 1 entity.
    """
    assert len(list(manifest.entities)) >= 1
    assert type(next(manifest.entities)) == Entity


def test_model_entities(model: Manifest):
    """
    Make sure that the entities property correctly returns a list of Entities.
    And that there are at least 1 entity.
    """
    assert len(list(model.entities)) >= 1
    assert type(next(model.entities)) == Entity
