from pyspark_cdm import CdmReader
from cdm.objectmodel import CdmManifestDefinition, CdmEntityDefinition


def test_reading_manifest(cdm_manifest_reader: CdmReader):
    manifest = cdm_manifest_reader.get_manifest_from_path(
        "cdm:/Tables/Tables.manifest.cdm.json"
    )

    assert type(manifest) == CdmManifestDefinition
    assert len(manifest.sub_manifests) > 0


# def test_loading_entities(cdm_manifest_reader: CdmReader):
#     assert len(cdm_manifest_reader.entities) > 0
#     assert type(cdm_manifest_reader.entities[0]) == CdmEntityDefinition
