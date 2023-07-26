from cdm.objectmodel import CdmEntityDefinition
from cdm.persistence.modeljson.types.local_entity import LocalEntity
import pytest
from pyspark_cdm import Entity
from tests.consts import MANIFEST_SAMPLE_PATH, MODEL_SAMPLE_PATH
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


def test_manifest_is_model(manifest_entity: Entity):
    """
    Make sure that the is_model property correctly returns False.
    """
    assert manifest_entity.is_model is False


def test_model_is_model(model_entity: Entity):
    """
    Make sure that the is_model property correctly returns True.
    """
    assert model_entity.is_model is True


def test_model_data(model_entity: Entity):
    print(model_entity.data.attributes)
    assert type(model_entity.data) == LocalEntity


def test_manifest_entity_document(manifest_entity: Entity):
    """
    Make sure that the document property correctly returns a CdmEntityDefinition.
    This is the actual content of the entity file.
    """
    assert type(manifest_entity.document) == CdmEntityDefinition


def test_manifest_file_patterns(manifest_entity: Entity):
    """
    Make sure that the file_patterns property correctly returns a list of file patterns.
    """
    assert (
        f"{MANIFEST_SAMPLE_PATH}/Tables/Common/Customer/Main/CustTable/*.csv"
        in list(manifest_entity.file_patterns)
    )


def test_model_file_patterns(model_entity: Entity):
    """
    Make sure that the file_patterns property correctly returns a list of file patterns.
    """
    assert list(model_entity.file_patterns) == []


def test_manifest_file_paths(manifest_entity: Entity):
    """
    Make sure that the file_paths property correctly returns a list of file paths.
    """
    assert (
        f"{MANIFEST_SAMPLE_PATH}/Tables/Common/Customer/Main/CustTable/CUSTTABLE_00001.csv"
        in list(manifest_entity.file_paths)
    )


def test_model_file_paths(model_entity: Entity):
    """
    Make sure that the file_paths property correctly returns a list of file paths.
    """
    assert f"{MODEL_SAMPLE_PATH}/example/Snapshot/1_1690183552.csv" in list(
        model_entity.file_paths
    )


def test_manifest_entity_schema(manifest_entity: Entity):
    """
    Make sure that the schema property correctly returns a StructType.
    """
    assert type(manifest_entity.schema) == StructType


def test_model_entity_schema(model_entity: Entity):
    """
    Make sure that the schema property correctly returns a StructType.
    """
    assert type(model_entity.schema) == StructType


def test_manifest_dataframe(manifest_entity: Entity, spark):
    """
    Make sure that the dataframe property correctly returns a Spark DataFrame.
    """
    df = manifest_entity.get_dataframe(spark=spark)
    assert type(df) == DataFrame
    assert df.count() == 3


def test_model_dataframe(model_entity: Entity, spark):
    """
    Make sure that the dataframe property correctly returns a Spark DataFrame.
    """
    df = model_entity.get_dataframe(spark=spark)
    assert type(df) == DataFrame
    assert df.count() == 3
