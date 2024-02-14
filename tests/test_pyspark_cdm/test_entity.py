from cdm.objectmodel import CdmEntityDefinition
from cdm.persistence.modeljson.types.local_entity import LocalEntity
import pytest
from pyspark_cdm import Entity
from tests.consts import MANIFEST_SAMPLE_PATH
import pyspark.sql.types as T
from pyspark.sql import DataFrame


def test_entity_name(entity: Entity):
    """
    Make sure that the name property correctly returns the name of the entity.
    """
    assert type(entity.name) == str
    assert len(entity.name) > 0


def test_entity_is_model(entity: Entity):
    """
    Make sure that the is_model property correctly returns False.
    """
    assert type(entity.is_model) == bool


def test_entity_data(entity: Entity):
    if entity.is_model:
        assert type(entity.data) == LocalEntity


def test_entity_document(entity: Entity):
    """
    Make sure that the document property correctly returns a CdmEntityDefinition.
    This is the actual content of the entity file.
    """
    if not entity.is_model:
        assert type(entity.document) == CdmEntityDefinition


def test_entity_file_patterns(entity: Entity):
    """
    Make sure that the file_patterns property correctly returns a list of file patterns.
    """
    if not entity.is_model:
        assert (
            f"{MANIFEST_SAMPLE_PATH}/Tables/Common/Customer/Main/CustTable/*.csv"
            in list(entity.file_patterns)
        )


def test_entity_file_patterns(entity: Entity):
    """
    Make sure that the file_patterns property correctly returns a list of file patterns.
    """
    assert type(list(entity.file_patterns)) == list


def test_entity_file_paths(entity: Entity):
    """
    Make sure that the file_paths property correctly returns a list of file paths.
    """
    assert type(list(entity.file_paths)) == list


def test_entity_schema(entity: Entity):
    """
    Make sure that the schema property correctly returns a StructType.
    """
    assert type(entity.schema) == T.StructType


def test_entity_dataframe(entity: Entity, spark):
    """
    Make sure that the dataframe property correctly returns a Spark DataFrame.
    """
    df = entity.get_dataframe(spark=spark)
    assert type(df) == DataFrame
    assert df.count() == 3


def test_entity_alter_schema(entity: Entity, spark):
    """
    Make sure that the alter_schema parameter correctly alters the schema of the dataframe.
    """

    def alter_schema(schema):
        return T.StructType([T.StructField("_id", T.StringType()), *schema[1:]])

    df = entity.get_dataframe(spark=spark, alter_schema=alter_schema)
    assert df.columns[0] == "_id"
