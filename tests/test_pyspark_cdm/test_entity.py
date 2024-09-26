from cdm.objectmodel import CdmEntityDefinition
from cdm.persistence.modeljson.types.local_entity import LocalEntity
from pyspark_cdm import Entity
from tests.consts import MANIFEST_SAMPLE_PATH
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pytest


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
    assert type(entity.catalog.schema) == T.StructType


def test_entity_dataframe(entity: Entity, spark):
    """
    Make sure that the dataframe property correctly returns a Spark DataFrame.
    """
    df = entity.get_dataframe(spark=spark)
    assert type(df) == DataFrame
    assert df.count() == 3


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_entity_with_timestamp_parsing(entity: Entity, spark):
    """
    Ensure that the timestamp parsing goes correctly.
    """
    df_parsed = entity.get_dataframe(spark=spark, infer_timestamp_formats=True)

    if entity.is_model:
        assert df_parsed.filter(F.col("SinkCreatedOn").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("SinkModifiedOn").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("CreatedOn").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("RandomDateTime3").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("RandomDateTime4").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("RandomDateTime5").isNotNull()).count() != 0

    else:
        assert df_parsed.filter(F.col("MODIFIEDDATETIME").isNotNull()).count() != 0
        assert df_parsed.filter(F.col("CREATEDDATETIME").isNotNull()).count() != 0
        assert (
            df_parsed.filter(
                F.col("CREDMANELIGIBLECREDITLIMITDATE").isNotNull()
            ).count()
            != 0
        )


def test_entity_alter_schema(entity: Entity, spark):
    """
    Make sure that the alter_schema parameter correctly alters the schema of the dataframe.
    """

    def alter_schema(schema):
        return T.StructType([T.StructField("_id", T.StringType()), *schema[1:]])

    df = entity.get_dataframe(spark=spark, alter_schema=alter_schema)
    assert df.columns[0] == "_id"


def test_entity_load_kwargs(entity: Entity, spark):
    """
    Make sure that the load_kwargs parameter correctly loads the dataframe.
    """
    df = entity.get_dataframe(spark=spark, header=False)
    assert df.count() == 3

    # We are missing one row because the header is being read as data.
    df = entity.get_dataframe(spark=spark, header=True)
    assert df.count() == 2
