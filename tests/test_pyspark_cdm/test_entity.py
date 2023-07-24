from cdm.objectmodel import CdmEntityDefinition
from pyspark_cdm import Entity
from tests.consts import MANIFEST_SAMPLE_PATH
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


def test_entity_document(entity: Entity):
    """
    Make sure that the document property correctly returns a CdmEntityDefinition.
    This is the actual content of the entity file.
    """
    assert type(entity.document) == CdmEntityDefinition


def test_entity_name(entity: Entity):
    """
    Make sure that the name property correctly returns the name of the entity.
    """
    assert entity.name == "CustTable"


def test_file_patterns(entity: Entity):
    """
    Make sure that the file_patterns property correctly returns a list of file patterns.
    """
    assert (
        f"{MANIFEST_SAMPLE_PATH}/Tables/Common/Customer/Main/CustTable/*.csv"
        in list(entity.file_patterns)
    )


def test_file_paths(entity: Entity):
    """
    Make sure that the file_paths property correctly returns a list of file paths.
    """
    assert (
        f"{MANIFEST_SAMPLE_PATH}/Tables/Common/Customer/Main/CustTable/CUSTTABLE_00001.csv"
        in list(entity.file_paths)
    )


def test_entity_schema(entity: Entity):
    """
    Make sure that the schema property correctly returns a StructType.
    """
    assert type(entity.schema) == StructType


def test_dataframe(entity: Entity, spark):
    """
    Make sure that the dataframe property correctly returns a Spark DataFrame.
    """
    df = entity.get_dataframe(spark=spark)
    assert type(df) == DataFrame
    assert df.count() == 3
