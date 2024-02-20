from pyspark_cdm import Entity
from pyspark_cdm.catalog import Catalog
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
)
from pyspark_cdm.catalog import catalog_factory


def test_entity_schema_overwrite_occured(entity: Entity):
    """
    When we use detection and "manually" detect the timestamp format we overwrite 
    the original schema. We expect that the timestamp typed columns are changed to 
    string types.
    """
    schema = StructType([
        StructField("foo", TimestampType(), True),
        StructField("foo", StringType(), True),
        StructField("foo", DateType(), True),
    ])

    assert entity.catalog.overwrite_timestamp_types(schema) == StructType([
        StructField("foo", StringType(), True),
        StructField("foo", StringType(), True),
        StructField("foo", StringType(), True),
    ])