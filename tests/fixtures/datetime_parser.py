import pytest
from pyspark.sql import SparkSession
from pyspark_cdm.datetime_parser import DatetimeParser
from pyspark_cdm.entity import Entity

@pytest.fixture
def datetime_parser(spark: SparkSession, entity: Entity):
    df = entity.get_dataframe(spark, True)
    catalog = entity.catalog

    datetime_parser = DatetimeParser(
        df=df,
        catalog=catalog,
    )
    
    return datetime_parser
