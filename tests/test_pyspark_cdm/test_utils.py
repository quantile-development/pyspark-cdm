from pyspark_cdm.utils import remove_root_from_path, first_non_empty_values
from pyspark.sql import Row
import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

def test_remove_root_from_path():
    """
    Make sure that the root is removed from the path.
    """
    assert remove_root_from_path("/dbfs/mnt/test/", "/dbfs") == "/mnt/test/"
    assert remove_root_from_path("/dbfs/mnt/test/", "/something") == "/dbfs/mnt/test/"

@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_first_non_empty_values(spark):
    """
    Checks if the util function correctly returns the first non empty value
    for each selected column.
    """
    # Create a spark dataframe with none values
    df_with_empty_values = spark.createDataFrame(
        data = [
            ("1", None, "0001-01-01T00:00:00.0000000"),
            ("2", "12/9/2023 9:44:23 PM", "1234"),
            ("3", "apple", None),
            ("4", None, None),
        ],
        schema=["id","timestamp_1","timestamp_2"]
    )

    first_non_empty_value_per_column = first_non_empty_values(df_with_empty_values, df_with_empty_values.columns)
    assert first_non_empty_value_per_column == Row(id='1', timestamp_1='12/9/2023 9:44:23 PM', timestamp_2='0001-01-01T00:00:00.0000000')


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_extracting_non_empty_values_with_empty_columns(spark): 
    """
    Checks if the util function functions with a column with None values.
    """
    df_with_empty_values = spark.createDataFrame(
        data = [
            ("1", None, "0001-01-01T00:00:00.0000000"),
            ("2", None, "1234"),
            ("3", None, None),
            ("4", None, None),
        ],
        schema=StructType([
            StructField("id", StringType(), True),
            StructField("timestamp_1", StringType(), True),
            StructField("timestamp_2", StringType(), True),
        ])
    )

    first_non_empty_value_per_column = first_non_empty_values(df_with_empty_values, df_with_empty_values.columns)
    assert first_non_empty_value_per_column == Row(id='1', timestamp_1=None, timestamp_2='0001-01-01T00:00:00.0000000')