import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col
from pyspark_cdm.catalog import Catalog
from typing import Optional
from .utils import first_non_empty_values

DATE_FORMATS = {
    None: r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,}Z", # yyyy-MM-dd'T'HH:mm:ss.SSSZ doesn't work, pyspark format should be empty
    "M/d/yyyy h:mm:ss a": r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M",
    "yyyy-MM-dd'T'HH:mm:ss.SSS": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}",
    "yyyy-MM-dd'T'HH:mm:ss'Z'": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
    "dd-MM-yyyy HH:mm:ss": r"\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}",
    "dd-MM-yyyy": r"\d{2}-\d{2}-\d{4}",
}

class DatetimeParser:
    def __init__(self, df: DataFrame, catalog: Catalog) -> None:
        self.df = df
        self.catalog = catalog

    def detect_date_format(self, date_string: str) -> Optional[str]:
        """
        Tries to find a matching regex pattern on the provided date_string. If it
        matches it returns the corresponding pyspark_format. Otherwise, it raises
        an exception.
        """
        for pyspark_format, regex in DATE_FORMATS.items():
            # The regex match fails if the provided date_string is a none value, therefore,
            # we return none once we stumble upon a none value.
            if date_string == None:
               return None

            if re.match(regex, date_string):
                return pyspark_format

        raise Exception(f"Cant find a matching datetime pattern for {date_string}")

    def try_parsing_datetime_column(
        self,
        df: DataFrame,
        column_name: str,
        datetime_format: str,
    ) -> DataFrame:
        """
        Convert the a single datetime column using the to_timestamp method 
        with the required datetime format (e.g. "M/d/yyyy hh:mm:ss a").
        """
        try:
          df = df.withColumn(column_name, to_timestamp(col(column_name), datetime_format))
          # print(f'Succesfully parsed {column_name} with {datetime_format}')
          return df

        except:
          print(f'Failed parsing {column_name} with {datetime_format}')
          return df

    def convert_datetime_columns(
        self,
    ) -> DataFrame:
        """
        Loops over all the timestamp related columns and transforms these from strings
        into datetime objects.
        """
        # Get for all the timestamp columns the first non empty value
        sampled_values = first_non_empty_values(
          self.df,
          self.catalog.timestamp_columns,
        )

        # Loop over all the timestamp columns and convert it into a datetime column
        df_parsed = self.df

        for column_name in self.catalog.timestamp_columns:
          pyspark_format = self.detect_date_format(sampled_values[column_name])

          df_parsed = self.try_parsing_datetime_column(
            df_parsed, 
            column_name, 
            pyspark_format
          )

        return df_parsed