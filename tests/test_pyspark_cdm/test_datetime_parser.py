from pyspark_cdm.datetime_parser import DatetimeParser
import pytest

@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_datetime_parser_datetime_regex(datetime_parser: DatetimeParser):
    """
    Ensure that the datetime parser correctly find the regex pattern and returns
    the correct pyspark datetime pattern.
    """
    assert datetime_parser.detect_date_format('1/1/1999 01:12:12 PM') == "M/d/yyyy h:mm:ss a"
    assert datetime_parser.detect_date_format('2022-03-22T13:40:11.000') == "yyyy-MM-dd'T'HH:mm:ss.SSS"
    assert datetime_parser.detect_date_format('2024-01-30T13:24:06Z') == "yyyy-MM-dd'T'HH:mm:ss'Z'"
    assert datetime_parser.detect_date_format('20-12-2023') == "dd-MM-yyyy"
    assert datetime_parser.detect_date_format('20-12-2023 15:45:03') == "dd-MM-yyyy HH:mm:ss"

    with pytest.raises(Exception):
        datetime_parser.detect_date_format('20-12ABC-2023')

    with pytest.raises(Exception):
        datetime_parser.detect_date_format('1/1/1999 01:12:12 PAM')
    