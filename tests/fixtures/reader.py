import pytest
from tests.consts import MANIFEST_SAMPLE_PATH
from pyspark_cdm import CdmReader
from pyspark.sql import SparkSession
from pathlib import Path


@pytest.fixture
def cdm_manifest_reader(spark: SparkSession):
    return CdmReader(
        spark=spark,
        root_path=str(MANIFEST_SAMPLE_PATH),
        start_manifest="cdm:/Tables/Tables.manifest.cdm.json",
    )
