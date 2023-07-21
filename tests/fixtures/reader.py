import pytest
import tests
from pyspark_cdm import CdmReader
from pyspark.sql import SparkSession
from pathlib import Path

SAMPLES_PATH = Path(tests.__file__).parent / "samples"
MANIFEST_SAMPLE_PATH = SAMPLES_PATH / "manifest"
MODEL_SAMPLE_PATH = SAMPLES_PATH / "model"


@pytest.fixture
def cdm_manifest_reader(spark: SparkSession):
    return CdmReader(
        spark=spark,
        root_path=str(MANIFEST_SAMPLE_PATH),
        start_manifest="cdm:/Tables/Tables.manifest.cdm.json",
    )
