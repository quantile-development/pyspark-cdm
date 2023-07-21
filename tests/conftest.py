import warnings
from tests.fixtures.spark import spark
from tests.fixtures.reader import cdm_manifest_reader

warnings.filterwarnings("ignore", module="cdm")
warnings.simplefilter("ignore", category=DeprecationWarning)
