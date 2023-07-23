import warnings
from tests.fixtures.spark import spark
from tests.fixtures.reader import cdm_manifest_reader
from tests.fixtures.manifest import manifest
from tests.fixtures.corpus import corpus
from tests.fixtures.entity import entity

warnings.filterwarnings("ignore", module="cdm")
warnings.simplefilter("ignore", category=DeprecationWarning)
