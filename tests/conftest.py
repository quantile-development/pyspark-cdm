import warnings
from tests.fixtures.spark import spark
from tests.fixtures.manifest import manifest
from tests.fixtures.corpus import manifest_corpus, model_corpus
from tests.fixtures.entity import entity

warnings.filterwarnings("ignore", module="cdm")
warnings.simplefilter("ignore", category=DeprecationWarning)
