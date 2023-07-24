from ast import Str
from glob import glob
from functools import cached_property
from typing import Generator, List
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmEntityDefinition,
    CdmLocalEntityDeclarationDefinition,
    CdmManifestDefinition,
)
from pyspark_cdm.utils import (
    get_document_from_path,
    cdm_data_type_to_spark,
    remove_root_from_path,
)
from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame


class Entity:
    def __init__(
        self,
        corpus: CdmCorpusDefinition,
        manifest,
        declaration: CdmLocalEntityDeclarationDefinition,
    ) -> None:
        self.corpus = corpus
        self.manifest = manifest
        self.declaration = declaration

    @property
    def name(self) -> str:
        """
        The name of the entity.

        Returns:
            str: The name of the entity.
        """
        return self.document.entity_name

    @property
    def path(self) -> str:
        """
        The path to the entity file.

        Returns:
            str: The path to the entity file.
        """
        return f"{self.manifest.document.folder.at_corpus_path}/{self.declaration.entity_path}"

    @cached_property
    def document(self) -> CdmEntityDefinition:
        return get_document_from_path(
            corpus=self.corpus,
            path=self.path,
        )

    @property
    def file_patterns(self) -> Generator[str, None, None]:
        """
        A list of file patterns that contain the data for the current entity.

        Returns:
            List[str]: A list of file paths.
        """
        for partition in self.declaration.data_partition_patterns:
            corpus_pattern = f"{self.manifest.document.folder.at_corpus_path}/{partition.root_location}/{partition.glob_pattern}"
            adapter_pattern = self.corpus.storage.corpus_path_to_adapter_path(
                corpus_pattern
            )

            if adapter_pattern:
                yield adapter_pattern

    @property
    def file_paths(self) -> Generator[str, None, None]:
        """
        Use the file patterns to get the actual file paths using the pathlib library.
        """
        for file_pattern in self.file_patterns:
            for file_path in glob(file_pattern):
                yield remove_root_from_path(file_path, "/dbfs")

    @property
    def schema(self) -> StructType:
        """
        The schema of the entity.

        Returns:
            str: The schema of the entity.
        """
        return StructType(
            [
                StructField(
                    attribute.name,
                    cdm_data_type_to_spark(attribute.data_format),
                )
                for attribute in self.document.attributes
            ]
        )

    def get_dataframe(self, spark) -> DataFrame:
        return spark.read.csv(
            list(self.file_paths),
            header=False,
            schema=self.schema,
            inferSchema=False,
            escape="'",
        )
