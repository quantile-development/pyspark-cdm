from ast import Str
import asyncio
from glob import glob
from functools import cached_property
from os import replace
from typing import Generator, List
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmEntityDefinition,
    CdmLocalEntityDeclarationDefinition,
    CdmManifestDefinition,
)
from cdm.persistence import PersistenceLayer
from cdm.persistence.modeljson.types.local_entity import LocalEntity
from pyspark_cdm.catalog import catalog_factory
from pyspark_cdm.utils import (
    get_document_from_path,
    remove_root_from_path,
)
from cdm.utilities.copy_options import CopyOptions
from cdm.utilities.resolve_options import ResolveOptions
from cdm.persistence.modeljson import LocalEntityDeclarationPersistence
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
        return self.declaration.entity_name

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
    def is_model(self) -> bool:
        """
        Whether the entity is a model or a manifest.

        Returns:
            bool: Whether the entity is a model or a manifest.
        """
        return self.manifest.document.at_corpus_path.endswith(
            PersistenceLayer.MODEL_JSON_EXTENSION
        )

    @cached_property
    def data(self) -> LocalEntity:
        """
        Get the raw underlying data of the entity.
        """
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            LocalEntityDeclarationPersistence.to_data(
                self.declaration,
                self.manifest.document,
                ResolveOptions(),
                CopyOptions(),
            )
        )
        data = loop.run_until_complete(task)
        return data

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

        for partition in self.declaration.data_partitions:
            location = partition.location
            location = location.replace("adls:", "local:")
            path = self.corpus.storage.corpus_path_to_adapter_path(location)
            yield remove_root_from_path(path, "/dbfs")

    @property
    def schema(self) -> StructType:
        """
        The schema of the entity.

        Returns:
            str: The schema of the entity.
        """
        catalog = catalog_factory(self)
        return catalog.schema

    def get_dataframe(self, spark) -> DataFrame:
        return spark.read.csv(
            list(self.file_paths),
            header=False,
            schema=self.schema,
            inferSchema=False,
            multiLine=True,
            escape='"',
        )
