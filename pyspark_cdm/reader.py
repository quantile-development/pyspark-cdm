import asyncio
import os
import sys
from typing import List, Optional, Tuple

from cdm.enums import CdmObjectType, CdmStatusLevel
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmLocalEntityDeclarationDefinition,
    CdmEntityDefinition,
    CdmManifestDefinition,
    CdmTraitDefinition,
    CdmObject,
    CdmManifestDeclarationDefinition,
)
from cdm.storage import LocalAdapter
from cdm.utilities import CopyOptions
from functools import cached_property
from pyspark.sql import SparkSession
from pyspark_cdm.utils import event_callback


class CdmReader:
    def __init__(
        self,
        spark: SparkSession,
        root_path: str,
        start_manifest: str,
    ) -> None:
        """
        CDM class to read CDM entities and manifests.

        Args:
            spark (SparkSession): Spark session.
            path (str): Path to the CDM folder.
        """
        self.spark = spark
        self.root_path = root_path
        self.start_manifest = start_manifest
        self.corpus.set_event_callback(event_callback, CdmStatusLevel.WARNING)
        self.corpus.storage.mount("cdm", LocalAdapter(root=self.root_path))

    @cached_property
    def corpus(self) -> CdmCorpusDefinition:
        """
        A reference to the CDM corpus.
        """
        return CdmCorpusDefinition()

    def get_manifest_from_path(self, path: str) -> Optional[CdmObject]:
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.corpus.fetch_object_async(path))
        manifest = loop.run_until_complete(task)
        return manifest

    def get_entity_declarations_and_sub_manifests(
        self,
        manifest: CdmManifestDefinition,
    ) -> Tuple[List[CdmLocalEntityDeclarationDefinition], List[CdmManifestDefinition]]:
        entities = []
        sub_manifests = []

        # Add all the entities in the manifest
        for entity in manifest.entities:
            entities.append(entity)

        # Loop through all possible sub manifests
        for sub_manifest_declaration in manifest.sub_manifests:
            # Get the sub manifest path
            sub_manifest_path = (
                manifest.folder.at_corpus_path + sub_manifest_declaration.definition
            )

            # Fetch the sub manifest content
            sub_manifest = self.get_manifest_from_path(sub_manifest_path)

            # Recursively get the entities and sub manifests from the sub manifest
            (
                sub_manifest_entities,
                sub_manifest_sub_manifests,
            ) = self.get_entity_declarations_and_sub_manifests(sub_manifest)

            # Add the entities and sub manifests to the list
            entities += sub_manifest_entities
            sub_manifests += sub_manifest_sub_manifests

        return entities, sub_manifests

    @cached_property
    def entity_declarations(self) -> List[CdmLocalEntityDeclarationDefinition]:
        start_manifest = self.get_manifest_from_path(self.start_manifest)
        # entity_declaration_definitions = self.get_entity_declarations_and_sub_manifests(
        #     start_manifest
        # )[0]

        # return entity_declaration_definitions

    @cached_property
    def entities(self) -> List[CdmEntityDefinition]:
        entities = []

        for entity_declaration in self.entity_declarations:
            for data_partition in entity_declaration.data_partition_patterns:
                print(data_partition.location)
                # entity = self.get_manifest_from_path(
                #     data_partition.location.replace("cdm:", "")
                # )
                # entities.append(entity)
        #     entity = self.get_manifest_from_path(
        #         entity_declaration.entity_path
        #     )
        #     entities.append(entity)

        # return entities
