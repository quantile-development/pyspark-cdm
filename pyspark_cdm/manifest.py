from functools import cached_property
from re import sub
from typing import Generator, List, Optional
from pyspark_cdm.entity import Entity
from pyspark_cdm.utils import get_document_from_path
from cdm.objectmodel import CdmCorpusDefinition, CdmManifestDefinition


class Manifest:
    def __init__(
        self,
        corpus: CdmCorpusDefinition,
        path: str,
    ):
        self.corpus = corpus
        self.path = path

    @cached_property
    def document(self) -> CdmManifestDefinition:
        return get_document_from_path(
            corpus=self.corpus,
            path=self.path,
        )

    @property
    def entities(self) -> Generator[Entity, None, None]:
        """
        A list of entities in the current manifest.

        Returns:
            List[Entity]: A list of entities.
        """
        for entity in self.document.entities:
            yield Entity(
                corpus=self.corpus,
                manifest=self,
                declaration=entity,
            )

        for sub_manifest in self.sub_manifests:
            for entity in sub_manifest.document.entities:
                yield Entity(
                    corpus=self.corpus,
                    manifest=sub_manifest,
                    declaration=entity,
                )

    @property
    def sub_manifests(self) -> Generator["Manifest", None, None]:
        """
        A generator of sub-manifests, it recursively returns all sub-manifests.

        Yields:
            Manifest: A sub-manifest.
        """
        # Loop through all the sub-manifests found in the current manifest.
        for sub_manifest in self.document.sub_manifests:
            # Create a new Manifest object for each sub-manifest.
            sub_manifest = Manifest(
                corpus=self.corpus,
                path=self.document.folder.at_corpus_path + sub_manifest.definition,
            )

            # Yield the sub-manifest.
            yield sub_manifest

            # If the sub-manifest has sub-manifests, yield them as well.
            for sub_sub_manifest in sub_manifest.sub_manifests:
                yield sub_sub_manifest
