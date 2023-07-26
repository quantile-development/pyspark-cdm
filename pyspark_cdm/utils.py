import asyncio
from typing import Optional
from cdm.enums import CdmStatusLevel, CdmDataFormat
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmObject,
)


from pyspark_cdm.exceptions import DocumentLoadingException


def event_callback(
    status_level: CdmStatusLevel,
    message: str,
) -> None:
    """
    Event callback function for CDM.

    Args:
        status_level (CdmStatusLevel): Status level of the message.
        message (str): Message to be printed.
    """
    print(message)


def get_document_from_path(
    corpus: CdmCorpusDefinition,
    path: str,
) -> Optional[CdmObject]:
    """
    Get the content of a document from the CDM corpus.

    Args:
        corpus (CdmCorpusDefinition): CDM corpus.
        path (str): Path to the document.

    Returns:
        Optional[CdmObject]: Content of the document, can be any CDM object.
    """
    loop = asyncio.get_event_loop()
    task = loop.create_task(corpus.fetch_object_async(path))
    manifest = loop.run_until_complete(task)

    if manifest is None:
        raise DocumentLoadingException(f"Unable to load document from path: {path}")

    return manifest


def remove_root_from_path(path: str, root: str) -> str:
    """
    Remove the root from the path.

    Args:
        path (str): Path to remove the root from.
        root (str): Root to remove from the path.

    Returns:
        str: Path without the root.
    """
    return f"/{path.lstrip(root)}"
