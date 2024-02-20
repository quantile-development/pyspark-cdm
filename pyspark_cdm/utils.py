import asyncio
from typing import Optional, List
from cdm.enums import CdmStatusLevel, CdmDataFormat
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmObject,
)
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F


from pyspark_cdm.exceptions import DocumentLoadingException


def get_or_create_eventloop() -> asyncio.AbstractEventLoop:
    """
    Get or create an event loop, this is to make it work in a multi-threaded environment.
    """
    try:
        return asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return asyncio.get_event_loop()


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
    loop = get_or_create_eventloop()
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

def first_non_empty_values(
    df: DataFrame,
    selected_columns: List[str]
) -> Row:
    """
    Returns for each given column the first non empty value. Be aware that
    once a column is full with nulls, it returns a None for that given
    column.
    """
    row = df.select(
        [
            F.first(column, ignorenulls=True).alias(column) 
            for column 
            in selected_columns
        ]
    ).first()

    return row