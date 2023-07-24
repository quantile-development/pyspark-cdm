import asyncio
from typing import Optional
from cdm.enums import CdmStatusLevel, CdmDataFormat
from cdm.objectmodel import (
    CdmCorpusDefinition,
    CdmObject,
)
from pyspark.sql.types import (
    DataType,
    StructType,
    StructField,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    TimestampType,
    DateType,
    BooleanType,
    DecimalType,
)


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
    return manifest


def cdm_data_type_to_spark(
    cdm_data_type: CdmDataFormat,
) -> DataType:
    """
    Convert a CDM data type to a Spark data type.

    Args:
        cdm_data_type (CdmDataFormat): CDM data type.
    """
    type_mapping = {
        CdmDataFormat.INT16: IntegerType(),
        CdmDataFormat.INT32: IntegerType(),
        CdmDataFormat.INT64: LongType(),
        CdmDataFormat.FLOAT: FloatType(),
        CdmDataFormat.DOUBLE: DoubleType(),
        CdmDataFormat.GUID: StringType(),
        CdmDataFormat.STRING: StringType(),
        CdmDataFormat.CHAR: StringType(),
        CdmDataFormat.BYTE: ByteType(),
        CdmDataFormat.BINARY: BinaryType(),
        CdmDataFormat.TIME: TimestampType(),
        CdmDataFormat.DATE: DateType(),
        CdmDataFormat.DATE_TIME: TimestampType(),
        CdmDataFormat.DATE_TIME_OFFSET: TimestampType(),
        CdmDataFormat.BOOLEAN: BooleanType(),
        CdmDataFormat.DECIMAL: DecimalType(18, 6),
        CdmDataFormat.JSON: StringType(),
    }

    return type_mapping[cdm_data_type]


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
