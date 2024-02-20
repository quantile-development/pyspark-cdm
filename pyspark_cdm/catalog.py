from abc import ABC, abstractproperty
from cdm.enums import CdmStatusLevel, CdmDataFormat
from pyspark.sql.types import (
    StructType,
    StructField,
    ByteType,
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

TIMESTAMP_TYPES = [TimestampType(), DateType()]

def catalog_factory(entity: "Entity") -> "Catalog":
    if entity.is_model:
        return ModelCatalog(entity)
    else:
        return ManifestCatalog(entity)


class Catalog(ABC):
    def __init__(self, entity: "Entity"):
        self.entity = entity

    @abstractproperty
    def type_mapping(self) -> dict:
        pass

    @abstractproperty
    def schema(self) -> StructType:
        pass

    @property
    def timestamp_columns(self) -> list[str]:
        """
        Extract the columns that are parsed as timestamp or datetimes data types.
        """
        return [
            entity.name
            for entity
            in self.schema
            if entity.dataType in TIMESTAMP_TYPES
        ]
    
    def overwrite_timestamp_types(self, schema: StructType) -> StructType:
        """
        Overwrite in the provided schema the timestamp date types to strings.
        """
        overwritten_date_types = list()

        for entity in schema:
            if entity.dataType in TIMESTAMP_TYPES:
                entity.dataType = StringType()

            overwritten_date_types.append(entity)

        return StructType(overwritten_date_types)


class ModelCatalog(Catalog):
    @property
    def type_mapping(self) -> dict:
        return {
            "int16": IntegerType(),
            "int32": IntegerType(),
            "int64": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "guid": StringType(),
            "string": StringType(),
            "char": StringType(),
            "byte": ByteType(),
            "binary": BinaryType(),
            "time": TimestampType(),
            "date": DateType(),
            "dateTime": TimestampType(),
            "dateTimeOffset": TimestampType(),
            "boolean": BooleanType(),
            "decimal": DecimalType(18, 6),
            "json": StringType(),
        }

    @property
    def schema(self) -> StructType:
        return StructType(
            [
                StructField(
                    attribute.name,
                    self.type_mapping[attribute.dataType],
                )
                for attribute in self.entity.data.attributes
            ]
        )


class ManifestCatalog(Catalog):
    @property
    def type_mapping(self) -> dict:
        return {
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

    @property
    def schema(self) -> StructType:
        return StructType(
            [
                StructField(
                    attribute.name,
                    self.type_mapping[attribute.data_format],
                )
                for attribute in self.entity.document.attributes
            ]
        )
