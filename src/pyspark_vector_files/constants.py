"""Constants."""

from types import MappingProxyType

from numpy import bool_, float32, float64, int16, int32, int64, object0
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

# See:
# https://gdal.org/python/index.html
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
# https://numpy.org/doc/stable/reference/arrays.dtypes.html

OGR_TO_SPARK = MappingProxyType(
    {
        (0, 0): IntegerType(),  # OFTInteger
        (0, 1): BooleanType(),  # OFSTBoolean
        (0, 2): ShortType(),  # OFSTInt16
        (1, 0): ArrayType(IntegerType()),  # OFTIntegerList
        (1, 1): ArrayType(ShortType()),  # List of OFSTInt16
        (1, 2): ArrayType(BooleanType()),  # List of OFSTBoolean
        (2, 0): DoubleType(),  # OFTReal
        (2, 3): FloatType(),  # OFSTFloat32
        (3, 0): ArrayType(DoubleType()),  # OFTRealList
        (3, 3): ArrayType(FloatType()),  # List of OFSTFloat32
        (4, 0): StringType(),  # OFTString
        # ? Could OFSTJSON be MapType?
        (4, 4): StringType(),  # OFSTJSON
        (4, 5): StringType(),  # OFSTUUID
        (5, 0): ArrayType(StringType()),  # OFTStringList
        (6, 0): StringType(),  # OFTWideString
        (7, 0): ArrayType(StringType()),  # OFTWideStringList
        (8, 0): BinaryType(),  # OFTBinary
        (9, 0): StringType(),  # OFTDate
        (10, 0): StringType(),  # OFTTime
        (11, 0): StringType(),  # OFTDateTime
        (12, 0): LongType(),  # OFTInteger64
        (13, 0): ArrayType(LongType()),  # OFTInteger64List
    }
)

SPARK_TO_PANDAS = MappingProxyType(
    {
        IntegerType(): int32,
        BooleanType(): bool_,
        ShortType(): int16,
        ArrayType(IntegerType()): object0,
        ArrayType(BooleanType()): object0,
        ArrayType(ShortType()): object0,
        DoubleType(): float64,
        FloatType(): float32,
        ArrayType(DoubleType()): object0,
        ArrayType(FloatType()): object0,
        StringType(): object0,
        ArrayType(StringType()): object0,
        BinaryType(): object0,
        LongType(): int64,
        ArrayType(LongType()): object0,
    }
)
