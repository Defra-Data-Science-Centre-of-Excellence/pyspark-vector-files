"""Functions for reading a GPKG using Spark's JDBC drivers."""

from struct import unpack
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, expr, udf
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

HEADER_LENGTH = 40


def _read_gpkg(
    filepath: str,
    layer_name: str,
    spark: SparkSession,
) -> SparkDataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{filepath}")
        .option("dbtable", layer_name)
        .load()
    )


return_schema = StructType(
    [
        StructField("magic", StringType()),
        StructField("version", IntegerType()),
        StructField("flags", StringType()),
        StructField("srs_id", IntegerType()),
        StructField("envelope", ArrayType(DoubleType())),
    ]
)


@udf(returnType=return_schema)
def _unpack_gpb_header(byte_array: bytearray) -> Dict[str, Any]:
    unpacked = unpack("ccBBidddd", byte_array)
    return {
        "magic": unpacked[0].decode("ascii") + unpacked[1].decode("ascii"),
        "version": unpacked[2],
        "flags": format(unpacked[3], "b").zfill(8),
        "srs_id": unpacked[4],
        "envelope": [unpacked[5], unpacked[6], unpacked[7], unpacked[8]],
    }


def _split_geometry(
    df: SparkDataFrame,
    original_geometry_column_name: str,
    header_length: int,
    drop_gpb_header: bool,
) -> SparkDataFrame:
    split_df = df.select(
        "*",
        expr(
            f"SUBSTRING({original_geometry_column_name}, 1, {header_length}) AS gpb_header"  # noqa B950
        ),
        expr(
            f"SUBSTRING({original_geometry_column_name}, {header_length}+1, LENGTH({original_geometry_column_name})-{header_length}) AS wkb_geometry"  # noqa B950
        ),
    )
    if drop_gpb_header:
        return split_df.drop(original_geometry_column_name, "gpb_header")
    else:
        return split_df.drop(original_geometry_column_name).withColumn(
            "gpb_header", _unpack_gpb_header("gpb_header")
        )


def list_layers(
    filepath: str,
    spark: SparkSession,
) -> List[str]:
    """List layers."""
    gpkg_contents = _read_gpkg(
        filepath=filepath,
        layer_name="gpkg_contents",
        spark=spark,
    )
    return gpkg_contents.select(collect_list("table_name")).first()[0]


def read(
    filepath: str,
    original_geometry_column_name: str = "geom",
    header_length: int = HEADER_LENGTH,
    drop_gpb_header: bool = False,
    layer_name: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> SparkDataFrame:
    """Read GeoPackage into Spark."""
    _spark = spark if spark else SparkSession.getActiveSession()

    _layer_name = (
        layer_name
        if layer_name
        else list_layers(
            filepath=filepath,
            spark=_spark,
        )[0]
    )

    df = _read_gpkg(
        filepath=filepath,
        layer_name=_layer_name,
        spark=_spark,
    )

    return _split_geometry(
        df=df,
        original_geometry_column_name=original_geometry_column_name,
        header_length=header_length,
        drop_gpb_header=drop_gpb_header,
    )
