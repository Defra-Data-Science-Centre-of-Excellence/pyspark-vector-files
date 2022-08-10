"""Functions for reading a GPKG using Spark's JDBC drivers.

Register the GeoPackage Dialect
===============================

To register the GeoPackage dialect, run the following in a Databricks notebook cell:

```scala
%scala
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._

object GeoPackageDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlite")

  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = typeName match {
    case "BOOLEAN" => Some(BooleanType)
    case "TINYINT" => Some(ByteType)
    case "SMALLINT" => Some(ShortType)
    case "MEDIUMINT" => Some(IntegerType)
    case "INT" | "INTEGER" => Some(LongType)
    case "FLOAT" => Some(FloatType)
    case "DOUBLE" | "REAL" => Some(DoubleType)
    case "TEXT" => Some(StringType)
    case "BLOB" => Some(BinaryType)
    case "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" |
      "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "CIRCULARSTRING" |
      "COMPOUNDCURVE" | "CURVEPOLYGON" | "MULTICURVE" | "MULTISURFACE" | "CURVE" |
      "SURFACE" => Some(BinaryType)
    case "DATE" => Some(DateType)
    case "DATETIME" => Some(StringType)
  }
}

JdbcDialects.registerDialect(GeoPackageDialect)
```
"""
from functools import partial
from struct import unpack
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, expr, udf
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
    """Read a given table from a GeoPackage/Sqlite file."""
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{filepath}")
        .option("dbtable", layer_name)
        .load()
    )


_get_gpkg_contents = partial(
    _read_gpkg,
    layer_name="gpkg_contents",
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
    """Unpack the GeoPackage Binary Header."""
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
    """Split the GeoPackage geometry column into the binary header and WKB geometry."""
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
    spark: Optional[SparkSession] = None,
) -> List[str]:
    """List layers from a GeoPackage.

    Examples:
        >>> list_layers(
            "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.gpkg"
        )
        ['reference_parcels']

    Args:
        filepath (str): Path to the GeoPackage. Must use the File API Format, i.e.
            start with `/dbfs` instead of `dbfs:`.
        spark (Optional[SparkSession], optional): The SparkSession to use. If none
             is provided, the active session will be used. Defaults to None.

    Returns:
        List[str]: A list of layer names.
    """  # noqa: B950
    _spark = spark if spark else SparkSession.getActiveSession()
    gpkg_contents = _get_gpkg_contents(
        filepath=filepath,
        spark=_spark,
    )
    return gpkg_contents.select(collect_list("table_name")).first()[0]


def get_crs(
    filepath: str,
    layer_name: str,
    spark: Optional[SparkSession] = None,
) -> str:
    """Get CRS of a layer in a GeoPackage.

    Examples:
        >>> get_crs(
            "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.gpkg",
            layer_name="reference_parcels",
        )
        'PROJCS["OSGB 1936 / British National Grid",GEOGCS["OSGB 1936",DATUM["OSGB_1936",SPHEROID["Airy 1830",6377563.396,299.3249646,AUTHORITY["EPSG","7001"]],AUTHORITY["EPSG","6277"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4277"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",49],PARAMETER["central_meridian",-2],PARAMETER["scale_factor",0.9996012717],PARAMETER["false_easting",400000],PARAMETER["false_northing",-100000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","27700"]]'

    Args:
        filepath (str): Path to the GeoPackage. Must use the File API Format, i.e.
            start with `/dbfs` instead of `dbfs:`.
        layer_name (str): The name of the layer to read.
        spark (Optional[SparkSession], optional): The SparkSession to use. If none
             is provided, the active session will be used. Defaults to None.

    Returns:
        str: `Well-known Text`_ Representation of the Spatial Reference System.

    .. _`Well-known Text`:
        https://www.ogc.org/standards/wkt-crs
    """  # noqa: B950
    _spark = spark if spark else SparkSession.getActiveSession()
    gpkg_contents = _get_gpkg_contents(
        filepath=filepath,
        spark=_spark,
    )
    srs_id = (
        gpkg_contents.filter(col("table_name") == layer_name)
        .select("srs_id")
        .first()[0]
    )
    gpkg_spatial_ref_sys = _read_gpkg(
        filepath=filepath,
        layer_name="gpkg_spatial_ref_sys",
        spark=_spark,
    )
    return (
        gpkg_spatial_ref_sys.filter(col("srs_id") == srs_id)
        .select("definition")
        .first()[0]
    )


def read_gpkg(
    filepath: str,
    original_geometry_column_name: str = "geom",
    header_length: int = HEADER_LENGTH,
    drop_gpb_header: bool = False,
    layer_name: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> SparkDataFrame:
    """Read a GeoPackage into Spark DataFrame.

    Args:
        filepath (str): Path to the GeoPackage. Must use the File API Format, i.e.
            start with `/dbfs` instead of `dbfs:`.
        original_geometry_column_name (str): The name of the geometry column
            of the layer in the GeoPackage. Defaults to "geom".
        header_length (int): The length of the `GPB header`_, in bytes.
            Defaults to 40.
        drop_gpb_header (bool): Whether to exclude the `GPB header`_ from
            the returned DataFrame. Defaults to False.
        layer_name (str, optional): The name of the layer to read. If none is provided,
            the first layer is read. Defaults to None.
        spark (SparkSession, optional): The SparkSession to use. If none
             is provided, the active session will be used. Defaults to None.

    Returns:
        SparkDataFrame: A Spark DataFrame with the geometry encoded in Well Known
            Binary.

    .. _`GPB header`:
        https://www.geopackage.org/spec/#gpb_format
    """
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
