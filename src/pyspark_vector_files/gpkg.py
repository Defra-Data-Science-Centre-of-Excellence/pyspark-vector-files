"""Functions for reading a GeoPackage using Spark's JDBC drivers.

Reading a GeoPackage using Spark's JDBC drivers.
===================================================

Register the GeoPackage Dialect
-------------------------------

To read a GeoPackage using Spark's JDBC drivers, you will need to register a custom
mapping of GeoPackage to Spark Catalyst types.


To do this, run the following in a Databricks notebook cell:

.. code-block:: scala

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

Once you've registered the GeoPackage dialect, you can list the layers in a given
GeoPackage, get the coordinate reference system of a given layer, or read a layer
into a Spark DataFrame with the geometry column encoded as Well Known Binary.

List layers
-----------

To list the layers in a given GeoPackage, use :func:`list_layers`:

.. code-block:: python

    list_layers(
        path="/path/to/file.gpkg"
    )

Get the coordinate reference system of a layer
----------------------------------------------

To get the coordinate reference system of a layers in a given GeoPackage, use
:func:`get_crs`:

.. code-block:: python

    get_crs(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
    )

Get the bounding box of a layer
-------------------------------

To get the bounding box of a layers in a given GeoPackage, use
:func:`get_bounds`:

.. code-block:: python

    get_bounds(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
    )

Read a layer into a Spark DataFrame
-----------------------------------

To read a layer into a Spark DataFrame, use :func:`read_gpkg`, If you don't supply
a layer, the first layer will be read:

.. code-block:: python

    read_gpkg(
        path="/path/to/file.gpkg"
    )

Read a specific layer
^^^^^^^^^^^^^^^^^^^^^

If you supply a layer, that will be read:

.. code-block:: python

    read_gpkg(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
   )

Supply a custom geometry column name
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The function will assume that the geometry column is called `geom`, if it's called
something else, supply a different `original_geometry_column_name`:

.. code-block:: python

    read_gpkg(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
        original_geometry_column_name="geometry",
    )

GeoPackage Geometry Encoding
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`GeoPackage Geometry Encoding`_ consists of two parts:

1. A GeoPackage Binary (GPB) header
2. A `Well Known Binary (WKB)`_ geometry

By default, the function will split the original geometry column into these two
parts, unpack the GPB header, and return both as, respectively, `gpb_header` and
`wkb_geometry`.

If you don't want the GPB header, set `drop_gpb_header` to `True`:

.. code-block:: python

    read_gpkg(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
        drop_gpb_header=True,
    )

The length of the GPB header depends on the format of envelope (bounding box) included
in it. The function assumes that the envelope will be [minx, maxx, miny, maxy] and
that, therefore, the GPB header will be 40 bytes long. However, it's possible for the
format of the envelope to be [minx, maxx, miny, maxy, minz, maxz] or [minx, maxx, miny,
maxy, minm, maxm] and, therefore 56 bytes long, or [minx, maxx, miny, maxy, minz, maxz,
minm, maxm] and, therefore, 72 bytes long.

In these cases you will need to set `header_length` to the appropriate value:

.. code-block:: python

    read_gpkg(
        path="/path/to/file.gpkg"
        layer_name="layer_name"
        header_length=56,
    )

.. _`GeoPackage Geometry Encoding`:
    https://www.geopackage.org/spec/#gpb_format

.. _`Well Known Binary (WKB)`:
    https://libgeos.org/specifications/wkb/

"""
from functools import partial
from struct import unpack
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, collect_list, expr, udf
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
    path: str,
    layer_name: str,
    spark: SparkSession,
) -> SparkDataFrame:
    """Read a given table from a GeoPackage/Sqlite file."""
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{path}")
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
    path: str,
    spark: Optional[SparkSession] = None,
) -> List[str]:
    """List layers from a GeoPackage.

    Examples:
        >>> list_layers(
            "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.gpkg"
        )
        ['reference_parcels']

    Args:
        path (str): Path to the GeoPackage. Must use the File API Format, i.e.
            start with `/dbfs` instead of `dbfs:`.
        spark (Optional[SparkSession], optional): The SparkSession to use. If none
             is provided, the active session will be used. Defaults to None.

    Returns:
        List[str]: A list of layer names.
    """  # noqa: B950
    _spark = spark if spark else SparkSession.getActiveSession()
    gpkg_contents = _get_gpkg_contents(
        path=path,
        spark=_spark,
    )
    # ! Explicitly coerce `table_names` to `List[str]` to prevent `mypy`
    # ! `[no-any-return]`
    table_names: List[str] = gpkg_contents.select(collect_list("table_name")).first()[0]
    return table_names


def get_crs(
    path: str,
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
        path (str): Path to the GeoPackage. Must use the File API Format, i.e.
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
        path=path,
        spark=_spark,
    )
    srs_id = (
        gpkg_contents.filter(col("table_name") == layer_name)
        .select("srs_id")
        .first()[0]
    )
    gpkg_spatial_ref_sys = _read_gpkg(
        path=path,
        layer_name="gpkg_spatial_ref_sys",
        spark=_spark,
    )
    # ! Explicitly coerce `definition` to `str` to prevent `mypy` `[no-any-return]`
    definition: str = (
        gpkg_spatial_ref_sys.filter(col("srs_id") == srs_id)
        .select("definition")
        .first()[0]
    )
    return definition


def get_bounds(
    path: str,
    layer_name: str,
    spark: Optional[SparkSession] = None,
) -> Tuple[float, ...]:
    """Get bounding box of a layer in a GeoPackage.

    Examples:
        >>> get_bounds(
            "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.gpkg",
            layer_name="reference_parcels",
        )
        (85668.93009999998, 7079.01, 654494.0698999999, 657493.4378)

    Args:
        path (str): Path to the GeoPackage. Must use the File API Format, i.e.
            start with `/dbfs` instead of `dbfs:`.
        layer_name (str): The name of the layer to read.
        spark (Optional[SparkSession], optional): The SparkSession to use. If none
             is provided, the active session will be used. Defaults to None.

    Returns:
        Tuple[float, ...]: a tuple containing `minx`, `miny`, `maxx`, `maxy` values
            for the layer.
    """  # noqa: B950
    _spark = spark if spark else SparkSession.getActiveSession()
    gpkg_contents = _get_gpkg_contents(
        path=path,
        spark=_spark,
    )
    # ! Explicitly coerce `bounds` to `Tuple[float, ...]` to prevent `mypy`
    # ! `[no-any-return]`
    bounds: Tuple[float, ...] = tuple(
        gpkg_contents.filter(col("table_name") == layer_name)
        .select(array(col("min_x"), col("min_y"), col("max_x"), col("max_y")))
        .first()[0]
    )
    return bounds


def read_gpkg(
    path: str,
    original_geometry_column_name: str = "geom",
    header_length: int = HEADER_LENGTH,
    drop_gpb_header: bool = False,
    layer_name: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> SparkDataFrame:
    """Read a GeoPackage into Spark DataFrame.

    Args:
        path (str): Path to the GeoPackage. Must use the File API Format, i.e.
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
        SparkDataFrame: A Spark DataFrame with geometry encoded as
            `Well Known Binary (WKB)`_.

    .. _`GPB header`:
        https://www.geopackage.org/spec/#gpb_format
    .. _`Well Known Binary (WKB)`:
        https://libgeos.org/specifications/wkb/
    """
    _spark = spark if spark else SparkSession.getActiveSession()

    _layer_name = (
        layer_name
        if layer_name
        else list_layers(
            path=path,
            spark=_spark,
        )[0]
    )

    df = _read_gpkg(
        path=path,
        layer_name=_layer_name,
        spark=_spark,
    )

    return _split_geometry(
        df=df,
        original_geometry_column_name=original_geometry_column_name,
        header_length=header_length,
        drop_gpb_header=drop_gpb_header,
    )
