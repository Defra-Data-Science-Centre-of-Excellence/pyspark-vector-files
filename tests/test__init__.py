"""Tests for the main io module."""
from typing import Optional

import pytest
from geopandas import GeoDataFrame, GeoSeries
from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pytest import FixtureRequest

from pyspark_vector_files import read_vector_files, temporary_spark_context


def to_gdf(
    sdf: SparkDataFrame,
    geometry_column_name: str = "geometry",
    crs: str = "EPSG:27700",
) -> GeoDataFrame:
    """Converts SparkDataFrame to GeoDataFrame."""
    pdf = sdf.toPandas()
    return GeoDataFrame(
        data=pdf,
        geometry=GeoSeries.from_wkb(
            pdf[geometry_column_name].apply(lambda row: bytes(row))
        ),
        crs=crs,
    )


def test_temporary_spark_context(
    spark_context: SparkSession,
    configuration_key: str,
    default_partitions: str,
    expected_temporary_partitions: str,
) -> None:
    """The number of partitions is temporarily changed to 100."""
    previous_partitions = spark_context.conf.get(configuration_key)
    assert previous_partitions == default_partitions

    with temporary_spark_context(
        configuration_key=configuration_key,
        new_configuration_value=expected_temporary_partitions,
        spark=spark_context,
    ) as spark:
        temporary_partitions = spark.conf.get(configuration_key)
        assert temporary_partitions == expected_temporary_partitions

    subsequent_partitions = spark_context.conf.get(configuration_key)
    assert subsequent_partitions == default_partitions


@pytest.mark.parametrize(
    argnames=[
        "path",
        "pattern",
        "suffix",
        "ideal_chunk_size",
        "coerce_to_schema",
        "vsi_prefix",
        "schema",
        "layer_identifier",
        "concurrency_strategy",
        "expected_gdf",
    ],
    argvalues=[
        (
            "shapefiles_path",
            "*",
            ".shp",
            3_000_000,
            False,
            None,
            None,
            None,
            "files",
            "expected_shapefiles_gdf",
        ),
        (
            "gzipped_shapefiles_path",
            "*",
            ".gz",
            3_000_000,
            False,
            "/vsitar/",
            None,
            None,
            "files",
            "expected_shapefiles_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            None,
            "files",
            "expected_gdb_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            "second",
            "files",
            "expected_gdb_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            0,
            "files",
            "expected_gdb_gdf",
        ),
        (
            "shapefiles_path",
            "*",
            ".shp",
            3_000_000,
            False,
            None,
            None,
            None,
            "rows",
            "expected_shapefiles_gdf",
        ),
        (
            "gzipped_shapefiles_path",
            "*",
            ".gz",
            3_000_000,
            False,
            "/vsitar/",
            None,
            None,
            "rows",
            "expected_shapefiles_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            None,
            "rows",
            "expected_gdb_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            "second",
            "rows",
            "expected_gdb_gdf",
        ),
        (
            "fileGDB_directory_path",
            "*",
            ".gdb",
            3_000_000,
            False,
            None,
            None,
            0,
            "rows",
            "expected_gdb_gdf",
        ),
    ],
    ids=[
        "Files strategy - multiple shapefiles",
        "Files strategy - multiple shapefiles - gzipped",
        "Files strategy - multiple gdb - No layer identifier",
        "Files strategy - multiple gdb - Layer name",
        "Files strategy - multiple gdb - Layer index",
        "Rows strategy - multiple shapefiles",
        "Rows strategy - multiple shapefiles - gzipped",
        "Rows strategy - multiple gdb - No layer identifier",
        "Rows strategy - multiple gdb - Layer name",
        "Rows strategy - multiple gdb - Layer index",
    ],
)
def test_read_vector_files(
    request: FixtureRequest,
    path: str,
    pattern: str,
    suffix: str,
    ideal_chunk_size: int,
    coerce_to_schema: bool,
    vsi_prefix: Optional[str],
    schema: Optional[StructType],
    layer_identifier: Optional[str],
    concurrency_strategy: str,
    expected_gdf: str,
) -> None:
    """Returns expected SparkDataFrames from different combinations of args."""
    sdf = read_vector_files(
        path=request.getfixturevalue(path),
        pattern=pattern,
        suffix=suffix,
        ideal_chunk_size=ideal_chunk_size,
        coerce_to_schema=coerce_to_schema,
        vsi_prefix=vsi_prefix,
        schema=schema,
        layer_identifier=layer_identifier,
        concurrency_strategy=concurrency_strategy,
    )

    gdf = to_gdf(sdf).sort_values(by=["id"]).reset_index(drop=True)

    assert_frame_equal(
        left=gdf,
        right=request.getfixturevalue(expected_gdf),
        check_like=True,
    )
