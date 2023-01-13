"""Module level fixtures."""
from pathlib import Path
from tarfile import open as open_tarfile
from types import MappingProxyType
from typing import List, Tuple

from geopandas import GeoDataFrame, GeoSeries
from numpy import int64, object_
from osgeo.ogr import DataSource, Open
from pandas import DataFrame as PandasDataFrame
from pandas import Series, concat
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BinaryType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pytest import fixture
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from pyspark_vector_files import OGR_TO_SPARK, SPARK_TO_PANDAS
from pyspark_vector_files._types import Chunks


@fixture
def shared_datadir() -> Path:
    """Path to local `tests/data` folder."""
    return Path(__file__).parent / "data"


@fixture
def layer_column_names() -> Tuple[str, ...]:
    """Column names shared by both dummy layers."""
    return ("id", "category", "geometry")


@fixture
def layer_column_names_missing_column(
    layer_column_names: Tuple[str, ...]
) -> Tuple[str, ...]:
    """Shared column names but missing `id` column."""
    return tuple(name for name in layer_column_names if name != "category")


@fixture
def layer_column_names_additional_column(
    layer_column_names: Tuple[str, ...]
) -> Tuple[str, ...]:
    """Shared column names but with extra column name."""
    return layer_column_names + ("additional",)


@fixture
def first_layer_first_row() -> Tuple[int, str, BaseGeometry]:
    """First row of first dummy layers."""
    return (0, "A", Point(0, 0))


@fixture
def first_layer_second_row() -> Tuple[int, str, BaseGeometry]:
    """Second row of first dummy layers."""
    return (1, "B", Point(1, 0))


@fixture
def first_file_first_layer_gdf(
    layer_column_names: Tuple[str, ...],
    first_layer_first_row: Tuple[int, str, BaseGeometry],
    first_layer_second_row: Tuple[int, str, BaseGeometry],
) -> GeoDataFrame:
    """First dummy layer."""
    return GeoDataFrame(
        data=(
            first_layer_first_row,
            first_layer_second_row,
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    ).astype(
        {
            "id": int64,
            "category": object_,
        },
    )


@fixture
def first_file_first_layer_pdf(
    first_file_first_layer_gdf: GeoDataFrame,
) -> PandasDataFrame:
    """First dummy layer as pdf with wkb geometry column."""
    return PandasDataFrame(
        first_file_first_layer_gdf.to_wkb(),
    )


@fixture
def first_file_first_layer_pdf_first_row(
    first_file_first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """Just the first row of the first layer PDF."""
    return first_file_first_layer_pdf.loc[[0]]


@fixture
def first_file_first_layer_pdf_with_additional_column(
    first_file_first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with additional 'id' column."""
    return first_file_first_layer_pdf.assign(
        additional=Series(),
    )


@fixture
def first_file_first_layer_pdf_with_wrong_types(
    first_file_first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf but all types are object."""
    return first_file_first_layer_pdf.astype(object_)


@fixture
def first_file_first_layer_pdf_with_missing_column(
    first_file_first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with missing 'category' column."""
    return first_file_first_layer_pdf.drop(
        columns=["category"],
    )


@fixture
def second_layer_first_row() -> Tuple[int, str, BaseGeometry]:
    """First row of second dummy layers."""
    return (2, "C", Point(1, 1))


@fixture
def second_layer_second_row() -> Tuple[int, str, BaseGeometry]:
    """Second row of second dummy layers."""
    return (3, "D", Point(0, 1))


@fixture
def first_file_second_layer_gdf(
    layer_column_names: Tuple[str, ...],
    second_layer_first_row: Tuple[int, str, BaseGeometry],
    second_layer_second_row: Tuple[int, str, BaseGeometry],
) -> GeoDataFrame:
    """Second dummy layer."""
    return GeoDataFrame(
        data=(
            second_layer_first_row,
            second_layer_second_row,
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    )


@fixture
def first_file_second_layer_pdf(
    first_file_second_layer_gdf: GeoDataFrame,
) -> PandasDataFrame:
    """Second dummy layer as pdf with wkb geometry column."""
    first_file_second_layer_gdf["geometry"] = first_file_second_layer_gdf[
        "geometry"
    ].to_wkb()
    return PandasDataFrame(
        first_file_second_layer_gdf,
    )


@fixture
def second_file_first_layer_gdf(
    layer_column_names: Tuple[str, ...],
) -> GeoDataFrame:
    """First dummy layer."""
    return GeoDataFrame(
        data=(
            (4, "E", Point(1, 1)),
            (5, "F", Point(1, 2)),
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    )


@fixture
def second_file_second_layer_gdf(
    layer_column_names: Tuple[str, ...],
) -> GeoDataFrame:
    """Second dummy layer."""
    return GeoDataFrame(
        data=(
            (6, "G", Point(2, 2)),
            (7, "H", Point(2, 1)),
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    )


@fixture
def first_file_third_layer_gdf(
    layer_column_names: Tuple[str, ...],
) -> GeoDataFrame:
    """Third dummy layer."""
    return GeoDataFrame(
        data=(
            (8, "H", None),
            (9, "I", None),
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    )


# @fixture
# def directory_path(
#     tmp_path_factory: TempPathFactory,
# ) -> Path:
#     """Pytest temporary directory as Path object."""
#     return tmp_path_factory.getbasetemp()


@fixture
def fileGDB_directory_path(
    shared_datadir: Path,
) -> Path:
    """Folder for FileGDB."""
    fileGDB_directory_path = shared_datadir / "fileGDB"
    if not fileGDB_directory_path.is_dir():
        fileGDB_directory_path.mkdir()
    return fileGDB_directory_path


@fixture
def erroneous_file_path() -> str:
    """A file path that doesn't exist."""
    return "/erroneous/file/path"


@fixture(autouse=True)
def first_fileGDB_path(
    fileGDB_directory_path: Path,
    first_file_first_layer_gdf: GeoDataFrame,
    first_file_second_layer_gdf: GeoDataFrame,
    first_file_third_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    path = fileGDB_directory_path / "first.gdb"

    if not path.exists():

        first_file_first_layer_gdf.to_file(
            filename=path,
            index=False,
            layer="first",
        )

        first_file_second_layer_gdf.to_file(
            filename=path,
            index=False,
            layer="second",
        )

        first_file_third_layer_gdf.to_file(
            filename=path,
            index=False,
            layer="third",
            ignore_fields=["geometry"],
        )

    return str(path)


@fixture(autouse=True)
def second_fileGDB_path(
    fileGDB_directory_path: Path,
    second_file_first_layer_gdf: GeoDataFrame,
    second_file_second_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB."""
    path = fileGDB_directory_path / "second.gdb"

    if not path.exists():

        second_file_first_layer_gdf.to_file(
            filename=path,
            index=False,
            layer="first",
        )

        second_file_second_layer_gdf.to_file(
            filename=path,
            index=False,
            layer="second",
        )

    return str(path)


@fixture
def all_fileGDB_paths(
    first_fileGDB_path: str,
    second_fileGDB_path: str,
) -> Tuple[str, ...]:
    """All FileGDB paths."""
    return (first_fileGDB_path, second_fileGDB_path)


@fixture
def shapefile_directory_path(
    shared_datadir: Path,
    first_file_first_layer_gdf: GeoDataFrame,
    first_file_second_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy shapefiles to folder and returns path as string."""
    shapefile_directory_path = shared_datadir / "shapefile"
    if not shapefile_directory_path.is_dir():
        shapefile_directory_path.mkdir()

    first_path = shapefile_directory_path / "first.shp"
    if not first_path.exists():
        first_file_first_layer_gdf.to_file(
            filename=first_path,
            index=False,
            layer="first",
        )

    second_path = shapefile_directory_path / "second.shp"
    if not second_path.exists():
        first_file_second_layer_gdf.to_file(
            filename=second_path,
            index=False,
            layer="second",
        )

    return str(shapefile_directory_path)


@fixture
def gzipped_shapefiles_path(
    shapefile_directory_path: str,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    _shapefile_directory_path = Path(shapefile_directory_path)

    first_tar_path = _shapefile_directory_path / "first.tar.gz"

    if not first_tar_path.exists():
        first_shapefile_parts = _shapefile_directory_path.glob("first.*")

        with open_tarfile(first_tar_path, "x:gz") as tar:
            for file in first_shapefile_parts:
                tar.add(file, arcname=file.name)

    second_tar_path = _shapefile_directory_path / "second.tar.gz"

    if not second_tar_path.exists():
        second_shapefile_parts = _shapefile_directory_path.glob("second.*")

        with open_tarfile(second_tar_path, "x:gz") as tar:
            for file in second_shapefile_parts:
                tar.add(file, arcname=file.name)

    return shapefile_directory_path


@fixture
def fileGDB_wrong_types_path(
    fileGDB_directory_path: Path,
    first_file_first_layer_pdf_with_wrong_types: PandasDataFrame,
    first_file_second_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    directory_path = fileGDB_directory_path / "wrong"

    if not directory_path.is_dir():
        directory_path.mkdir()

    path = directory_path / "data_source_wrong_types.gdb"

    path_as_string = str(path)

    if not path.exists():

        first_file_first_layer_gdf = GeoDataFrame(
            data=first_file_first_layer_pdf_with_wrong_types,
            geometry=GeoSeries.from_wkb(
                first_file_first_layer_pdf_with_wrong_types["geometry"]
            ),
            crs="EPSG:27700",
        )

        first_file_first_layer_gdf.to_file(
            filename=path_as_string,
            index=False,
            layer="first",
        )

        first_file_second_layer_gdf.to_file(
            filename=path_as_string,
            index=False,
            layer="second",
        )

    return path_as_string


@fixture
def fileGDB_data_source(
    first_fileGDB_path: str,
) -> DataSource:
    """DataSource for FileGDB."""  # noqa: D403
    return Open(first_fileGDB_path)


@fixture
def fileGDB_schema() -> StructType:
    """Schema for dummy FileGDB."""
    return StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )


@fixture
def fileGDB_schema_field_details() -> Tuple[Tuple[str, DataType], ...]:
    """Field details from dummy FileGDB schema."""
    return (
        ("id", LongType()),
        ("category", StringType()),
        ("geometry", BinaryType()),
    )


@fixture
def ogr_to_spark_mapping() -> MappingProxyType:
    """OGR to Spark data type mapping."""
    ogr_to_spark: MappingProxyType = OGR_TO_SPARK
    return ogr_to_spark


@fixture
def spark_to_pandas_mapping() -> MappingProxyType:
    """Spark to Pandas data type mapping."""
    spark_to_pandas: MappingProxyType = SPARK_TO_PANDAS
    return spark_to_pandas


@fixture
def expected_single_chunk() -> Chunks:
    """FileGDB as single chunk."""  # noqa: D403
    return ((0, 3),)


@fixture
def expected_sequence_containing_single_chunk(
    expected_single_chunk: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as single chunk."""
    return (expected_single_chunk,)


@fixture
def expected_multiple_chunks() -> Chunks:
    """FileGDB as two chunks."""  # noqa: D403
    return ((0, 1), (1, 3))


@fixture
def expected_sequence_containing_multiple_chunks(
    expected_multiple_chunks: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as two chunks."""
    return (expected_multiple_chunks,)


@fixture
def expected_sequence_of_chunks(
    expected_single_chunk: Chunks,
    expected_multiple_chunks: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as single chunk and FileGDB as two chunks."""
    return (expected_single_chunk, expected_multiple_chunks)


@fixture
def spark_context() -> SparkSession:
    """Local Spark context."""
    return (
        SparkSession.builder.master(
            "local",
        )
        .appName(
            "Test context",
        )
        .getOrCreate()
    )


@fixture
def expected_paths_sdf(
    spark_context: SparkSession,
    first_fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB path."""
    return spark_context.createDataFrame(
        data=((first_fileGDB_path,),),
        schema="path: string",
    )


@fixture
def expected_single_chunk_sdf(
    spark_context: SparkSession,
    first_fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB as single chunk."""
    return spark_context.createDataFrame(
        data=(((first_fileGDB_path, "first", 0, 0, 3),)),
        schema=StructType(
            [
                StructField("path", StringType()),
                StructField("layer_name", StringType()),
                StructField("id", LongType(), False),
                StructField("start", IntegerType()),
                StructField("stop", IntegerType()),
            ],
        ),
    )


@fixture
def expected_multiple_chunks_sdf(
    spark_context: SparkSession,
    first_fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB as two chunks."""
    return spark_context.createDataFrame(
        data=(
            (
                Row(
                    path=first_fileGDB_path,
                    layer_name="first",
                    id=0,
                    start=0,
                    stop=1,
                ),
                Row(
                    path=first_fileGDB_path,
                    layer_name="first",
                    id=1,
                    start=1,
                    stop=3,
                ),
            )
        ),
        schema=StructType(
            [
                StructField("path", StringType()),
                StructField("layer_name", StringType()),
                StructField("id", LongType(), False),
                StructField("start", IntegerType()),
                StructField("stop", IntegerType()),
            ],
        ),
    )


@fixture
def expected_null_data_frame(
    layer_column_names: Tuple[str, ...],
) -> PandasDataFrame:
    """Empty PDF with correct column names and dtypes."""
    return PandasDataFrame(columns=layer_column_names).astype(
        {
            "id": int64,
            "category": object_,
            "geometry": object_,
        },
    )


@fixture
def expected_shapefiles_gdf(
    first_file_first_layer_gdf: GeoDataFrame,
    first_file_second_layer_gdf: GeoDataFrame,
) -> GeoDataFrame:
    """Expected concatenation of shapefile gdfs."""
    return concat(
        objs=[
            first_file_first_layer_gdf,
            first_file_second_layer_gdf,
        ],
        ignore_index=True,
    )


@fixture
def expected_gdb_gdf(
    first_file_second_layer_gdf: GeoDataFrame,
    second_file_second_layer_gdf: GeoDataFrame,
) -> GeoDataFrame:
    """Expected concatenation of gdb first layer gdfs."""
    return concat(
        objs=[
            first_file_second_layer_gdf,
            second_file_second_layer_gdf,
        ],
        ignore_index=True,
    )


@fixture
def expected_parallel_reader_for_files_closures(
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
) -> List:
    """Expected closures for for test__generate_parallel_reader_for_files."""
    return [
        True,
        "geometry",
        "first",
        fileGDB_schema,
        spark_to_pandas_mapping,
    ]


@fixture
def expected_parallel_reader_for_chunks_closures(
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
) -> List:
    """Expected closures for for test__generate_parallel_reader_for_chunks."""
    return [
        True,
        "geometry",
        fileGDB_schema,
        spark_to_pandas_mapping,
    ]


@fixture
def configuration_key() -> str:
    """Spark configuration key."""
    return "spark.sql.shuffle.partitions"


@fixture
def default_partitions() -> str:
    """Spark configuration key default value."""
    return "200"


@fixture
def expected_temporary_partitions() -> str:
    """Spark configuration key temporary value."""
    return "100"
