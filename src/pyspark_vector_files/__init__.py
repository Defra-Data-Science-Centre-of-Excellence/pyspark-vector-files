"""Read various spatial vector formats into a Spark DataFrame.

Basic usage
===========

Read the first layer from a file or files into a single Spark DataFrame:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
    )

Filename pattern matching
=========================

Read files that begin with "abc" into a single Spark DataFrame:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        pattern="abc*",
        suffix=".ext",
    )

Read files that end with four digits into a single Spark DataFrame:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        pattern="*[0-9][0-9][0-9][0-9]",
        suffix=".ext",
    )

For more information on pattern matching using Unix shell-style wildcards, see
Python's `fnmatch`_ module.

Reading files from nested folders
=================================

By default, the library will only look within the specified folder. To enable
recursive searching of subdirectories, use the `recursive` argument.

Example:
    Given the following folder structure::

        /path/to/files
        |    file_0.ext
        |    file_1.ext
        |
        |-- subfolder
        |        file_2.ext
        |        file_3.ext

    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
    )

    will read `file_0.ext` and `file_1.ext`, while

    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        recursive=True,
    )

    will read `file_0.ext`, `file_1.ext`, `subfolder/file_2.ext`, and
    `subfolder/file_3.ext`.


Reading layers
==============

Read a specific layer for a file or files, using layer name:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        layer_identifier="layer_name"
    )

or layer index:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        layer_identifier=1
    )

GDAL Virtual File Systems
=========================

Read compressed files using GDAL Virtual File Systems:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".gz",
        layer_identifier="layer_name",
        vsi_prefix="/vsigzip/",
    )

For more information, see `GDAL Virtual File Systems`_.

User-defined Schema
===================

By default, a schema will be generated from the first file in the folder. For a
single tabular dataset that has been partitioned across several files, this will
work fine.

However, it won't work for a list format like GML, as not every file will contain
the same fields. In this case, you can define a schema yourself. You will also need
to set the coerce_to_schema flag to True.

Example:
    >>> schema = StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )

    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        layer_identifier="layer_name",
        schema=schema,
        coerce_to_schema=True,
    )

Concurrency Strategy
====================

By default, the function will parallelise across files.

This should work well for single dataset that has been partitioned across several
files. Especially if it has been partition so that those individual files can be
comfortably read into memory on a single machine.

However, the function also provides a way of parallelising across chunks of rows
within a file or files.

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        concurrency_strategy="rows",
    )

By default, a chunk will consist of 1 million rows but you cab change this using the
ideal_chunk_size parameter.

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
        concurrency_strategy="rows",
        ideal_chunk_size=5_000_000,
    )

.. warning::
    Reading chunks adds a substantial overhead as files have to be opened to get a row
    count. The "rows" strategy should only be used for a single large file or a small
    number of large files.

.. _`fnmatch`: https://docs.python.org/3/library/fnmatch.html#module-fnmatch

.. _`GDAL Virtual File Systems`: https://gdal.org/user/virtual_file_systems.html

"""
from contextlib import contextmanager
from types import MappingProxyType
from typing import Iterator, Optional, Union

from numpy import float32, int32, int64, object0
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from pyspark_vector_files._files import (
    _add_vsi_prefix,
    _create_chunks_sdf,
    _create_paths_sdf,
    _get_data_sources,
    _get_feature_counts,
    _get_layer_names,
    _get_paths,
    _get_sequence_of_chunks,
    _get_total_chunks,
)
from pyspark_vector_files._parallel_reader import (
    _generate_parallel_reader_for_chunks,
    _generate_parallel_reader_for_files,
)
from pyspark_vector_files._schema import (
    _create_schema_for_chunks,
    _create_schema_for_files,
)
from pyspark_vector_files._types import ConcurrencyStrategy

# See:
# https://gdal.org/python/index.html
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
# https://numpy.org/doc/stable/reference/arrays.dtypes.html

OGR_TO_SPARK = MappingProxyType(
    {
        "Binary": BinaryType(),
        "Date": StringType(),
        "DateTime": StringType(),
        "Integer": IntegerType(),
        "IntegerList": ArrayType(IntegerType()),
        "Integer64": LongType(),
        "Integer64List": ArrayType(LongType()),
        "Real": FloatType(),
        "RealList": ArrayType(FloatType()),
        "String": StringType(),
        "StringList": ArrayType(StringType()),
        "Time": StringType(),
        "WideString": StringType(),
        "WideStringList": ArrayType(StringType()),
    }
)

SPARK_TO_PANDAS = MappingProxyType(
    {
        ArrayType(FloatType()): object0,
        ArrayType(IntegerType()): object0,
        ArrayType(LongType()): object0,
        ArrayType(StringType()): object0,
        BinaryType(): object0,
        FloatType(): float32,
        IntegerType(): int32,
        LongType(): int64,
        StringType(): object0,
    }
)


@contextmanager
def temporary_spark_context(
    configuration_key: str,
    new_configuration_value: str,
    spark: SparkSession = None,
) -> Iterator[SparkSession]:
    """Changes then resets spark configuration."""
    spark = spark if spark else SparkSession.getActiveSession()
    old_configuration_value = spark.conf.get(configuration_key)
    spark.conf.set(configuration_key, new_configuration_value)
    try:
        yield spark
    finally:
        spark.conf.set(configuration_key, old_configuration_value)


def read_vector_files(
    path: str,
    ogr_to_spark_type_map: MappingProxyType = OGR_TO_SPARK,
    pattern: str = "*",
    suffix: str = "*",
    recursive: bool = False,
    ideal_chunk_size: int = 1_000_000,
    geom_field_name: str = "geometry",
    geom_field_type: str = "Binary",
    coerce_to_schema: bool = False,
    spark_to_pandas_type_map: MappingProxyType = SPARK_TO_PANDAS,
    concurrency_strategy: str = "files",
    vsi_prefix: Optional[str] = None,
    schema: Optional[StructType] = None,
    layer_identifier: Optional[Union[str, int]] = None,
) -> SparkDataFrame:
    """Read vector file(s) into a Spark DataFrame.

    Args:
        path (str): Path to a folder of vector files.
        ogr_to_spark_type_map (MappingProxyType): A mapping of OGR to Spark data
            types. Defaults to OGR_TO_SPARK.
        pattern (str): A filename pattern. This will be passed to to `pathlib`'s
            `Path.glob` method. For more information, see
            https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob.
            Defaults to "*".
        suffix (str): A file extention pattern. This will be passed to to `pathlib`'s
            `Path.glob` method. For more information, see
            https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob.
            Defaults to "*".
        recursive (bool): If True, recursive globbing in enabled. For more
            information, see
            https://docs.python.org/3/library/pathlib.html#pathlib.Path.rglob.
            Defaults to False.
        ideal_chunk_size (int): The max number of rows to be read into a chunk.
            Defaults to 1_000_000.
        geom_field_name (str): The name of the geometry column. Defaults to
            "geometry".
        geom_field_type (str): The data type of the geometry column when it is
            passed to Spark. Defaults to "Binary".
        coerce_to_schema (bool): If True, all files or chunks will be forced to
            fit the supplied schema. Missing columns will be added and additional
            columns will be removed. Defaults to False.
        spark_to_pandas_type_map (MappingProxyType): A mapping of Spark to Pandas data
            types. Defaults to SPARK_TO_PANDAS.
        concurrency_strategy (str): The concurrency strategy to use, can be "files"
            or "chunks". Defaults to "files".
        vsi_prefix (str, optional): The GDAL virtual file system prefix(es) to use.
            For more information, see https://gdal.org/user/virtual_file_systems.html.
            Defaults to None.
        schema (StructType, optional): A user-defined Spark schema. Defaults to None.
        layer_identifier (Union[str, int], optional): A layer name or index. If
            None is given, the first layer will be return. Defaults to None.

    Returns:
        SparkDataFrame: [description]
    """
    _concurrency_strategy = ConcurrencyStrategy(concurrency_strategy)

    paths = _get_paths(
        path=path,
        pattern=pattern,
        suffix=suffix,
        recursive=recursive,
    )

    if vsi_prefix:
        paths = _add_vsi_prefix(
            paths=paths,
            vsi_prefix=vsi_prefix,
        )

    if _concurrency_strategy == ConcurrencyStrategy.FILES:

        number_of_partitions = len(paths)

        with temporary_spark_context(
            configuration_key="spark.sql.shuffle.partitions",
            new_configuration_value=str(number_of_partitions),
        ) as spark:

            df = _create_paths_sdf(
                spark=spark,
                paths=paths,
            )

            _schema = (
                schema
                if schema
                else _create_schema_for_files(
                    path=paths[0],
                    layer_identifier=layer_identifier,
                    ogr_to_spark_type_map=ogr_to_spark_type_map,
                    geom_field_name=geom_field_name,
                    geom_field_type=geom_field_type,
                )
            )

            parallel_read = _generate_parallel_reader_for_files(
                layer_identifier=layer_identifier,
                geom_field_name=geom_field_name,
                coerce_to_schema=coerce_to_schema,
                spark_to_pandas_type_map=spark_to_pandas_type_map,
                schema=_schema,
            )

            return (
                df.repartition(number_of_partitions, "path")
                .groupby("path")
                .applyInPandas(parallel_read, _schema)
            )

    else:
        data_sources = _get_data_sources(paths)

        layer_names = _get_layer_names(
            data_sources=data_sources,
            layer_identifier=layer_identifier,
        )

        feature_counts = _get_feature_counts(
            data_sources=data_sources,
            layer_names=layer_names,
        )

        sequence_of_chunks = _get_sequence_of_chunks(
            feature_counts=feature_counts,
            ideal_chunk_size=ideal_chunk_size,
        )

        number_of_partitions = _get_total_chunks(sequence_of_chunks)

        with temporary_spark_context(
            configuration_key="spark.sql.shuffle.partitions",
            new_configuration_value=str(number_of_partitions),
        ) as spark:

            df = _create_chunks_sdf(
                spark=spark,
                paths=paths,
                layer_names=layer_names,
                sequence_of_chunks=sequence_of_chunks,
            )

            _schema = (
                schema
                if schema
                else _create_schema_for_chunks(
                    data_source=data_sources[0],
                    layer_name=layer_names[0],
                    ogr_to_spark_type_map=ogr_to_spark_type_map,
                    geom_field_name=geom_field_name,
                    geom_field_type=geom_field_type,
                )
            )

            parallel_read = _generate_parallel_reader_for_chunks(
                geom_field_name=geom_field_name,
                coerce_to_schema=coerce_to_schema,
                spark_to_pandas_type_map=spark_to_pandas_type_map,
                schema=_schema,
            )

            return (
                df.repartition(number_of_partitions, "id")
                .groupby("id")
                .applyInPandas(parallel_read, _schema)
            )
