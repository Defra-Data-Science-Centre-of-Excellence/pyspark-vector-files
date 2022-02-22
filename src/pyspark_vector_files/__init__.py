"""Read various spatial vector formats into a Spark DataFrame.

Basic usage
===========

Read the first layer from a file or files into a single Spark DataFrame:

Example:
    >>> sdf = read_vector_files(
        path="/path/to/files/",
        suffix=".ext",
    )

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

By default, a chunk will consist of 3 million rows but you cab change this using the
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
    ideal_chunk_size: int = 3_000_000,
    geom_field_name: str = "geometry",
    geom_field_type: str = "Binary",
    coerce_to_schema: bool = False,
    spark_to_pandas_type_map: MappingProxyType = SPARK_TO_PANDAS,
    vsi_prefix: Optional[str] = None,
    schema: StructType = None,
    layer_identifier: Optional[Union[str, int]] = None,
    concurrency_strategy: str = "files",
) -> SparkDataFrame:
    """Read vector file(s) into a Spark DataFrame.

    Args:
        path (str): [description]
        ogr_to_spark_type_map (MappingProxyType): [description]. Defaults
            to OGR_TO_SPARK.
        pattern (str): [description]. defaults to "*".
        suffix (str): [description]. Defaults to "*".
        recursive (bool): [description]. Defaults to False.
        ideal_chunk_size (int): [description]. Defaults to 3_000_000.
        geom_field_name (str): [description]. Defaults to "geometry".
        geom_field_type (str): [description]. Defaults to "Binary".
        coerce_to_schema (bool): [description]. Defaults to False.
        spark_to_pandas_type_map (MappingProxyType): [description]. Defaults
            to SPARK_TO_PANDAS.
        vsi_prefix (str, optional): [description]. Defaults to None.
        schema (StructType): [description]. Defaults to None.
        layer_identifier (str, optional): [description]. Defaults to None.
        concurrency_strategy (str): [description]. Defaults to "files".

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
