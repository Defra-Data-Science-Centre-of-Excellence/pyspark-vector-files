from functools import singledispatch
from pathlib import Path
from typing import Optional, Tuple, Union

from more_itertools import pairwise
from osgeo.ogr import DataSource, Layer, Open
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, explode, monotonically_increasing_id
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_vector_files._types import Chunks


def _get_paths(
    path: str, pattern: str, suffix: str, recursive: bool
) -> Tuple[str, ...]:
    """Returns full paths for all files in a path, with the given suffix."""
    _path = Path(path)

    if not _path.is_dir():
        raise NotADirectoryError(
            f'"{str(_path)}" is not a directory.',
        )

    _suffix = suffix.strip(".")

    if recursive:
        paths = tuple(str(path) for path in _path.rglob(f"{pattern}.{_suffix}"))
    else:
        paths = tuple(str(path) for path in _path.glob(f"{pattern}.{_suffix}"))

    if len(paths) == 0:
        raise FileNotFoundError(
            f'No files found for: path: "{path}", pattern: "{pattern}", suffix: "{suffix}", recursive: {recursive}.',  # noqa: B950
        )
    else:
        return paths


def _add_vsi_prefix(paths: Tuple[str, ...], vsi_prefix: str) -> Tuple[str, ...]:
    """Adds GDAL virtual file system prefix to the paths."""
    _vsi_prefix = vsi_prefix.strip("/")
    _paths = tuple(path.lstrip("/") for path in paths)
    return tuple("/" + _vsi_prefix + "//" + path for path in _paths)


def _get_data_sources(paths: Tuple[str, ...]) -> Tuple[DataSource, ...]:
    return tuple(Open(path) for path in paths)


def _get_data_source_layer_names(data_source: DataSource) -> Tuple[str, ...]:
    """Given a OGR DataSource, returns a sequence of layers."""
    return tuple(
        data_source.GetLayer(index).GetName()
        for index in range(data_source.GetLayerCount())
    )


@singledispatch
def _get_layer_name(
    layer_identifier: Optional[Union[str, int]],
    data_source: DataSource,
) -> Layer:
    """Returns the layer name, the layer name at an index, or the first layer name."""
    pass


@_get_layer_name.register
def _get_layer_name_none(
    layer_identifier: None,
    data_source: DataSource,
) -> Layer:
    """Returns the name of the first layer."""
    data_source_layer_names = _get_data_source_layer_names(
        data_source,
    )

    layer_name = data_source_layer_names[0]

    return layer_name


@_get_layer_name.register
def _get_layer_name_str(
    layer_identifier: str,
    data_source: DataSource,
) -> Layer:
    """Returns the given layer name, if that layer name exists."""
    data_source_layer_names = _get_data_source_layer_names(
        data_source,
    )

    if layer_identifier not in data_source_layer_names:
        raise ValueError(
            f"Expecting one of {data_source_layer_names} but received {layer_identifier}.",  # noqa B950
        )
    else:
        layer_name = layer_identifier

    return layer_name


@_get_layer_name.register
def _get_layer_name_int(
    layer_identifier: int,
    data_source: DataSource,
) -> Layer:
    """Returns the layer name at given index, if that index is valid."""
    data_source_layer_names = _get_data_source_layer_names(
        data_source,
    )

    number_of_layers = len(data_source_layer_names)

    if layer_identifier >= number_of_layers:
        raise ValueError(
            f"Expecting index between 0 and {number_of_layers} but received {layer_identifier}.",  # noqa B950
        )
    else:
        layer_name = data_source_layer_names[layer_identifier]

    return layer_name


def _get_layer_names(
    layer_identifier: Optional[Union[str, int]],
    data_sources: Tuple[DataSource, ...],
) -> Tuple[str, ...]:
    return tuple(
        _get_layer_name(layer_identifier, data_source=data_source)
        for data_source in data_sources
    )


def _get_feature_count(data_source: DataSource, layer_name: str) -> int:
    layer = data_source.GetLayer(layer_name)
    # ! Explicitly coerce `feat_count` to `int` to prevent `mypy` `[no-any-return]`
    feat_count: int = layer.GetFeatureCount()
    if feat_count == -1:
        feat_count = layer.GetFeatureCount(force=True)
    return feat_count


def _get_feature_counts(
    data_sources: Tuple[DataSource, ...],
    layer_names: Tuple[str, ...],
) -> Tuple[int, ...]:
    return tuple(
        _get_feature_count(data_source=data_source, layer_name=layer_name)
        for data_source, layer_name in zip(  # noqa: B905 no strict parameter in 3.8.10
            data_sources, layer_names
        )
    )


def _get_chunks(feature_count: int, ideal_chunk_size: int) -> Chunks:
    exclusive_range = range(0, feature_count, ideal_chunk_size)
    inclusive_range = tuple(exclusive_range) + (feature_count + 1,)
    range_pairs = pairwise(inclusive_range)
    return tuple(range_pairs)


def _get_sequence_of_chunks(
    feature_counts: Tuple[int, ...],
    ideal_chunk_size: int,
) -> Tuple[Chunks, ...]:
    return tuple(
        _get_chunks(feature_count=feature_count, ideal_chunk_size=ideal_chunk_size)
        for feature_count in feature_counts
    )


def _get_total_chunks(sequence_of_chunks: Tuple[Chunks, ...]) -> int:
    return sum(len(chunks) for chunks in sequence_of_chunks)


def _create_paths_sdf(
    spark: SparkSession,
    paths: Tuple[str, ...],
) -> SparkDataFrame:
    rows = tuple(Row(path=path) for path in paths)

    return spark.createDataFrame(
        data=rows,
        schema=StructType(
            [
                StructField("path", StringType()),
            ]
        ),
    )


def _create_chunks_sdf(
    spark: SparkSession,
    paths: Tuple[str, ...],
    layer_names: Tuple[str, ...],
    sequence_of_chunks: Tuple[Chunks, ...],
) -> SparkDataFrame:
    rows = tuple(
        Row(
            path=path,
            layer_name=layer_name,
            chunks=chunks,
        )
        for path, layer_name, chunks in zip(  # noqa: B905 no strict parameter in 3.8.10
            paths,
            layer_names,
            sequence_of_chunks,
        )
    )

    sdf = spark.createDataFrame(
        data=rows,
        schema=StructType(
            [
                StructField("path", StringType()),
                StructField("layer_name", StringType()),
                StructField("chunks", ArrayType(ArrayType(IntegerType()))),
            ]
        ),
    )

    return (
        sdf.withColumn("chunk", explode("chunks"))
        .withColumn("id", monotonically_increasing_id())
        .withColumn("start", col("chunk")[0])
        .withColumn("stop", col("chunk")[1])
        .drop("chunk", "chunks")
    )
