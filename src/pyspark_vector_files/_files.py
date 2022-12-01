from functools import singledispatch
from glob import glob
from pathlib import Path
from typing import Optional, Sequence, Tuple, Union

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

# vsi lookup dict
vsi_lookup = {
    (".zip",): "/vsizip/",
    (".gz",): "/vsigzip/",
    (".tar",): "/vsitar/",
    (".tgz",): "/vsitar/",
    (
        ".tar",
        ".gz",
    ): "/vsitar/",
}


def is_http(
    file_path: str,
) -> bool:
    """Check if a path is a URL and whether its valid."""
    if not file_path.startswith("http"):
        return False
    elif "*" in file_path:
        raise ValueError("URLs cannot contain wildcards.")
    else:
        return True


def prefix_path(
    file_path: str,
) -> str:
    """Prefix a path with the correct GDAL prefix, if required.

    Args:
        file_path (str): A file path without required prefixes.

    Returns:
        str: A file path with required prefixes.
    """
    _file_path = file_path
    if file_path.startswith("http"):
        _file_path = f"/vsicurl/{_file_path}"

    _suffixes = tuple(Path(file_path).suffixes)
    prefix = vsi_lookup.get(_suffixes, "")
    _file_path = f"{prefix}{_file_path}"
    return _file_path


def prefix_paths(paths: Sequence[str]) -> Tuple[str, ...]:
    """Run prefix_path over a list of paths."""
    return tuple(prefix_path(path) for path in paths)


def _process_path(
    path: str,
    pattern: str,
    suffix: str,
    recursive: bool,
) -> str:
    """Flexibly construct a path from different elements."""
    if not path.endswith("/"):
        return path
    else:
        return f"{path}{pattern}{suffix}"


def _get_paths(
    path: Union[str, Path],
    pattern: str,
    suffix: str,
    recursive: bool,
) -> Tuple[str, ...]:
    """Process a given path."""
    if str(path).startswith("http") and isinstance(path, Path):
        raise ValueError("URLs must be provided as a string.")

    _path = str(path)

    processed_path = _process_path(
        path=_path,
        pattern=pattern,
        suffix=suffix,
        recursive=recursive,
    )

    if is_http(processed_path):
        paths = [processed_path]
    else:
        paths = glob(processed_path)
    if not paths:
        raise ValueError("Pattern matching has not returned any paths.")

    prefixed_paths = prefix_paths(paths)
    return prefixed_paths


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
        for data_source, layer_name in zip(data_sources, layer_names)
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
        for path, layer_name, chunks in zip(
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
