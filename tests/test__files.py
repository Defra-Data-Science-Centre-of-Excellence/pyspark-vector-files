"""Tests for _pyspark module."""
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union

import pytest
from _pytest.fixtures import FixtureRequest
from chispa.dataframe_comparer import assert_df_equality
from osgeo.ogr import DataSource, Open
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pytest import raises

from pyspark_vector_files._files import (
    _add_vsi_prefix,
    _create_chunks_sdf,
    _create_paths_sdf,
    _get_chunks,
    _get_data_source_layer_names,
    _get_data_sources,
    _get_feature_count,
    _get_feature_counts,
    _get_layer_name,
    _get_layer_names,
    _get_paths,
    _get_sequence_of_chunks,
    _get_total_chunks,
)
from pyspark_vector_files._types import Chunks


@pytest.mark.parametrize(
    argnames=[
        "pattern",
        "expected_paths",
    ],
    argvalues=[
        ("*", "all_fileGDB_paths"),
        ("first*", "first_fileGDB_path"),
    ],
    ids=[
        "Star",
        "First star",
    ],
)
def test__get_paths(
    fileGDB_directory_path: Path,
    pattern: str,
    expected_paths: str,
    request: FixtureRequest,
) -> None:
    """Returns collection of FileGDB file paths."""
    paths = _get_paths(
        path=str(fileGDB_directory_path),
        pattern=pattern,
        suffix="gdb",
        recursive=False,
    )

    _expected_paths = request.getfixturevalue(expected_paths)

    if isinstance(_expected_paths, str):
        assert paths == (_expected_paths,)
    else:
        assert paths == _expected_paths


@pytest.mark.parametrize(
    argnames=[
        "vsi_prefix",
    ],
    argvalues=[
        ("/vsigzip/",),
        ("vsigzip",),
        ("/vsigzip",),
        ("vsigzip/",),
    ],
    ids=[
        "Wrapped by slashes",
        "No slashes",
        "Prefixed with slash",
        "Postfixed with slash",
    ],
)
def test__add_vsi_prefix(
    first_fileGDB_path: str,
    second_fileGDB_path: str,
    vsi_prefix: str,
) -> None:
    """VSI prefix is prepended to paths."""
    _paths = (first_fileGDB_path, second_fileGDB_path)
    prefixed_paths = _add_vsi_prefix(paths=_paths, vsi_prefix=vsi_prefix)
    assert prefixed_paths == (
        "/" + vsi_prefix.strip("/") + "/" + first_fileGDB_path,
        "/" + vsi_prefix.strip("/") + "/" + second_fileGDB_path,
    )


def test__get_data_sources(
    first_fileGDB_path: str,
) -> None:
    """Returns a tuple of DataSources."""
    data_sources = _get_data_sources(
        paths=(first_fileGDB_path,),
    )
    assert all(isinstance(data_source, DataSource) for data_source in data_sources)


def test__get_data_source_layer_names(first_fileGDB_path: str) -> None:
    """Returns layer names from dummy FileGDB."""
    data_source = Open(first_fileGDB_path)
    layer_names = _get_data_source_layer_names(
        data_source=data_source,
    )
    assert sorted(layer_names) == ["first", "second", "third"]


@pytest.mark.parametrize(
    argnames=[
        "layer_identifier",
        "expected_layer_name",
        "expected_exception",
    ],
    argvalues=[
        ("first", "first", does_not_raise()),
        ("fourth", None, raises(ValueError)),
        (0, "second", does_not_raise()),
        (3, None, raises(ValueError)),
        (None, "second", does_not_raise()),
    ],
    ids=[
        "Valid layer name",
        "Invalid layer name",
        "Valid layer index",
        "Invalid layer index",
        "None",
    ],
)
def test__get_layer_name(
    first_fileGDB_path: str,
    layer_identifier: Optional[Union[str, int]],
    expected_layer_name: Optional[str],
    expected_exception: ContextManager,
) -> None:
    """Returns given layer."""
    data_source = Open(first_fileGDB_path)
    with expected_exception:
        layer_name = _get_layer_name(
            layer_identifier,
            data_source=data_source,
        )
        assert layer_name == expected_layer_name


@pytest.mark.parametrize(
    argnames=[
        "layer_identifier",
        "expected_layer_name",
        "expected_exception",
    ],
    argvalues=[
        ("first", "first", does_not_raise()),
        ("fourth", None, raises(ValueError)),
        (0, "second", does_not_raise()),
        (3, None, raises(ValueError)),
        (None, "second", does_not_raise()),
    ],
    ids=[
        "Valid layer name",
        "Invalid layer name",
        "Valid layer index",
        "Invalid layer index",
        "None",
    ],
)
def test__get_layer_names(
    layer_identifier: Optional[Union[str, int]],
    fileGDB_data_source: DataSource,
    expected_layer_name: Optional[str],
    expected_exception: ContextManager,
) -> None:
    """Returns a tuple containing expected layer names."""
    with expected_exception:
        layer_names = _get_layer_names(
            layer_identifier=layer_identifier,
            data_sources=(fileGDB_data_source,),
        )
        assert layer_names == (expected_layer_name,)


def test__get_feature_count(
    fileGDB_data_source: DataSource,
) -> None:
    """0th layer in dummy FileGDB has 2 features."""
    feat_count = _get_feature_count(
        data_source=fileGDB_data_source,
        layer_name="first",
    )
    assert feat_count == 2


def test__get_feature_counts(
    fileGDB_data_source: DataSource,
) -> None:
    """Returns a tuple of feature counts."""
    feat_counts = _get_feature_counts(
        data_sources=(fileGDB_data_source,),
        layer_names=("first",),
    )
    assert feat_counts == (2,)


@pytest.mark.parametrize(
    argnames=[
        "feature_count",
        "ideal_chunk_size",
        "expected_chunks",
    ],
    argvalues=[
        (2, 2, "expected_single_chunk"),
        (2, 1, "expected_multiple_chunks"),
    ],
    ids=[
        "single chunk",
        "multiple chunks",
    ],
)
def test__get_chunks(
    feature_count: int,
    ideal_chunk_size: int,
    request: FixtureRequest,
    expected_chunks: str,
) -> None:
    """Returns expected Chunks."""
    chunks = _get_chunks(
        feature_count=feature_count,
        ideal_chunk_size=ideal_chunk_size,
    )
    assert chunks == request.getfixturevalue(expected_chunks)


@pytest.mark.parametrize(
    argnames=[
        "ideal_chunk_size",
        "expected_sequence_of_chunks",
    ],
    argvalues=[
        (2, "expected_sequence_containing_single_chunk"),
        (1, "expected_sequence_containing_multiple_chunks"),
    ],
    ids=[
        "Sequence containing single chunk",
        "Sequence containing multiple chunks",
    ],
)
def test__get_sequence_of_chunks(
    ideal_chunk_size: int,
    request: FixtureRequest,
    expected_sequence_of_chunks: str,
) -> None:
    """Returns a tuple of expected Chunks."""
    sequence_of_chunks = _get_sequence_of_chunks(
        feature_counts=(2,),
        ideal_chunk_size=ideal_chunk_size,
    )
    assert sequence_of_chunks == request.getfixturevalue(
        expected_sequence_of_chunks,
    )


def test___get_total_chunks(
    expected_sequence_of_chunks: Tuple[Chunks, ...],
) -> None:
    """Returns the total number of Chunks."""
    total_chunks = _get_total_chunks(
        sequence_of_chunks=expected_sequence_of_chunks,
    )
    assert total_chunks == 3


def test__create_paths_sdf(
    spark_context: SparkSession,
    first_fileGDB_path: str,
    expected_paths_sdf: SparkDataFrame,
) -> None:
    """Returns the expected SparkDataFrame of paths."""
    paths_sdf = _create_paths_sdf(
        spark=spark_context,
        paths=(first_fileGDB_path,),
    )
    assert_df_equality(paths_sdf, expected_paths_sdf)


@pytest.mark.parametrize(
    argnames=[
        "expected_sequence_of_chunks",
        "expected_chunks_sdf",
    ],
    argvalues=[
        ("expected_sequence_containing_single_chunk", "expected_single_chunk_sdf"),
        (
            "expected_sequence_containing_multiple_chunks",
            "expected_multiple_chunks_sdf",
        ),
    ],
    ids=[
        "Sequence containing single chunk",
        "Sequence containing multiple chunks",
    ],
)
def test__create_chunks_sdf(
    spark_context: SparkSession,
    first_fileGDB_path: str,
    request: FixtureRequest,
    expected_sequence_of_chunks: str,
    expected_chunks_sdf: str,
) -> None:
    """Returns the expected SparkDataFrame of Chunks."""
    chunks_sdf = _create_chunks_sdf(
        spark=spark_context,
        paths=(first_fileGDB_path,),
        layer_names=("first",),
        sequence_of_chunks=request.getfixturevalue(expected_sequence_of_chunks),
    )
    assert_df_equality(chunks_sdf, request.getfixturevalue(expected_chunks_sdf))
