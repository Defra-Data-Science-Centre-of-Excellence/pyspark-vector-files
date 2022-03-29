"""Tests for _pyspark module."""
from inspect import getsourcelines
from types import MappingProxyType
from typing import List, Optional, Tuple

import pytest
from osgeo.ogr import Open
from pandas import DataFrame as PandasDataFrame
from pandas.testing import assert_frame_equal, assert_series_equal
from pyspark.sql.types import DataType, StructType
from pytest import FixtureRequest
from shapely.geometry import Point
from shapely.wkb import loads

from pyspark_vector_files._parallel_reader import (
    _add_missing_columns,
    _coerce_columns_to_schema,
    _coerce_types_to_schema,
    _drop_additional_columns,
    _generate_parallel_reader_for_chunks,
    _generate_parallel_reader_for_files,
    _get_columns_names,
    _get_features,
    _get_field_details,
    _get_field_names,
    _get_geometry,
    _get_properties,
    _identify_additional_columns,
    _identify_missing_columns,
    _null_data_frame_from_schema,
    _pdf_from_vector_file,
    _pdf_from_vector_file_chunk,
)


def test__null_data_frame_from_schema(
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
    expected_null_data_frame: PandasDataFrame,
) -> None:
    """Returns an empty PDF with the expect column names and dtypes."""
    null_data_frame = _null_data_frame_from_schema(
        schema=fileGDB_schema,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )
    assert_frame_equal(
        left=null_data_frame,
        right=expected_null_data_frame,
    )


def test__get_properties(first_fileGDB_path: str) -> None:
    """Properties from 0th row from 0th layer."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayer()
    feature = layer.GetFeature(0)
    properties = _get_properties(feature)
    assert properties == (2, "C")


@pytest.mark.parametrize(
    argnames=["layer_name", "expected_geometry"],
    argvalues=[
        (
            "first",
            (
                bytearray(
                    b"\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # noqa: B950
                ),
            ),
        ),
        ("third", (None,)),
    ],
    ids=[
        "Has geometry column",
        "No geometry column",
    ],
)
def test__get_geometry(
    first_fileGDB_path: str,
    layer_name: str,
    expected_geometry: Tuple[Optional[bytearray]],
) -> None:
    """Geometry from 0th row from 0th layer."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayerByName(layer_name)
    feature = layer.GetFeature(0)
    geometry = _get_geometry(feature)
    assert geometry == expected_geometry


def test__get_features(first_fileGDB_path: str) -> None:
    """All fields from the 0th row of the 0th layer."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayer()
    features_generator = _get_features(layer)
    *properties, geometry = next(features_generator)
    shapely_object = loads(bytes(geometry))
    assert tuple(properties) == (2, "C")
    assert shapely_object == Point(1, 1)


def test__get_field_details(
    fileGDB_schema: StructType,
    fileGDB_schema_field_details: Tuple[Tuple[str, DataType], ...],
) -> None:
    """Field details from dummy FileGDB schema."""
    fields = _get_field_details(schema=fileGDB_schema)
    assert fields == fileGDB_schema_field_details


def test__get_field_names(
    fileGDB_schema_field_details: Tuple[Tuple[str, DataType], ...],
    layer_column_names: Tuple[str, ...],
) -> None:
    """Field names from dummy FileGDB schema details."""
    field_names = _get_field_names(schema_field_details=fileGDB_schema_field_details)
    assert field_names == layer_column_names


def test__get_columns_names(
    first_file_first_layer_pdf: PandasDataFrame,
    layer_column_names: Tuple[str, ...],
) -> None:
    """Column names from pandas version of dummy first layer."""
    column_names = _get_columns_names(
        pdf=first_file_first_layer_pdf,
    )
    assert column_names == layer_column_names


@pytest.mark.parametrize(
    argnames=[
        "column_names",
        "expected_mask",
    ],
    argvalues=[
        ("layer_column_names", (False, False, False)),
        ("layer_column_names_missing_column", (False, True, False)),
    ],
    ids=[
        "Same column names",
        "Missing column name",
    ],
)
def test__identify_missing_columns(
    layer_column_names: Tuple[str, ...],
    request: FixtureRequest,
    column_names: str,
    expected_mask: Tuple[bool, ...],
) -> None:
    """Missing column is identified as `True`."""
    missing_columns = _identify_missing_columns(
        schema_field_names=layer_column_names,
        column_names=request.getfixturevalue(column_names),
    )
    assert missing_columns == expected_mask


@pytest.mark.parametrize(
    argnames=[
        "column_names",
        "expected_mask",
    ],
    argvalues=[
        ("layer_column_names", (False, False, False)),
        ("layer_column_names_additional_column", (False, False, False, True)),
    ],
    ids=[
        "Same column names",
        "Additional column name",
    ],
)
def test__identify_additional_columns(
    layer_column_names: Tuple[str, ...],
    request: FixtureRequest,
    column_names: str,
    expected_mask: Tuple[bool, ...],
) -> None:
    """Additional column is identified as `True`."""
    additional_columns = _identify_additional_columns(
        schema_field_names=layer_column_names,
        column_names=request.getfixturevalue(column_names),
    )
    assert additional_columns == expected_mask


def test__drop_additional_columns(
    first_file_first_layer_pdf_with_additional_column: PandasDataFrame,
    layer_column_names_additional_column: Tuple[bool, ...],
    first_file_first_layer_pdf: PandasDataFrame,
) -> None:
    """Additional column is removed."""
    pdf = _drop_additional_columns(
        pdf=first_file_first_layer_pdf_with_additional_column,
        column_names=layer_column_names_additional_column,
        additional_columns=(False, False, False, True),
    )
    assert_frame_equal(
        left=pdf,
        right=first_file_first_layer_pdf,
    )


def test__coerce_types_to_schema(
    fileGDB_schema_field_details: Tuple[Tuple[str, DataType], ...],
    spark_to_pandas_mapping: MappingProxyType,
    first_file_first_layer_pdf_with_wrong_types: PandasDataFrame,
    first_file_first_layer_pdf: PandasDataFrame,
) -> None:
    """Returns a PDF with the expected dtypes."""
    coerced_pdf = _coerce_types_to_schema(
        pdf=first_file_first_layer_pdf_with_wrong_types,
        schema_field_details=fileGDB_schema_field_details,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )
    assert_frame_equal(
        left=coerced_pdf,
        right=first_file_first_layer_pdf,
    )


def test__add_missing_columns(
    first_file_first_layer_pdf_with_missing_column: PandasDataFrame,
    fileGDB_schema_field_details: Tuple[Tuple[str, DataType], ...],
    spark_to_pandas_mapping: MappingProxyType,
    layer_column_names: Tuple[str, ...],
    layer_column_names_missing_column: Tuple[bool, ...],
    first_file_first_layer_pdf: PandasDataFrame,
) -> None:
    """The same columns, with the same data types, in the same order."""
    pdf = _add_missing_columns(
        pdf=first_file_first_layer_pdf_with_missing_column,
        schema_field_details=fileGDB_schema_field_details,
        missing_columns=layer_column_names_missing_column,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
        schema_field_names=layer_column_names,
    )
    assert_series_equal(
        left=pdf.dtypes,
        right=first_file_first_layer_pdf.dtypes,
    )


@pytest.mark.parametrize(
    argnames=[
        "pdf",
    ],
    argvalues=[
        ("first_file_first_layer_pdf",),
        ("first_file_first_layer_pdf_with_missing_column",),
        ("first_file_first_layer_pdf_with_additional_column",),
    ],
    ids=[
        "Same PDF",
        "PDF with missing column",
        "PDF with additional column",
    ],
)
def test__coerce_columns_to_schema(
    request: FixtureRequest,
    pdf: str,
    fileGDB_schema_field_details: Tuple[Tuple[str, DataType], ...],
    spark_to_pandas_mapping: MappingProxyType,
    first_file_first_layer_pdf: PandasDataFrame,
) -> None:
    """Missing columns are added and additional columns removed."""
    coerced_pdf = _coerce_columns_to_schema(
        pdf=request.getfixturevalue(pdf),
        schema_field_details=fileGDB_schema_field_details,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )
    if pdf == "first_file_first_layer_pdf_with_missing_column":
        assert_series_equal(
            left=coerced_pdf.dtypes,
            right=first_file_first_layer_pdf.dtypes,
        )
    else:
        assert_frame_equal(
            left=coerced_pdf,
            right=first_file_first_layer_pdf,
        )


@pytest.mark.parametrize(
    argnames=[
        "path",
        "layer_identifier",
        "coerce_to_schema",
        "expected_pdf_name",
    ],
    argvalues=[
        ("erroneous_file_path", "first", False, "expected_null_data_frame"),
        (
            "first_fileGDB_path",
            "erroneous layer identifier",
            False,
            "expected_null_data_frame",
        ),
        ("fileGDB_wrong_types_path", "first", True, "first_file_first_layer_pdf"),
        ("first_fileGDB_path", "first", False, "first_file_first_layer_pdf"),
    ],
    ids=[
        "Path doesn't exist",
        "Layer doesn't exist",
        "Needs to be coerced",
        "Doesn't need to be coerced",
    ],
)
def test__pdf_from_vector_file(
    request: FixtureRequest,
    path: str,
    layer_identifier: str,
    coerce_to_schema: bool,
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
    expected_pdf_name: str,
) -> None:
    """Returns the expected df or a null df if path or layer don't exist."""
    pdf = _pdf_from_vector_file(
        path=request.getfixturevalue(path),
        layer_identifier=layer_identifier,
        geom_field_name="geometry",
        coerce_to_schema=coerce_to_schema,
        schema=fileGDB_schema,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )
    pdf["geometry"] = pdf["geometry"].apply(lambda row: loads(bytes(row)))

    expected_pdf: PandasDataFrame = request.getfixturevalue(expected_pdf_name)
    expected_pdf["geometry"] = expected_pdf["geometry"].apply(
        lambda row: loads(bytes(row))
    )

    assert_frame_equal(
        left=pdf,
        right=expected_pdf,
    )


@pytest.mark.parametrize(
    argnames=[
        "path",
        "layer_name",
        "start",
        "stop",
        "coerce_to_schema",
        "expected_pdf_name",
    ],
    argvalues=[
        ("erroneous_file_path", "first", None, None, False, "expected_null_data_frame"),
        (
            "first_fileGDB_path",
            "erroneous layer name",
            None,
            None,
            False,
            "expected_null_data_frame",
        ),
        ("fileGDB_wrong_types_path", "first", 0, 2, True, "first_file_first_layer_pdf"),
        ("first_fileGDB_path", "first", 0, 2, False, "first_file_first_layer_pdf"),
        (
            "first_fileGDB_path",
            "first",
            0,
            1,
            False,
            "first_file_first_layer_pdf_first_row",
        ),
    ],
    ids=[
        "Path doesn't exist",
        "Layer doesn't exist",
        "Needs to be coerced",
        "Whole dataset, doesn't need to be coerced",
        "Chunk, doesn't need to be coerced",
    ],
)
def test__pdf_from_vector_file_chunk(
    request: FixtureRequest,
    path: str,
    layer_name: str,
    start: int,
    stop: int,
    coerce_to_schema: bool,
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
    expected_pdf_name: str,
) -> None:
    """Returns the expected df or a null df if path or layer don't exist."""
    pdf = _pdf_from_vector_file_chunk(
        path=request.getfixturevalue(path),
        layer_name=layer_name,
        start=start,
        stop=stop,
        geom_field_name="geometry",
        coerce_to_schema=coerce_to_schema,
        schema=fileGDB_schema,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )
    pdf["geometry"] = pdf["geometry"].apply(lambda row: loads(bytes(row)))

    expected_pdf: PandasDataFrame = request.getfixturevalue(expected_pdf_name)
    expected_pdf["geometry"] = expected_pdf["geometry"].apply(
        lambda row: loads(bytes(row))
    )

    assert_frame_equal(
        left=pdf,
        right=expected_pdf,
    )


def test__generate_parallel_reader_for_files(
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
    expected_parallel_reader_for_files: Tuple[List[str], int],
) -> None:
    """Returns the expected source code."""
    parallel_reader = _generate_parallel_reader_for_files(
        layer_identifier="first",
        geom_field_name="geometry",
        coerce_to_schema=True,
        schema=fileGDB_schema,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )

    assert getsourcelines(parallel_reader) == expected_parallel_reader_for_files


def test__generate_parallel_reader_for_chunks(
    fileGDB_schema: StructType,
    spark_to_pandas_mapping: MappingProxyType,
    expected_parallel_reader_for_chunks: Tuple[List[str], int],
) -> None:
    """Returns the expected source code."""
    parallel_reader = _generate_parallel_reader_for_chunks(
        geom_field_name="geometry",
        coerce_to_schema=True,
        schema=fileGDB_schema,
        spark_to_pandas_type_map=spark_to_pandas_mapping,
    )

    assert getsourcelines(parallel_reader) == expected_parallel_reader_for_chunks
