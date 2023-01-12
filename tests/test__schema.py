"""Tests for _pyspark module."""
from types import MappingProxyType
from typing import Tuple

import pytest
from osgeo.ogr import Layer, Open
from pyspark.sql.types import StructType

from pyspark_vector_files._schema import (
    _create_schema_for_chunks,
    _create_schema_for_files,
    _get_feature_schema,
    _get_layer,
    _get_property_names,
    _get_property_types,
)


@pytest.mark.parametrize(
    argnames=[
        "layer_name",
        "start",
        "stop",
        "expected_feature_count",
    ],
    argvalues=[
        ("first", None, None, 2),
        ("second", 0, 1, 1),
    ],
    ids=[
        "Whole layer",
        "Layer chunk",
    ],
)
def test__get_layer(
    first_fileGDB_path: str,
    layer_name: str,
    start: int,
    stop: int,
    expected_feature_count: int,
) -> None:
    """Returns expected layer name and feature count for each method."""
    data_source = Open(first_fileGDB_path)

    _layer: Layer = _get_layer(
        data_source=data_source,
        layer_name=layer_name,
        start=start,
        stop=stop,
    )

    feature_count = _layer.GetFeatureCount()

    assert feature_count == expected_feature_count


def test__get_property_names(
    first_fileGDB_path: str,
    layer_column_names: Tuple[str, ...],
) -> None:
    """Returns expected non-geometry field names."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayer()
    property_names = _get_property_names(layer=layer)
    assert property_names == layer_column_names[:2]


def test__get_property_types(first_fileGDB_path: str) -> None:
    """Returns expected non-geometry field types."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayer()
    property_types = _get_property_types(layer=layer)
    assert property_types == ((12, 0), (4, 0))  # (OFTInteger64, OFTString)


def test__get_feature_schema(
    first_fileGDB_path: str,
    fileGDB_schema: StructType,
    ogr_to_spark_mapping: MappingProxyType,
) -> None:
    """Returns expected Spark schema."""
    data_source = Open(first_fileGDB_path)
    layer = data_source.GetLayer()
    schema = _get_feature_schema(
        layer=layer,
        ogr_to_spark_type_map=ogr_to_spark_mapping,
        geom_field_name="geometry",
        geom_field_type=(8, 0),
    )
    assert schema == fileGDB_schema


def test__create_schema_for_chunks(
    first_fileGDB_path: str,
    fileGDB_schema: StructType,
    ogr_to_spark_mapping: MappingProxyType,
) -> None:
    """Returns expected Spark schema regardless of `_get_layer` function used."""
    data_source = Open(first_fileGDB_path)

    schema = _create_schema_for_chunks(
        data_source=data_source,
        layer_name="first",
        geom_field_name="geometry",
        geom_field_type=(8, 0),
        ogr_to_spark_type_map=ogr_to_spark_mapping,
    )
    assert schema == fileGDB_schema


def test__create_schema_for_files(
    first_fileGDB_path: str,
    fileGDB_schema: StructType,
    ogr_to_spark_mapping: MappingProxyType,
) -> None:
    """Returns expected Spark schema regardless of `_get_layer` function used."""
    schema = _create_schema_for_files(
        path=first_fileGDB_path,
        layer_identifier="first",
        geom_field_name="geometry",
        geom_field_type=(8, 0),
        ogr_to_spark_type_map=ogr_to_spark_mapping,
    )
    assert schema == fileGDB_schema
