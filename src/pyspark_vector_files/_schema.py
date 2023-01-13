from types import MappingProxyType
from typing import Optional, Tuple, Union

from osgeo.ogr import DataSource, Layer, Open
from pyspark.sql.types import StructField, StructType

from pyspark_vector_files._files import _get_layer_name


def _get_layer(
    data_source: DataSource,
    layer_name: str,
    start: Optional[int],
    stop: Optional[int],
) -> Layer:
    """Returns a GDAL Layer from a SQL statement, name or index, or 0th layer."""
    # ! "S608: Possible SQL injection vector through string-based query
    # ! construction" is turned off here because `_get_layer_name` will
    # ! raise an error if the user supplied layer doesn't exist and
    # ! `start` and `stop` are generated within the function.
    if isinstance(start, int) and isinstance(stop, int):
        sql = f'SELECT * from "{layer_name}" WHERE FID >= {start} AND FID < {stop}'  # noqa: S608, B950
    else:
        sql = f'SELECT * from "{layer_name}"'  # noqa: S608

    return data_source.ExecuteSQL(sql)


def _get_property_names(layer: Layer) -> Tuple[str, ...]:
    """Given a GDAL Layer, return the non-geometry field names."""
    layer_definition = layer.GetLayerDefn()
    return tuple(
        layer_definition.GetFieldDefn(index).GetName()
        for index in range(layer_definition.GetFieldCount())
    )


def _get_property_types(layer: Layer) -> Tuple[Tuple[int, int], ...]:
    """Given a GDAL Layer, return the non-geometry field types."""
    layer_definition = layer.GetLayerDefn()
    field_definitions = tuple(
        layer_definition.GetFieldDefn(index)
        for index in range(layer_definition.GetFieldCount())
    )
    return tuple(
        (field_definition.GetType(), field_definition.GetSubType())
        for field_definition in field_definitions
    )


def _get_feature_schema(
    layer: Layer,
    ogr_to_spark_type_map: MappingProxyType,
    geom_field_name: str,
    geom_field_type: Tuple[int, int],
) -> StructType:
    """Given a GDAL Layer and a data type mapping, return a PySpark DataFrame schema."""
    property_names = _get_property_names(layer=layer)
    property_types = _get_property_types(layer=layer)
    property_struct_fields = [
        StructField(field_name, ogr_to_spark_type_map[field_type])
        for field_name, field_type in zip(  # noqa: B905 no strict parameter in 3.8.10
            property_names, property_types
        )
    ]
    geometry_struct_field = [
        StructField(geom_field_name, ogr_to_spark_type_map[geom_field_type])
    ]
    return StructType(property_struct_fields + geometry_struct_field)


def _create_schema_for_files(
    path: str,
    layer_identifier: Optional[Union[str, int]],
    geom_field_name: str,
    geom_field_type: Tuple[int, int],
    ogr_to_spark_type_map: MappingProxyType,
) -> StructType:
    """Returns a schema for a given layer in the first file in a list of file paths."""
    data_source = Open(path)

    layer_name = _get_layer_name(
        # ! first argument to singledispatch function must be positional
        layer_identifier,
        data_source=data_source,
    )

    layer = _get_layer(
        data_source=data_source,
        layer_name=layer_name,
        start=None,
        stop=None,
    )

    return _get_feature_schema(
        layer=layer,
        ogr_to_spark_type_map=ogr_to_spark_type_map,
        geom_field_name=geom_field_name,
        geom_field_type=geom_field_type,
    )


def _create_schema_for_chunks(
    data_source: Optional[DataSource],
    layer_name: str,
    geom_field_name: str,
    geom_field_type: Tuple[int, int],
    ogr_to_spark_type_map: MappingProxyType,
) -> StructType:
    """Returns a schema for a given layer in the first file in a list of file paths."""
    layer = _get_layer(
        data_source=data_source,
        layer_name=layer_name,
        start=None,
        stop=None,
    )

    return _get_feature_schema(
        layer=layer,
        ogr_to_spark_type_map=ogr_to_spark_type_map,
        geom_field_name=geom_field_name,
        geom_field_type=geom_field_type,
    )
