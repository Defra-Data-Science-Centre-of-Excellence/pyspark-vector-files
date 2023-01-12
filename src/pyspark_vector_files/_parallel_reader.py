from itertools import compress
from types import MappingProxyType
from typing import Callable, Generator, Optional, Tuple, Union

from osgeo.ogr import Feature, Layer, Open, wkbNDR
from pandas import DataFrame as PandasDataFrame
from pandas import Series
from pyspark.sql.types import DataType, StructType

from pyspark_vector_files._files import _get_layer_name
from pyspark_vector_files._schema import _get_layer, _get_property_names


def _null_data_frame_from_schema(
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Generates an empty DataFrame that fits the schema."""
    return PandasDataFrame(
        data={
            field.name: Series(dtype=spark_to_pandas_type_map[field.dataType])
            for field in schema.fields
        }
    )


def _get_properties(feature: Feature) -> Tuple:
    """Given a GDAL Feature, return the non-geometry fields."""
    return tuple(field for field in feature)


def _get_geometry(feature: Feature) -> Tuple[Optional[bytearray]]:
    """Given a GDAL Feature, return the geometry field, if there is one."""
    geometry = feature.GetGeometryRef()
    if geometry:
        return (geometry.ExportToWkb(wkbNDR),)
    else:
        return (None,)


def _get_features(layer: Layer) -> Generator:
    """Given a GDAL Layer, return the all fields."""
    return ((_get_properties(feature) + _get_geometry(feature)) for feature in layer)


def _get_field_details(schema: StructType) -> Tuple[Tuple[str, DataType], ...]:
    """Returns fields from schema."""
    return tuple((field.name, field.dataType) for field in schema.fields)


def _get_field_names(
    schema_field_details: Tuple[Tuple[str, DataType], ...],
) -> Tuple[str, ...]:
    """Returns field names from schema fields."""
    return tuple(field[0] for field in schema_field_details)


def _get_columns_names(
    pdf: PandasDataFrame,
) -> Tuple[str, ...]:
    """Returns columns names from DataFrame."""
    return tuple(column for column in pdf.columns)


def _identify_missing_columns(
    schema_field_names: Tuple[str, ...],
    column_names: Tuple[str, ...],
) -> Tuple[bool, ...]:
    """Returns boolean mask of schema field names not in column names."""
    return tuple(field_name not in column_names for field_name in schema_field_names)


def _identify_additional_columns(
    schema_field_names: Tuple[str, ...],
    column_names: Tuple[str, ...],
) -> Tuple[bool, ...]:
    """Returns boolean mask of column names not in schema field names."""
    return tuple(column not in schema_field_names for column in column_names)


def _drop_additional_columns(
    pdf: PandasDataFrame,
    column_names: Tuple,
    additional_columns: Tuple,
) -> PandasDataFrame:
    """Removes additional columns from pandas DataFrame."""
    # ! columns has to be a list
    to_drop = list(compress(column_names, additional_columns))
    return pdf.drop(columns=to_drop)


def _coerce_types_to_schema(
    pdf: PandasDataFrame,
    schema_field_details: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    for column_name, data_type in schema_field_details:
        pdf[column_name] = pdf[column_name].astype(spark_to_pandas_type_map[data_type])
    return pdf


def _add_missing_columns(
    pdf: PandasDataFrame,
    schema_field_names: Tuple,
    missing_columns: Tuple,
    schema_field_details: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Adds missing fields to pandas DataFrame."""
    to_add = compress(schema_field_details, missing_columns)
    missing_column_series = tuple(
        Series(
            name=field_name,
            dtype=spark_to_pandas_type_map[field_type],
        )
        for field_name, field_type in to_add
    )
    pdf_plus_missing_columns = pdf.append(missing_column_series)
    reindexed_pdf = pdf_plus_missing_columns.reindex(columns=schema_field_names)
    coerced_pdf = _coerce_types_to_schema(
        pdf=reindexed_pdf,
        schema_field_details=schema_field_details,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
    )
    return coerced_pdf


def _coerce_columns_to_schema(
    pdf: PandasDataFrame,
    schema_field_details: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Adds missing fields or removes additional columns to match schema."""
    schema_field_names = _get_field_names(
        schema_field_details=schema_field_details,
    )

    column_names = _get_columns_names(
        pdf=pdf,
    )

    if column_names == schema_field_names:
        return pdf

    missing_columns = _identify_missing_columns(
        schema_field_names=schema_field_names,
        column_names=column_names,
    )

    additional_columns = _identify_additional_columns(
        schema_field_names=schema_field_names,
        column_names=column_names,
    )

    if any(missing_columns) and any(additional_columns):
        pdf_minus_additional_columns = _drop_additional_columns(
            pdf=pdf,
            column_names=column_names,
            additional_columns=additional_columns,
        )
        return _add_missing_columns(
            pdf=pdf_minus_additional_columns,
            schema_field_details=schema_field_details,
            missing_columns=missing_columns,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
            schema_field_names=schema_field_names,
        )

    elif any(missing_columns):
        return _add_missing_columns(
            pdf=pdf,
            schema_field_details=schema_field_details,
            missing_columns=missing_columns,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
            schema_field_names=schema_field_names,
        )

    else:
        return _drop_additional_columns(
            pdf=pdf,
            column_names=column_names,
            additional_columns=additional_columns,
        )


def _coerce_pdf_to_schema(
    pdf: PandasDataFrame,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    schema_field_details = _get_field_details(schema=schema)

    coerced_columns_pdf = _coerce_columns_to_schema(
        pdf=pdf,
        schema_field_details=schema_field_details,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
    )

    return _coerce_types_to_schema(
        pdf=coerced_columns_pdf,
        schema_field_details=schema_field_details,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
    )


def _pdf_from_layer(
    layer: Layer,
    geom_field_name: str,
) -> PandasDataFrame:
    features_generator = _get_features(layer=layer)
    feature_names = _get_property_names(layer=layer) + (geom_field_name,)
    return PandasDataFrame(data=features_generator, columns=feature_names)


def _pdf_from_vector_file(
    path: str,
    layer_identifier: Optional[Union[str, int]],
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Given a file path and layer, returns a pandas DataFrame."""
    data_source = Open(path)

    if data_source is None:
        return _null_data_frame_from_schema(
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    try:
        layer_name = _get_layer_name(
            # ! first argument to singledispatch function must be positional
            layer_identifier,
            data_source=data_source,
        )
    except ValueError:
        return _null_data_frame_from_schema(
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    layer = _get_layer(
        data_source=data_source,
        layer_name=layer_name,
        start=None,
        stop=None,
    )

    pdf = _pdf_from_layer(
        layer=layer,
        geom_field_name=geom_field_name,
    )

    if coerce_to_schema:
        return _coerce_pdf_to_schema(
            pdf=pdf,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    return pdf


def _pdf_from_vector_file_chunk(
    path: str,
    layer_name: str,
    start: int,
    stop: int,
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Given a file path and layer, returns a pandas DataFrame."""
    data_source = Open(path)

    if data_source is None:
        return _null_data_frame_from_schema(
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    layer = _get_layer(
        data_source=data_source,
        layer_name=layer_name,
        start=start,
        stop=stop,
    )

    if layer is None:
        return _null_data_frame_from_schema(
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    pdf = _pdf_from_layer(
        layer=layer,
        geom_field_name=geom_field_name,
    )

    if coerce_to_schema:
        return _coerce_pdf_to_schema(
            pdf=pdf,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    return pdf


def _generate_parallel_reader_for_files(
    layer_identifier: Optional[Union[str, int]],
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> Callable[[PandasDataFrame], PandasDataFrame]:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a pandas_udf compatible version of _pdf_from_vector_file."""
        return _pdf_from_vector_file(
            path=str(pdf["path"][0]),
            layer_identifier=layer_identifier,
            geom_field_name=geom_field_name,
            coerce_to_schema=coerce_to_schema,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    return _


def _generate_parallel_reader_for_chunks(
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> Callable[[PandasDataFrame], PandasDataFrame]:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a pandas_udf compatible version of _pdf_from_vector_file_chunk."""
        return _pdf_from_vector_file_chunk(
            path=str(pdf["path"][0]),
            layer_name=str(pdf["layer_name"][0]),
            start=int(pdf["start"][0]),
            stop=int(pdf["stop"][0]),
            geom_field_name=geom_field_name,
            coerce_to_schema=coerce_to_schema,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    return _


# def _set_geometry_column_metadata(
#     df: DataFrame,
#     geometry_column_name: str,
#     layer: Layer,
# ) -> None:
#     df.schema[geometry_column_name].metadata = {
#         "crs": layer.GetSpatialRef().ExportToWkt(),
#         "encoding": "WKT",
#         "bbox": layer.GetExtent(),
#     }
