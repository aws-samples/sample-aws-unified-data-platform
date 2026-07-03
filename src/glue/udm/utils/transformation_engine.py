# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    initcap,
    length,
    lit,
    lower,
    substring,
    upper,
)

from .job_utils import convert_namespace_to_dict
from .transformations.array_transformations import ArrayTransformations
from .transformations.business_transformations import BusinessTransformations
from .transformations.core_transformations import CoreTransformations
from .transformations.data_quality import DataQuality
from .transformations.date_transformations import DateTransformations
from .transformations.metadata_extractor import MetadataExtractor
from .transformations.nested_data import NestedDataTransformations
from .transformations.s3_transformations import S3Transformations

logger = logging.getLogger(__name__)


def apply_rule_based_transformations(
    df: DataFrame,
    config: dict,
    select_columns: bool = False,
    target_table_schema: dict = None,
) -> DataFrame:
    """
    Apply transformations based on simplified configuration.

    Args:
        df: Input DataFrame
        config: Configuration dict
        select_columns: Whether to automatically select only configured columns
        target_table_schema: Dict of column_name -> data_type from target table
    """
    # Convert all SimpleNamespace objects to dicts recursively
    config = convert_namespace_to_dict(config)
    defaults = config.get("defaults", {})
    columns = config.get("columns", {})

    # Store target schema for later alignment (after transformations)

    for field_name, field_config in columns.items():
        field_type = field_config.get("type")
        # Get defaults for field type
        type_defaults = defaults.get(field_type, {})

        # Handle column renaming if rename_from is specified
        rename_from = field_config.get("rename_from")
        if rename_from and rename_from in df.columns:
            df = CoreTransformations.rename_column(df, rename_from, field_name)

        # Check if flattening is needed (source_path indicates flattening)
        source_path = field_config.get("source_path")

        if source_path:
            if isinstance(source_path, list):
                # Multiple paths - use coalesce for fallback
                temp_cols = []

                for i, path in enumerate(source_path):
                    temp_col = f"{field_name}_temp_{i}"
                    try:
                        df = NestedDataTransformations.flatten_column(
                            df, temp_col, path, field_type
                        )
                        temp_cols.append(col(temp_col))
                    except Exception as e:
                        logger.debug(
                            "Path %s not found for field %s, trying next path: %s",  # pylint: disable=line-too-long
                            path, field_name, e
                        )

                if temp_cols:
                    df = df.withColumn(field_name, coalesce(*temp_cols))

                    # Final extraction complete
                else:
                    df = df.withColumn(field_name, lit(None))

                # Clean up temp columns
                temp_col_names = [
                    f"{field_name}_temp_{i}" for i in range(len(source_path))
                ]
                existing_temp_cols = [
                    col_name for col_name in temp_col_names if col_name in df.columns
                ]
                if existing_temp_cols:
                    df = df.drop(*existing_temp_cols)
            else:
                # Single path
                try:
                    df = NestedDataTransformations.flatten_column(
                        df, field_name, source_path, field_type
                    )

                    # Single path extraction complete

                except Exception:
                    # If source path doesn't exist, create column with null values
                    df = df.withColumn(field_name, lit(None))

        # Handle different field types
        if field_type == "string":
            missing_default = field_config.get(
                "missing_value_default", type_defaults.get("missing_value_default")
            )
            if missing_default:
                missing_config = {
                    "default_value": missing_default,
                    "data_type": "string",
                }
                df = DataQuality.handle_missing_values(df, field_name, missing_config)

            corrections = field_config.get("corrections")
            if corrections:
                df = CoreTransformations.apply_corrections_mapping(
                    df, field_name, corrections
                )

            # Handle case transformations
            if field_config.get("lowercase"):
                df = df.withColumn(field_name, lower(col(field_name)))
            elif field_config.get("uppercase"):
                df = df.withColumn(field_name, upper(col(field_name)))
            elif field_config.get("title_case"):
                df = df.withColumn(field_name, initcap(col(field_name)))
            elif field_config.get("capitalize"):
                # Capitalize only first letter of entire string
                df = df.withColumn(
                    field_name,
                    concat(
                        upper(substring(col(field_name), 1, 1)),
                        lower(substring(col(field_name), 2, length(col(field_name)))),
                    ),
                )
            # Apply string cleaning unless skip_cleaning is true
            if not field_config.get("skip_cleaning"):
                df = CoreTransformations.string_cleaning(df, [field_name])

        elif field_type in ["integer", "float", "double"]:
            missing_default = field_config.get(
                "missing_value_default", type_defaults.get("missing_value_default")
            )

            if not field_config.get("skip_cleaning"):
                missing_config = {
                    "default_value": missing_default,
                    "data_type": field_type,
                }
                df = DataQuality.handle_missing_values(df, field_name, missing_config)

            if field_type == "integer":
                target_type = "int"
            elif field_type == "double":
                target_type = "double"
            else:  # float
                target_type = "float"
            df = CoreTransformations.convert_data_types(df, field_name, target_type)

        elif field_type in ["date", "timestamp"]:
            date_overrides = field_config.get(
                "date_overrides", type_defaults.get("date_overrides")
            )
            df = DateTransformations.clean_date_strings(df, field_name, date_overrides)

            # Handle format conversion
            input_format = field_config.get("input_format")
            target_format = field_config.get("target_format")

            if field_type == "date":
                df = DateTransformations.convert_date_format(
                    df, field_name, input_format, target_format
                )
            else:  # timestamp
                df = DateTransformations.convert_timestamp_format(
                    df, field_name, input_format, target_format
                )

            # Final type conversion if no custom formatting
            if not target_format:
                target_type = "date" if field_type == "date" else "datetime"
                df = CoreTransformations.convert_data_types(df, field_name, target_type)

        elif field_type == "fee_rate":
            invalid_values = field_config.get(
                "invalid_values", type_defaults.get("invalid_values")
            )
            df = DataQuality.clean_invalid_values(df, field_name, invalid_values)
            df = CoreTransformations.convert_data_types(df, field_name, "float")

        elif field_type == "value_normalization":
            missing_default = field_config.get(
                "missing_value_default",
                type_defaults.get("missing_value_default", "Unknown"),
            )
            missing_config = {"default_value": missing_default, "data_type": "string"}
            df = DataQuality.handle_missing_values(df, field_name, missing_config)
            df = CoreTransformations.string_cleaning(df, [field_name])

            # Get the specific normalization key (e.g., 'business_line', 'product')
            normalization_key = field_config.get("normalization_key")
            if normalization_key and normalization_key in type_defaults:
                normalization_map = type_defaults[normalization_key]
                df = BusinessTransformations.normalize_values(
                    df, field_name, normalization_map
                )

        elif field_type == "struct":
            # Handle struct type - apply missing value defaults
            missing_default = field_config.get(
                "missing_value_default", type_defaults.get("missing_value_default")
            )
            if missing_default is not None:
                missing_config = {
                    "default_value": missing_default,
                    "data_type": "struct",
                }
                df = DataQuality.handle_missing_values(df, field_name, missing_config)

        # elif field_type == "array_of_struct":
        #     # Handle array of struct type
        #     missing_default = field_config.get(
        #         "missing_value_default", type_defaults.get("missing_value_default", [])  # pylint: disable=line-too-long
        #     )

        #     # If column doesn't exist, create with null value
        #     if field_name not in df.columns:
        #         df = df.withColumn(field_name, lit(None))

        #     # Handle missing values for existing column
        #     missing_config = {
        #         "default_value": missing_default,
        #         "data_type": "array_of_struct",
        #     }
        #     df = DataQuality.handle_missing_values(df, field_name, missing_config)

        elif field_type == "boolean":
            # Handle boolean type conversion
            true_values = field_config.get("true_values")
            false_values = field_config.get("false_values")
            df = CoreTransformations.convert_to_boolean(
                df, field_name, true_values, false_values
            )

            # Only handle missing values if explicitly configured
            # By default, allow null for unmatched values
            missing_default = field_config.get("missing_value_default")
            if missing_default is not None:
                missing_config = {
                    "default_value": missing_default,
                    "data_type": "boolean",
                }
                df = DataQuality.handle_missing_values(df, field_name, missing_config)

        elif field_type == "array_explode":
            # Handle array exploding - this will create multiple rows

            try:
                df = ArrayTransformations.explode_array_column(
                    df, field_name, field_config, defaults
                )

            except Exception as e:
                logger.error(
                    "[TRANSFORM_ENGINE] Array explode failed for %s: %s",
                    field_name, e
                )
                raise e

        elif field_type == "metadata_extractor":
            # Handle metadata extraction from JSON

            try:
                source_column = field_config.get("source_column") or type_defaults.get(
                    "source_column", "_raw_data"
                )
                explode_columns = field_config.get("explode_columns", {})

                # Extract field names from explode_columns
                target_fields = [
                    col_config.get("source_field")
                    for col_config in explode_columns.values()
                    if col_config.get("source_field")
                ]

                df = MetadataExtractor.extract_metadata_fields(
                    df, source_column, target_fields
                )

            except Exception as e:
                logger.error(
                    "[TRANSFORM_ENGINE] Metadata extraction failed for %s: %s",
                    field_name, e
                )
                raise e

        elif field_type == "s3_prefix_extractor":
            # Handle S3 prefix value extraction
            try:
                source_column = field_config.get("source_column", "source_file_name")
                position = field_config.get("position", 1)
                remove_extension = field_config.get("remove_extension", True)

                df = S3Transformations.extract_from_s3_prefix(
                    df, field_name, source_column, position, remove_extension
                )

            except Exception as e:
                logger.error(
                    "[TRANSFORM_ENGINE] S3 prefix extraction failed for %s: %s",
                    field_name, e
                )
                raise e

    # Optional column selection (disabled by default to preserve exploded columns)
    if select_columns:
        # Select only columns defined in config
        config_columns = [
            name
            for name, config in columns.items()
            if config.get("type") not in ["array_explode", "metadata_extractor"]
        ]

        # Add exploded columns from array_explode and metadata_extractor fields
        for field_name, field_config in columns.items():
            if field_config.get("type") in ["array_explode", "metadata_extractor"]:
                explode_columns = field_config.get("explode_columns", {})
                config_columns.extend(explode_columns.keys())

        # Filter to only include columns that exist in the dataframe
        available_columns = df.columns
        final_columns = [col for col in config_columns if col in available_columns]

        if final_columns:
            df = df.select(*final_columns)

    # Apply target schema casting AFTER all transformations
    if target_table_schema:
        df = _align_with_target_schema(df, target_table_schema)

    return df


def _align_with_target_schema(df: DataFrame, target_schema: dict) -> DataFrame:
    """
    Cast DataFrame columns to match target table schema
    """

    for col_name, target_type in target_schema.items():
        if col_name in df.columns:
            current_type = dict(df.dtypes)[col_name]
            target_type_str = str(target_type)

            # Only cast if types don't match
            if current_type != target_type_str:
                try:
                    # For incompatible casts (like string to array), use null
                    if current_type == "string" and "array" in target_type_str.lower():
                        df = df.withColumn(col_name, lit(None).cast(target_type))
                    else:
                        df = df.withColumn(col_name, col(col_name).cast(target_type))

                except Exception as e:
                    logger.error(
                        "Schema alignment cast failed for column %s: %s",
                        col_name, e
                    )
                    raise Exception(
                        f"Failed to cast column '{col_name}' from {current_type} to {target_type_str}: {e}"  # pylint: disable=line-too-long
                    ) from e

    return df
