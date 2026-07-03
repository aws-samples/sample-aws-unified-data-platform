# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    explode,
    from_json,
    get_json_object,
    lit,
    schema_of_json,
    size,
    when,
)
from pyspark.sql.types import DoubleType, FloatType, IntegerType

logger = logging.getLogger(__name__)


class ArrayTransformations:
    @staticmethod
    def explode_array_column(
        df: DataFrame, field_name: str, field_config: dict, defaults: dict = None
    ) -> DataFrame:
        source_path = field_config.get("source_path")
        explode_columns = field_config.get("explode_columns", {})

        if not source_path:
            return df

        # Handle simple array explosion (no explode_columns)
        if not explode_columns:
            return ArrayTransformations._explode_simple_array(
                df, field_name, field_config, defaults
            )

        # Extract array from nested path
        array_col = f"{field_name}_array"
        df = ArrayTransformations._extract_array(df, array_col, source_path)

        # If no array data, return with default columns
        if array_col not in df.columns:
            return ArrayTransformations._add_default_columns(
                df, explode_columns, defaults, array_col
            )

        # Check column type and filter accordingly
        col_type = dict(df.dtypes).get(array_col, "unknown")

        if col_type.startswith("array"):
            # Array type - use size function
            valid_arrays = df.filter(
                col(array_col).isNotNull() & (size(col(array_col)) > 0)
            )
            df_no_arrays = df.filter(
                col(array_col).isNull() | (size(col(array_col)) == 0)
            )
        else:
            # String or other type - treat as no valid arrays
            valid_arrays = df.filter(lit(False))  # Empty dataset
            df_no_arrays = df

        # Check if we have valid arrays to explode (avoid expensive count())
        if valid_arrays.take(1) == []:
            return ArrayTransformations._add_default_columns(
                df, explode_columns, defaults, array_col
            )

        # Explode array
        df_exploded = valid_arrays.withColumn("exploded_item", explode(col(array_col)))
        column_expressions = []

        # Process each column once
        for col_name, col_config in explode_columns.items():
            col_type = col_config.get("type", "string")
            column_default = ArrayTransformations._get_column_default(
                col_config, col_type, defaults
            )

            # Handle exploded rows
            try:
                col_expr = ArrayTransformations._build_column_expression(
                    df_exploded, col_config
                )

                if col_type in ["float", "int", "double"]:
                    col_expr = when(
                        col_expr.isNotNull(),
                        ArrayTransformations._cast_with_type(col_expr, col_type),
                    )
                    col_expr = when(
                        col_expr.isNull(),
                        ArrayTransformations._cast_with_type(
                            lit(column_default), col_type
                        ),
                    ).otherwise(col_expr)
                else:
                    col_expr = when(col_expr.isNull(), lit(column_default)).otherwise(
                        col_expr
                    )

                df_exploded.select(col_expr.alias("test")).limit(1).collect()
                column_expressions.append(col_expr.alias(col_name))
            except Exception as e:
                logger.warning("[ARRAY_EXPLODE] Expression failed for %s: %s", col_name, e)
                column_expressions.append(
                    ArrayTransformations._cast_with_type(
                        lit(column_default), col_type
                    ).alias(col_name)
                )

            # Handle non-array rows
            df_no_arrays = df_no_arrays.withColumn(
                col_name,
                ArrayTransformations._cast_with_type(lit(column_default), col_type),
            )

        # Select final columns - use expressions for exploded DataFrame
        original_cols = [col(c) for c in df.columns if c != array_col]
        df_exploded_final = df_exploded.select(*original_cols, *column_expressions)

        # Reuse original_cols for consistency
        exploded_col_names = list(explode_columns.keys())
        df_no_arrays_final = df_no_arrays.select(
            *[c for c in df.columns if c != array_col] + exploded_col_names
        )

        return df_exploded_final.union(df_no_arrays_final)

    @staticmethod
    def _explode_simple_array(
        df: DataFrame, field_name: str, field_config: dict, defaults: dict = None
    ) -> DataFrame:
        """Handle simple array types like array<string>, array<int>, etc."""
        source_path = field_config.get("source_path")

        # Extract array from source path
        array_col = f"{field_name}_array"
        df = ArrayTransformations._extract_array(df, array_col, source_path)

        if array_col not in df.columns:
            # No array found, add column with default value
            field_type = field_config.get("type", "string")
            default_value = ArrayTransformations._get_column_default(
                field_config, field_type, defaults
            )
            return df.withColumn(
                field_name,
                ArrayTransformations._cast_with_type(lit(default_value), field_type),
            )

        # Check if we have valid arrays
        col_type = dict(df.dtypes).get(array_col, "unknown")

        if col_type.startswith("array"):
            valid_arrays = df.filter(
                col(array_col).isNotNull() & (size(col(array_col)) > 0)
            )
            df_no_arrays = df.filter(
                col(array_col).isNull() | (size(col(array_col)) == 0)
            )
        else:
            valid_arrays = df.filter(lit(False))
            df_no_arrays = df

        # Handle rows with no arrays
        field_type = field_config.get("type", "string")
        default_value = ArrayTransformations._get_column_default(
            field_config, field_type, defaults
        )
        df_no_arrays = df_no_arrays.withColumn(
            field_name,
            ArrayTransformations._cast_with_type(lit(default_value), field_type),
        ).drop(array_col)

        # Handle rows with arrays - explode them
        if valid_arrays.take(1):
            df_exploded = valid_arrays.withColumn(
                field_name, explode(col(array_col))
            ).drop(array_col)

            # Cast to proper type if needed
            if field_type != "string":
                df_exploded = df_exploded.withColumn(
                    field_name,
                    ArrayTransformations._cast_with_type(col(field_name), field_type),
                )

            return df_exploded.union(df_no_arrays)
        else:
            return df_no_arrays

    @staticmethod
    def _add_default_columns(
        df: DataFrame, explode_columns: dict, defaults: dict, array_col: str
    ) -> DataFrame:
        """Add default columns with proper types when no arrays to explode."""
        for col_name, col_config in explode_columns.items():
            col_type = col_config.get("type", "string")
            column_default = ArrayTransformations._get_column_default(
                col_config, col_type, defaults
            )
            df = df.withColumn(
                col_name,
                ArrayTransformations._cast_with_type(lit(column_default), col_type),
            )
        return df.drop(array_col)

    @staticmethod
    def _get_column_default(col_config: dict, col_type: str, defaults: dict = None):
        """Get default value for column type."""
        column_default = col_config.get("missing_value_default")
        if column_default is None and defaults:
            type_defaults = defaults.get(col_type, {})
            column_default = type_defaults.get("missing_value_default")
        if column_default is None:
            if col_type in ["float", "double"]:
                return 0.0
            elif col_type == "int":
                return 0
            else:
                return ""
        return column_default

    @staticmethod
    def _cast_with_type(value_expr, col_type: str):
        """Cast expression with proper Spark type."""
        if col_type == "float":
            return value_expr.cast(FloatType())
        elif col_type == "int":
            return value_expr.cast(IntegerType())
        elif col_type == "double":
            return value_expr.cast(DoubleType())
        else:
            return value_expr

    @staticmethod
    def _build_column_expression(df_exploded: DataFrame, col_config: dict):
        """Build column expression with fallback logic for missing fields."""
        source_field = col_config.get("source_field")
        source_path = col_config.get("source_path")

        if source_path:
            paths = source_path if isinstance(source_path, list) else [source_path]
            working_exprs = []

            for path in paths:
                parts = path.split(".")
                try:
                    # Test if this path works by trying it on a sample
                    expr = col("exploded_item")
                    for part in parts:
                        expr = expr.getItem(part)

                    # Try to execute the expression to see if it works
                    df_exploded.select(expr.alias("test")).limit(1).collect()
                    working_exprs.append(expr)
                except Exception as e:
                    # This path doesn't work, skip it
                    logger.debug("Path %s not valid for array item, skipping: %s", path, e)
                    continue

            # Use coalesce with only working expressions
            return coalesce(*working_exprs) if working_exprs else lit(None)

        elif source_field:
            try:
                # Test if this field exists
                expr = col("exploded_item").getItem(source_field)
                df_exploded.select(expr.alias("test")).limit(1).collect()
                return expr
            except Exception:
                # Field doesn't exist, return None (will be handled by default logic)
                return lit(None)

        return lit(None)

    @staticmethod
    def _extract_array(df: DataFrame, target_col: str, source_path: str) -> DataFrame:
        try:
            parts = source_path.split(".")
            base_col = parts[0]

            if base_col not in df.columns:
                return df.withColumn(target_col, lit(None))

            # Handle JSON string extraction
            if dict(df.dtypes)[base_col] == "string":
                json_path = "$." + ".".join(parts[1:])
                df = df.withColumn(
                    target_col, get_json_object(col(base_col), json_path)
                )

                # Convert JSON string to array
                sample = (
                    df.select(target_col)
                    .filter(
                        col(target_col).isNotNull()
                        & (col(target_col) != "")
                        & col(target_col).startswith("[")
                    )
                    .take(5)
                )

                if sample:
                    # Find a non-empty array for schema inference
                    valid_json = None
                    for row in sample:
                        json_str = row[0].strip()
                        if json_str != "[]":
                            valid_json = json_str
                            break

                    if valid_json:
                        schema = schema_of_json(lit(valid_json))
                        df = df.withColumn(
                            target_col,
                            when(
                                col(target_col).isNotNull()
                                & (col(target_col) != "")
                                & col(target_col).startswith("["),
                                from_json(col(target_col), schema),
                            ).otherwise(lit(None)),
                        )

            else:
                # Handle struct extraction or direct column access
                if len(parts) == 1:
                    # Direct column access for simple arrays
                    df = df.withColumn(target_col, col(base_col))
                else:
                    # Nested struct access
                    expr = col(base_col)
                    for part in parts[1:]:
                        expr = expr.getItem(part)
                    df = df.withColumn(target_col, expr)

        except Exception as e:
            logger.warning("Failed to extract array from %s: %s", source_path, e)
            df = df.withColumn(target_col, lit(None))

        return df
