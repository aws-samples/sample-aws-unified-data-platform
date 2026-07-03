# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
)


class CoreTransformations:
    """Basic string, numeric, and type conversion transformations"""

    @staticmethod
    def string_cleaning(df: DataFrame, columns: list) -> DataFrame:
        """Clean string columns - strip whitespace, handle nulls"""
        for col_name in columns:
            df = df.withColumn(
                col_name, trim(regexp_replace(col(col_name), r"\s+", " "))
            )
        return df

    @staticmethod
    def convert_data_types(df: DataFrame, column: str, target_type: str) -> DataFrame:
        """Convert column data types"""
        if target_type == "string":
            return df.withColumn(column, col(column).cast(StringType()))
        elif target_type == "float":
            return df.withColumn(column, col(column).cast(FloatType()))
        elif target_type == "double":
            return df.withColumn(column, col(column).cast(DoubleType()))
        elif target_type == "int":
            return df.withColumn(column, col(column).cast(IntegerType()))
        elif target_type == "date":
            return df.withColumn(column, to_date(col(column)))
        elif target_type == "datetime":
            return df.withColumn(column, to_timestamp(col(column)))
        return df

    @staticmethod
    def apply_corrections_mapping(
        df: DataFrame, column: str, corrections_dict: dict
    ) -> DataFrame:
        """Apply corrections mapping to column values"""
        correction_expr = col(column)
        for old_val, new_val in corrections_dict.items():
            correction_expr = when(col(column) == old_val, lit(new_val)).otherwise(
                correction_expr
            )
        return df.withColumn(column, correction_expr)

    @staticmethod
    def convert_to_boolean(
        df: DataFrame,
        column: str,
        true_values: list = None,
        false_values: list = None,
    ) -> DataFrame:
        """Convert column to boolean based on true/false value mappings"""
        # Default values if not provided
        if true_values is None:
            true_values = ["yes", "y", "true", "1", "1.0"]
        if false_values is None:
            false_values = ["no", "n", "false", "0", "0.0", "N/A"]

        # Normalize values to lowercase strings for comparison
        true_values_lower = [str(v).lower() for v in true_values]
        false_values_lower = [str(v).lower() for v in false_values]

        # Also create numeric versions for direct comparison
        true_values_numeric = []
        false_values_numeric = []
        for v in true_values:
            try:
                true_values_numeric.append(float(v))
            except (ValueError, TypeError):
                pass
        for v in false_values:
            try:
                false_values_numeric.append(float(v))
            except (ValueError, TypeError):
                pass

        # Build boolean conversion expression with both string and numeric checks
        boolean_expr = when(
            col(column).isNotNull(),
            when(
                lower(col(column).cast(StringType())).isin(true_values_lower)
                | col(column).cast(FloatType()).isin(true_values_numeric),
                lit(True),
            )
            .when(
                lower(col(column).cast(StringType())).isin(false_values_lower)
                | col(column).cast(FloatType()).isin(false_values_numeric),
                lit(False),
            )
            .otherwise(lit(None)),
        ).otherwise(lit(None))

        return df.withColumn(column, boolean_expr)

    @staticmethod
    def rename_column(
        df: DataFrame, old_column_name: str, new_column_name: str
    ) -> DataFrame:
        """
        Rename a column in the DataFrame.

        Args:
            df: Input DataFrame
            old_column_name: Current column name
            new_column_name: New column name

        Returns:
            DataFrame with renamed column
        """
        if old_column_name in df.columns:
            return df.withColumnRenamed(old_column_name, new_column_name)
        return df
