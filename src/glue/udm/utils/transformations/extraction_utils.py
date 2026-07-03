# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Common extraction utilities for nested data and arrays."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, get_json_object, lit


class ExtractionUtils:
    """Shared utilities for extracting data from struct or JSON string columns."""

    @staticmethod
    def handle_multiple_source_paths(
        df: DataFrame, field_name: str, source_paths: list
    ) -> DataFrame:
        """
        Handle multiple source paths with fallback logic using coalesce.

        Args:
            df: Input DataFrame
            field_name: Target field name
            source_paths: List of source paths to try

        Returns:
            DataFrame with extracted field
        """
        temp_cols = []

        for i, path in enumerate(source_paths):
            temp_col = f"{field_name}_temp_{i}"
            df = ExtractionUtils.extract_from_path(df, temp_col, path)
            temp_cols.append(col(temp_col))

        if temp_cols:
            df = df.withColumn(field_name, coalesce(*temp_cols))
        else:
            df = df.withColumn(field_name, lit(None))

        # Clean up temp columns
        temp_col_names = [f"{field_name}_temp_{i}" for i in range(len(source_paths))]
        existing_temp_cols = [
            col_name for col_name in temp_col_names if col_name in df.columns
        ]
        if existing_temp_cols:
            df = df.drop(*existing_temp_cols)

        return df

    @staticmethod
    def extract_from_path(
        df: DataFrame, target_column: str, source_path: str
    ) -> DataFrame:
        """
        Extract data from source_path using struct access or JSON extraction.

        Args:
            df: Input DataFrame
            target_column: Target column name
            source_path: Dot-separated path to extract from

        Returns:
            DataFrame with extracted column
        """
        try:
            # Check if source_path contains dots (nested path)
            if "." in source_path:
                parts = source_path.split(".")
                parent_col = parts[0]
                nested_path = ".".join(parts[1:])

                # Check if parent column exists
                if parent_col not in df.columns:
                    return df.withColumn(target_column, lit(None))

                # Try struct access first
                try:
                    result_df = df.withColumn(target_column, col(source_path))
                    # Test if this works by checking schema
                    _ = result_df.schema
                    return result_df
                except Exception:
                    # If struct access fails, try JSON extraction
                    json_path = f"$.{nested_path}"
                    return df.withColumn(
                        target_column, get_json_object(col(parent_col), json_path)
                    )
            else:
                # Simple column access
                return df.withColumn(target_column, col(source_path))

        except Exception:
            return df.withColumn(target_column, lit(None))
