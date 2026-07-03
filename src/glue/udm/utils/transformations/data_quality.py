# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


class DataQuality:
    """Data quality transformations - missing values, invalid values, validation"""

    @staticmethod
    def clean_invalid_values(
        df: DataFrame, column: str, invalid_values_mapping: dict = None
    ) -> DataFrame:
        """Clean invalid values and replace with specified values"""
        if invalid_values_mapping is None:
            invalid_values_mapping = {
                "VAR/TBD": None,
                "VAR/TBD?": None,
                "-": None,
                "N/A": None,
                "": None,
                "data not available": None,
            }

        result_col = col(column)
        for invalid_val, replacement in invalid_values_mapping.items():
            result_col = when(col(column) == invalid_val, lit(replacement)).otherwise(
                result_col
            )

        return df.withColumn(column, result_col)

    @staticmethod
    def handle_missing_values(
        df: DataFrame, column: str, missing_value_config: dict = None
    ) -> DataFrame:
        """Handle missing values with defaults from config"""
        if missing_value_config is None:
            missing_value_config = {
                "default_value": "Not specified",
                "data_type": "string",
            }

        default_value = missing_value_config.get("default_value")
        data_type = missing_value_config.get("data_type", "string")

        if data_type == "string":
            default = default_value if default_value is not None else ""
        elif data_type == "float":
            default = default_value if default_value is not None else 0.0
        elif data_type == "int":
            default = default_value if default_value is not None else 0
        elif data_type == "boolean":
            default = default_value if default_value is not None else False
        # elif data_type in ["struct", "array_of_struct", "array_explode"]:
        elif data_type in ["struct", "array_explode", "metadata_extractor"]:
            # For complex types, translate empty values to null to avoid conflicts
            if default_value in [None, {}, [], "null", "NULL", "Null"]:
                default = None
            else:
                default = default_value
        else:
            default = "Not specified"

        return df.withColumn(
            column, when(col(column).isNull(), lit(default)).otherwise(col(column))
        )
