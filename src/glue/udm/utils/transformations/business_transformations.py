# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


class BusinessTransformations:
    """Business and product normalization transformations"""

    @staticmethod
    def normalize_values(
        df: DataFrame, column: str, normalization_mapping: dict
    ) -> DataFrame:
        """Generic method to normalize values using mapping dictionary."""
        result_col = col(column)
        for old_val, new_val in normalization_mapping.items():
            result_col = when(col(column) == old_val, lit(new_val)).otherwise(
                result_col
            )
        return df.withColumn(column, result_col)
