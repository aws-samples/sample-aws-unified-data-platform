# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    get_json_object,
    lit,
    schema_of_json,
    when,
)

from .extraction_utils import ExtractionUtils

logger = logging.getLogger(__name__)


class NestedDataTransformations:
    """Transformations for nested data structures and arrays"""

    @staticmethod
    def flatten_column(
        df: DataFrame, target_column: str, source_path: str, target_type: str = "string"
    ) -> DataFrame:
        """Flatten nested column using struct access or clean JSON extraction"""
        try:
            # Check if source_path contains dots (nested path)
            if "." in source_path:
                parts = source_path.split(".")
                parent_col = parts[0]
                nested_path = ".".join(parts[1:])

                # Check if parent column exists
                if parent_col not in df.columns:
                    return df.withColumn(target_column, lit(None))

                if target_type == "struct":
                    # For struct type, extract and convert directly
                    json_path = f"$.{nested_path}"
                    return NestedDataTransformations._extract_and_convert_to_struct(
                        df, target_column, parent_col, json_path
                    )
                else:
                    # For string type, use common extraction logic
                    return ExtractionUtils.extract_from_path(
                        df, target_column, source_path
                    )
            else:
                # Simple column access
                return df.withColumn(target_column, col(source_path))

        except Exception:
            return df.withColumn(target_column, lit(None))

    @staticmethod
    def _extract_and_convert_to_struct(
        df: DataFrame, target_column: str, parent_col: str, json_path: str
    ) -> DataFrame:
        """Extract JSON from parent column and convert directly to struct"""
        try:
            # First extract the JSON string
            temp_col = f"{target_column}_temp_json"
            df_with_json = df.withColumn(
                temp_col, get_json_object(col(parent_col), json_path)
            )

            # Sample the extracted JSON to infer schema
            sample_data = (
                df_with_json.select(temp_col)
                .filter(col(temp_col).isNotNull() & (col(temp_col) != ""))
                .limit(5)
                .collect()
            )

            if not sample_data:
                return df.withColumn(target_column, lit(None)).drop(temp_col)

            # Try to infer schema from first non-null value
            for row in sample_data:
                json_str = row[0]
                if json_str and isinstance(json_str, str):
                    try:
                        # Let Spark determine if it's valid JSON
                        json_schema = schema_of_json(lit(json_str.strip()))

                        # Convert to struct using inferred schema
                        result_df = df_with_json.withColumn(
                            target_column,
                            when(
                                col(temp_col).isNotNull() & (col(temp_col) != ""),
                                from_json(col(temp_col), json_schema),
                            ).otherwise(lit(None)),
                        ).drop(temp_col)

                        logger.info(
                            "Successfully converted %s to struct type",
                            target_column,
                        )
                        return result_df

                    except Exception as e:
                        logger.info("Failed to convert %s to struct: %s", target_column, e)
                        continue

            # Fallback: return as string if no valid JSON found
            return df_with_json.withColumnRenamed(temp_col, target_column)

        except Exception as e:
            logger.info("Error in _extract_and_convert_to_struct: %s", e)
            return df.withColumn(target_column, lit(None))
