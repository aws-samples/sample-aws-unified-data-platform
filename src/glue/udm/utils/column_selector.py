# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Column selector module for handling target column selection."""

import logging

from pyspark.sql import DataFrame

from .job_utils import convert_namespace_to_dict

logger = logging.getLogger(__name__)


class ColumnSelector:
    """Class for selecting target columns including exploded array columns."""

    @staticmethod
    def select_target_columns(
        df: DataFrame, target_columns: list, config: dict
    ) -> DataFrame:
        """
        Select target columns including exploded array columns.

        Args:
            df: Input DataFrame
            target_columns: List of target column names from config
            config: Configuration dict containing column definitions

        Returns:
            DataFrame with selected columns
        """

        # Convert config to dict if needed
        config = convert_namespace_to_dict(config)
        columns_config = config.get("columns", {})

        # Add exploded columns from array fields
        exploded_columns = []
        base_columns = []

        for col_name in target_columns:
            col_config = columns_config.get(col_name, {})
            if col_config.get("type") in ["array_explode", "metadata_extractor"]:
                explode_columns = col_config.get("explode_columns", {})
                if explode_columns:
                    # Complex array/metadata with explode_columns
                    exploded_columns.extend(explode_columns.keys())

                else:
                    # Simple array - the column itself will be exploded
                    base_columns.append(col_name)

            else:
                base_columns.append(col_name)

        # Combine base columns with exploded columns
        all_target_columns = base_columns + exploded_columns

        # Filter to only include columns that exist in the dataframe
        available_columns = df.columns
        selected_columns = [
            col for col in all_target_columns if col in available_columns
        ]

        if selected_columns:
            try:
                result_df = df.select(*selected_columns)

                return result_df
            except Exception as e:
                logger.error("[COLUMN_SELECTOR] Failed to select columns: %s", e)
                logger.error(
                    "[COLUMN_SELECTOR] Attempted to select: %s", selected_columns
                )
                raise e
        else:
            return df
