# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Deduplication module for removing duplicate records."""

from pyspark.sql import DataFrame


class DedupeHandler:
    """Handler for deduplicating DataFrames."""

    @staticmethod
    def dedupe_dataframe(df: DataFrame) -> DataFrame:
        """
        Remove duplicate records by grouping on all columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with duplicates removed
        """
        if df is None:
            return df

        # Remove duplicates by selecting distinct rows across all columns
        return df.distinct()
