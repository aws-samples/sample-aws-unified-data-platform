# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""S3-specific transformations for extracting values from S3 paths and prefixes."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace, size, split


class S3Transformations:
    """Transformations for extracting data from S3 paths and prefixes."""

    @staticmethod
    def extract_from_s3_prefix(
        df: DataFrame,
        target_column: str,
        source_column: str,
        position: int,
        remove_extension: bool = True,
    ) -> DataFrame:
        """
        Extract value from S3 prefix path by position.

        Args:
            df: Input DataFrame
            target_column: Name of column to create with extracted value
            source_column: Source column containing S3 path
            position: Position in path to extract (1-based index)
            remove_extension: Whether to remove file extension from extracted value (default: True)  # pylint: disable=line-too-long

        Returns:
            DataFrame with new column containing extracted value

        Example:
            S3 path: s3://bucket/folder1/folder2/8003247/file.json
            After removing s3://: bucket/folder1/folder2/8003247/file.json
            position: 4 -> extracts "8003247"
            position: -1 -> extracts "file" (from "file.json", extension removed by default)  x
        """
        if source_column not in df.columns:
            # If source column doesn't exist, create target with null
            return df.withColumn(target_column, lit(None))

        # Remove s3:// prefix and split by '/'
        cleaned_path = regexp_replace(col(source_column), "^s3://", "")
        path_parts = split(cleaned_path, "/")

        # Extract the value at the specified position
        # Adjust for 0-based indexing: position 1 = index 0
        # Handle negative positions (e.g., -1 for last element)
        if position < 0:
            extracted_value = path_parts.getItem(size(path_parts) + position)
        else:
            extracted_value = path_parts.getItem(position - 1)

        # Remove file extension if requested
        if remove_extension:
            filename_parts = split(extracted_value, r"\.")
            extracted_value = filename_parts.getItem(0)

        return df.withColumn(target_column, extracted_value)
