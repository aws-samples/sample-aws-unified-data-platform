# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Metadata extraction module for extracting specific fields from complex JSON structures."""  # pylint: disable=line-too-long

import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extract specific fields from metadata using dynamic key-value mappings."""

    @staticmethod
    def extract_metadata_fields(
        df: DataFrame, json_column: str, target_fields: list
    ) -> DataFrame:
        """
        Extract metadata fields from JSON column by reading field mappings from metadata.

        Args:
            df: Input DataFrame
            json_column: Name of column containing the JSON data
            target_fields: List of field names to extract

        Returns:
            DataFrame with extracted metadata columns
        """
        logger.info(
            "[METADATA_EXTRACTOR] Starting metadata extraction from column: %s",  # pylint: disable=line-too-long
            json_column,
        )
        logger.info("[METADATA_EXTRACTOR] Target fields: %s", target_fields)

        # Create UDF for metadata extraction
        extract_udf = udf(MetadataExtractor._extract_field_by_name, StringType())

        # Extract each target field
        for field_name in target_fields:
            column_name = (
                field_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            )
            logger.info(
                "[METADATA_EXTRACTOR] Extracting %s -> %s",
                field_name,
                column_name,
            )
            df = df.withColumn(
                column_name, extract_udf(col(json_column), lit(field_name))
            )

        logger.info(
            "[METADATA_EXTRACTOR] Completed extraction of %s fields",
            len(target_fields),
        )
        return df

    @staticmethod
    def _extract_field_by_name(json_str: str, field_name: str) -> str:
        """
        Extract field value by finding the key from metadata and getting value from data.categories.

        Args:
            json_str: JSON string containing the full structure
            field_name: Field name to search for (e.g., "Document Number")

        Returns:
            Extracted value as string, or None if not found
        """
        try:
            if not json_str:
                return None

            data = json.loads(json_str)

            # Find the metadata key for this field name
            metadata_key = MetadataExtractor._find_metadata_key(data, field_name)
            if not metadata_key:
                return None

            # Navigate to data.categories array
            categories = data.get("data", {}).get("categories", [])
            if not categories:
                return None

            # Search through all category objects for the metadata key
            for category in categories:
                if metadata_key in category and category[metadata_key] is not None:
                    value = category[metadata_key]

                    # Handle different value types
                    if isinstance(value, list):
                        # For arrays, return as JSON string or first non-null value
                        non_null_values = [v for v in value if v is not None]
                        if non_null_values:
                            return (
                                json.dumps(non_null_values)
                                if len(non_null_values) > 1
                                else str(non_null_values[0])
                            )
                        return None
                    elif isinstance(value, (dict, bool)):
                        return json.dumps(value)
                    else:
                        return str(value)

            return None

        except Exception as e:
            logger.warning("[METADATA_EXTRACTOR] Failed to extract %s: %s", field_name, e)
            return None

    @staticmethod
    def _find_metadata_key(data: dict, field_name: str) -> str:
        """
        Find the metadata key for a given field name by searching through metadata.categories.

        Args:
            data: Parsed JSON data structure
            field_name: Field name to search for

        Returns:
            Metadata key if found, None otherwise
        """
        try:
            metadata_categories = data.get("metadata", {}).get("categories", [])

            # Search through all metadata categories
            for category in metadata_categories:
                for key, metadata_info in category.items():
                    if (
                        isinstance(metadata_info, dict)
                        and metadata_info.get("name") == field_name
                    ):
                        return key

            return None

        except Exception as e:
            logger.warning(
                "[METADATA_EXTRACTOR] Failed to find key for %s: %s",
                field_name,
                e,
            )
            return None
