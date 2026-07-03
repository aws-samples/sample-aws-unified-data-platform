# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for metadata_extractor module."""

import json
from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.transformations.metadata_extractor import MetadataExtractor


class TestMetadataExtractor:
    """Test cases for MetadataExtractor class."""

    def test_extract_metadata_fields_basic(self):
        """Test basic metadata field extraction."""
        mock_df = Mock()
        mock_df1 = Mock()
        mock_df2 = Mock()

        # Chain withColumn calls
        mock_df.withColumn.return_value = mock_df1
        mock_df1.withColumn.return_value = mock_df2

        with patch(
            "src.glue.udm.utils.transformations.metadata_extractor.udf"
        ) as mock_udf:
            with patch(
                "src.glue.udm.utils.transformations.metadata_extractor.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.metadata_extractor.lit"
                ) as mock_lit:
                    mock_udf_func = Mock()
                    mock_udf.return_value = mock_udf_func
                    mock_col.return_value = "col_expr"
                    mock_lit.return_value = "lit_expr"
                    mock_udf_func.return_value = "udf_result"

                    result = MetadataExtractor.extract_metadata_fields(
                        mock_df, "json_col", ["field1", "field2"]
                    )

                    assert result == mock_df2
                    assert mock_df.withColumn.call_count == 1
                    assert mock_df1.withColumn.call_count == 1

    def test_extract_metadata_fields_empty_target_fields(self):
        """Test metadata extraction with empty target fields."""
        mock_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.metadata_extractor.udf"
        ) as mock_udf:
            mock_udf_func = Mock()
            mock_udf.return_value = mock_udf_func

            result = MetadataExtractor.extract_metadata_fields(mock_df, "json_col", [])

            assert result == mock_df
            mock_udf.assert_called_once()

    def test_extract_metadata_fields_single_field(self):
        """Test metadata extraction with single target field."""
        mock_df = Mock()
        mock_result_df = Mock()
        mock_df.withColumn.return_value = mock_result_df

        with patch(
            "src.glue.udm.utils.transformations.metadata_extractor.udf"
        ) as mock_udf:
            with patch(
                "src.glue.udm.utils.transformations.metadata_extractor.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.metadata_extractor.lit"
                ) as mock_lit:
                    mock_udf_func = Mock()
                    mock_udf.return_value = mock_udf_func
                    mock_col.return_value = "col_expr"
                    mock_lit.return_value = "lit_expr"
                    mock_udf_func.return_value = "udf_result"

                    result = MetadataExtractor.extract_metadata_fields(
                        mock_df, "json_col", ["single_field"]
                    )

                    assert result == mock_result_df
                    mock_udf.assert_called_once()

    def test_extract_field_by_name_empty_json(self):
        """Test _extract_field_by_name with empty JSON."""
        result = MetadataExtractor._extract_field_by_name("", "field_name")
        assert result is None

    def test_extract_field_by_name_none_json(self):
        """Test _extract_field_by_name with None JSON."""
        result = MetadataExtractor._extract_field_by_name(None, "field_name")
        assert result is None

    def test_extract_field_by_name_no_metadata_key(self):
        """Test _extract_field_by_name when metadata key not found."""
        json_data = {"metadata": {"categories": []}, "data": {"categories": []}}
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "nonexistent_field")
        assert result is None

    def test_extract_field_by_name_no_categories(self):
        """Test _extract_field_by_name when no data categories."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result is None

    def test_extract_field_by_name_string_value(self):
        """Test _extract_field_by_name with string value."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": "test_value"}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == "test_value"

    def test_extract_field_by_name_list_value_single(self):
        """Test _extract_field_by_name with single item list."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": ["single_value"]}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == "single_value"

    def test_extract_field_by_name_list_value_multiple(self):
        """Test _extract_field_by_name with multiple item list."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": ["value1", "value2"]}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == '["value1", "value2"]'

    def test_extract_field_by_name_list_with_nulls(self):
        """Test _extract_field_by_name with list containing nulls."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": [None, "value", None]}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == "value"

    def test_extract_field_by_name_dict_value(self):
        """Test _extract_field_by_name with dict value."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": {"nested": "value"}}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == '{"nested": "value"}'

    def test_extract_field_by_name_bool_value(self):
        """Test _extract_field_by_name with boolean value."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": True}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result == "true"

    def test_extract_field_by_name_null_value(self):
        """Test _extract_field_by_name with null value."""
        json_data = {
            "metadata": {"categories": [{"key1": {"name": "field_name"}}]},
            "data": {"categories": [{"key1": None}]},
        }
        json_str = json.dumps(json_data)

        result = MetadataExtractor._extract_field_by_name(json_str, "field_name")
        assert result is None

    def test_extract_field_by_name_invalid_json(self):
        """Test _extract_field_by_name with invalid JSON."""
        result = MetadataExtractor._extract_field_by_name("invalid json", "field_name")
        assert result is None

    def test_find_metadata_key_found(self):
        """Test _find_metadata_key when key is found."""
        data = {
            "metadata": {
                "categories": [
                    {"key1": {"name": "field_name"}},
                    {"key2": {"name": "other_field"}},
                ]
            }
        }

        result = MetadataExtractor._find_metadata_key(data, "field_name")
        assert result == "key1"

    def test_find_metadata_key_not_found(self):
        """Test _find_metadata_key when key is not found."""
        data = {"metadata": {"categories": [{"key1": {"name": "other_field"}}]}}

        result = MetadataExtractor._find_metadata_key(data, "field_name")
        assert result is None

    def test_find_metadata_key_no_metadata(self):
        """Test _find_metadata_key when no metadata."""
        data = {}

        result = MetadataExtractor._find_metadata_key(data, "field_name")
        assert result is None

    def test_find_metadata_key_invalid_structure(self):
        """Test _find_metadata_key with invalid metadata structure."""
        data = {"metadata": {"categories": [{"key1": "not_a_dict"}]}}

        result = MetadataExtractor._find_metadata_key(data, "field_name")
        assert result is None

    def test_find_metadata_key_exception(self):
        """Test _find_metadata_key with exception."""
        data = {"metadata": {"categories": "not_a_list"}}

        result = MetadataExtractor._find_metadata_key(data, "field_name")
        assert result is None
