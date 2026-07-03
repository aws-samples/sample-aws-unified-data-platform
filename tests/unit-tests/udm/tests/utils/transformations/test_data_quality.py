# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DataQuality class."""

from unittest.mock import Mock

import pytest
from utils.transformations.data_quality import DataQuality


class TestDataQuality:
    """Test cases for DataQuality transformations."""

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        return mock_df

    def test_clean_invalid_values_with_default_mapping(self, mock_df):
        """Test cleaning invalid values with default mapping."""
        result = DataQuality.clean_invalid_values(mock_df, "column")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_clean_invalid_values_with_custom_mapping(self, mock_df):
        """Test cleaning invalid values with custom mapping."""
        custom_mapping = {"bad_value": None, "another_bad": "good"}
        result = DataQuality.clean_invalid_values(mock_df, "column", custom_mapping)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_clean_invalid_values_with_empty_mapping(self, mock_df):
        """Test cleaning invalid values with empty mapping."""
        empty_mapping = {}
        result = DataQuality.clean_invalid_values(mock_df, "column", empty_mapping)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_clean_invalid_values_with_single_mapping(self, mock_df):
        """Test cleaning invalid values with single mapping."""
        single_mapping = {"VAR/TBD": None}
        result = DataQuality.clean_invalid_values(mock_df, "column", single_mapping)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_clean_invalid_values_with_multiple_mappings(self, mock_df):
        """Test cleaning invalid values with multiple mappings."""
        multiple_mapping = {"VAR/TBD": None, "N/A": None, "-": "Unknown", "": "Empty"}
        result = DataQuality.clean_invalid_values(mock_df, "column", multiple_mapping)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_with_default_config(self, mock_df):
        """Test handling missing values with default config."""
        result = DataQuality.handle_missing_values(mock_df, "column")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_string_type(self, mock_df):
        """Test handling missing values for string type."""
        config = {"default_value": "Unknown", "data_type": "string"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_string_type_none_default(self, mock_df):
        """Test handling missing values for string type with None default."""
        config = {"default_value": None, "data_type": "string"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_float_type(self, mock_df):
        """Test handling missing values for float type."""
        config = {"default_value": 0.5, "data_type": "float"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_float_type_none_default(self, mock_df):
        """Test handling missing values for float type with None default."""
        config = {"default_value": None, "data_type": "float"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_int_type(self, mock_df):
        """Test handling missing values for int type."""
        config = {"default_value": 42, "data_type": "int"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_int_type_none_default(self, mock_df):
        """Test handling missing values for int type with None default."""
        config = {"default_value": None, "data_type": "int"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_boolean_type(self, mock_df):
        """Test handling missing values for boolean type."""
        config = {"default_value": True, "data_type": "boolean"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_boolean_type_none_default(self, mock_df):
        """Test handling missing values for boolean type with None default."""
        config = {"default_value": None, "data_type": "boolean"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type(self, mock_df):
        """Test handling missing values for struct type."""
        config = {"default_value": {"key": "value"}, "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_none_default(self, mock_df):
        """Test handling missing values for struct type with None default."""
        config = {"default_value": None, "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_empty_dict(self, mock_df):
        """Test handling missing values for struct type with empty dict."""
        config = {"default_value": {}, "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_empty_list(self, mock_df):
        """Test handling missing values for struct type with empty list."""
        config = {"default_value": [], "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_null_string(self, mock_df):
        """Test handling missing values for struct type with null string."""
        config = {"default_value": "null", "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_null_uppercase(self, mock_df):
        """Test handling missing values for struct type with NULL string."""
        config = {"default_value": "NULL", "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_struct_type_null_capitalized(self, mock_df):
        """Test handling missing values for struct type with Null string."""
        config = {"default_value": "Null", "data_type": "struct"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_array_explode_type(self, mock_df):
        """Test handling missing values for array_explode type."""
        config = {"default_value": {"key": "value"}, "data_type": "array_explode"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_metadata_extractor_type(self, mock_df):
        """Test handling missing values for metadata_extractor type."""
        config = {"default_value": {"key": "value"}, "data_type": "metadata_extractor"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_unknown_type(self, mock_df):
        """Test handling missing values for unknown type."""
        config = {"default_value": "custom", "data_type": "unknown_type"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_handle_missing_values_no_data_type(self, mock_df):
        """Test handling missing values with no data_type specified."""
        config = {"default_value": "custom"}
        result = DataQuality.handle_missing_values(mock_df, "column", config)

        mock_df.withColumn.assert_called_once()
        assert result == mock_df
