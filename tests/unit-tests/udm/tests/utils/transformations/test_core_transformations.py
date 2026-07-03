# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for CoreTransformations class."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest
from pyspark.sql.functions import col, lit, when
from utils.transformations.core_transformations import CoreTransformations


class TestCoreTransformations:
    """Test cases for CoreTransformations class."""

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock()
        mock_df.columns = ["name", "age", "salary", "active", "messy_text"]
        mock_df.withColumn.return_value = mock_df
        mock_df.withColumnRenamed.return_value = mock_df
        return mock_df

    def test_string_cleaning_single_column(self, mock_df):
        """Test string cleaning on single column."""
        result = CoreTransformations.string_cleaning(mock_df, ["name"])

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_string_cleaning_multiple_columns(self, mock_df):
        """Test string cleaning on multiple columns."""
        result = CoreTransformations.string_cleaning(mock_df, ["name", "messy_text"])

        assert mock_df.withColumn.call_count == 2
        assert result == mock_df

    def test_string_cleaning_empty_list(self, mock_df):
        """Test string cleaning with empty column list."""
        result = CoreTransformations.string_cleaning(mock_df, [])

        mock_df.withColumn.assert_not_called()
        assert result == mock_df

    def test_convert_data_types_to_string(self, mock_df):
        """Test data type conversion to string."""
        result = CoreTransformations.convert_data_types(mock_df, "age", "string")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_to_float(self, mock_df):
        """Test data type conversion to float."""
        result = CoreTransformations.convert_data_types(mock_df, "salary", "float")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_to_double(self, mock_df):
        """Test data type conversion to double."""
        result = CoreTransformations.convert_data_types(mock_df, "salary", "double")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_to_int(self, mock_df):
        """Test data type conversion to integer."""
        result = CoreTransformations.convert_data_types(mock_df, "age", "int")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_to_date(self, mock_df):
        """Test data type conversion to date."""
        result = CoreTransformations.convert_data_types(mock_df, "date_str", "date")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_to_datetime(self, mock_df):
        """Test data type conversion to datetime."""
        result = CoreTransformations.convert_data_types(
            mock_df, "datetime_str", "datetime"
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_convert_data_types_unsupported_type(self, mock_df):
        """Test data type conversion with unsupported type."""
        result = CoreTransformations.convert_data_types(
            mock_df, "column", "unsupported"
        )

        mock_df.withColumn.assert_not_called()
        assert result == mock_df

    def test_apply_corrections_mapping_basic(self, mock_df):
        """Test applying corrections mapping."""
        corrections = {"old_value": "new_value", "another_old": "another_new"}
        result = CoreTransformations.apply_corrections_mapping(
            mock_df, "name", corrections
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_apply_corrections_mapping_empty_dict(self, mock_df):
        """Test applying empty corrections mapping."""
        corrections = {}
        result = CoreTransformations.apply_corrections_mapping(
            mock_df, "name", corrections
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_apply_corrections_mapping_single_correction(self, mock_df):
        """Test applying single correction."""
        corrections = {"old_value": "new_value"}
        result = CoreTransformations.apply_corrections_mapping(
            mock_df, "name", corrections
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_with_defaults(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with default true/false values."""
        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(mock_df, "active")

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_custom_values(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with custom true/false values."""
        custom_true = ["yes", "active", "enabled"]
        custom_false = ["no", "inactive", "disabled"]

        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(
            mock_df, "active", custom_true, custom_false
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_with_numeric_values(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with numeric values."""
        custom_true = ["1", "1.0", 1, 1.0]
        custom_false = ["0", "0.0", 0, 0.0]

        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(
            mock_df, "active", custom_true, custom_false
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_mixed_types(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with mixed string and numeric types."""
        custom_true = ["yes", 1, "true", 1.0]
        custom_false = ["no", 0, "false", 0.0]

        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(
            mock_df, "active", custom_true, custom_false
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_invalid_numeric_values(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with invalid numeric values."""
        custom_true = ["yes", "not_a_number", "true"]
        custom_false = ["no", "also_not_a_number", "false"]

        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(
            mock_df, "active", custom_true, custom_false
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    @patch("utils.transformations.core_transformations.when")
    @patch("utils.transformations.core_transformations.col")
    @patch("utils.transformations.core_transformations.lower")
    @patch("utils.transformations.core_transformations.lit")
    def test_convert_to_boolean_none_values(
        self, mock_lit, mock_lower, mock_col, mock_when, mock_df
    ):
        """Test boolean conversion with None in value lists."""
        custom_true = ["yes", None, "true"]
        custom_false = ["no", None, "false"]

        # Mock the PySpark functions to avoid the | operator issue
        mock_when.return_value.when.return_value.when.return_value.otherwise.return_value.otherwise.return_value = (
            Mock()
        )

        result = CoreTransformations.convert_to_boolean(
            mock_df, "active", custom_true, custom_false
        )

        mock_df.withColumn.assert_called_once()
        assert result == mock_df

    def test_rename_column_existing(self, mock_df):
        """Test renaming existing column."""
        result = CoreTransformations.rename_column(mock_df, "name", "full_name")

        mock_df.withColumnRenamed.assert_called_once_with("name", "full_name")
        assert result == mock_df

    def test_rename_column_nonexistent(self, mock_df):
        """Test renaming non-existent column."""
        mock_df.columns = ["age", "salary", "active"]  # "name" not in columns

        result = CoreTransformations.rename_column(mock_df, "name", "full_name")

        mock_df.withColumnRenamed.assert_not_called()
        assert result == mock_df

    def test_rename_column_same_name(self, mock_df):
        """Test renaming column to same name."""
        result = CoreTransformations.rename_column(mock_df, "name", "name")

        mock_df.withColumnRenamed.assert_called_once_with("name", "name")
        assert result == mock_df

    def test_rename_column_empty_string(self, mock_df):
        """Test renaming column with empty string."""
        result = CoreTransformations.rename_column(mock_df, "name", "")

        mock_df.withColumnRenamed.assert_called_once_with("name", "")
        assert result == mock_df

    def test_chained_transformations(self, mock_df):
        """Test chaining multiple transformations."""
        # Chain transformations
        result_df = mock_df
        result_df = CoreTransformations.string_cleaning(result_df, ["name"])
        result_df = CoreTransformations.convert_data_types(result_df, "age", "int")
        result_df = CoreTransformations.rename_column(result_df, "name", "full_name")

        # Verify transformations were applied
        assert (
            mock_df.withColumn.call_count >= 2
        )  # string_cleaning + convert_data_types
        mock_df.withColumnRenamed.assert_called_once_with("name", "full_name")
        assert result_df == mock_df
