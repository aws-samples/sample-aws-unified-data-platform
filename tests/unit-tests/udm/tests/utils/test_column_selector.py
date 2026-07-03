# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for column_selector module."""

# pylint: disable=C0301,C0415

from argparse import Namespace
from unittest.mock import Mock, patch

import pytest
from utils.column_selector import ColumnSelector


class TestColumnSelector:
    """Test cases for ColumnSelector class."""

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock()
        mock_df.columns = [
            "id",
            "name",
            "age",
            "salary",
            "department",
            "email",
            "address",
            "phone",
        ]
        mock_df.select.return_value = mock_df
        return mock_df

    def test_select_target_columns_basic(self, mock_df):
        """Test basic column selection."""
        target_columns = ["id", "name", "department"]
        config = {"columns": {}}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("id", "name", "department")
        assert result == mock_df

    def test_select_target_columns_with_missing_columns(self, mock_df):
        """Test column selection when some target columns don't exist."""
        target_columns = ["id", "name", "nonexistent_column", "department"]
        config = {"columns": {}}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        # Should only select existing columns
        mock_df.select.assert_called_once_with("id", "name", "department")
        assert result == mock_df

    def test_select_target_columns_empty_list(self, mock_df):
        """Test column selection with empty target list."""
        target_columns = []
        config = {"columns": {}}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        # Should return original dataframe when no columns selected
        mock_df.select.assert_not_called()
        assert result == mock_df

    def test_select_target_columns_with_array_explode(self, mock_df):
        """Test column selection with array_explode type."""
        mock_df.columns = ["id", "name", "exploded_field1", "exploded_field2"]
        target_columns = ["array_col"]
        config = {
            "columns": {
                "array_col": {
                    "type": "array_explode",
                    "explode_columns": {"exploded_field1": {}, "exploded_field2": {}},
                }
            }
        }

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("exploded_field1", "exploded_field2")
        assert result == mock_df

    def test_select_target_columns_with_metadata_extractor(self, mock_df):
        """Test column selection with metadata_extractor type."""
        mock_df.columns = ["id", "name", "meta_field1", "meta_field2"]
        target_columns = ["metadata_col"]
        config = {
            "columns": {
                "metadata_col": {
                    "type": "metadata_extractor",
                    "explode_columns": {"meta_field1": {}, "meta_field2": {}},
                }
            }
        }

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("meta_field1", "meta_field2")
        assert result == mock_df

    def test_select_target_columns_simple_array_without_explode_columns(self, mock_df):
        """Test column selection with simple array type without explode_columns."""
        # Add simple_array to available columns
        mock_df.columns = [
            "id",
            "name",
            "age",
            "salary",
            "department",
            "email",
            "address",
            "phone",
            "simple_array",
        ]
        target_columns = ["simple_array"]
        config = {"columns": {"simple_array": {"type": "array_explode"}}}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        # Should include the column itself when no explode_columns defined
        mock_df.select.assert_called_once_with("simple_array")
        assert result == mock_df

    def test_select_target_columns_mixed_types(self, mock_df):
        """Test column selection with mixed column types."""
        mock_df.columns = ["id", "name", "exploded1", "exploded2", "regular_col"]
        target_columns = ["array_col", "regular_col"]
        config = {
            "columns": {
                "array_col": {
                    "type": "array_explode",
                    "explode_columns": {"exploded1": {}, "exploded2": {}},
                },
                "regular_col": {"type": "string"},
            }
        }

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("regular_col", "exploded1", "exploded2")
        assert result == mock_df

    def test_select_target_columns_with_namespace_config(self, mock_df):
        """Test column selection with Namespace config."""
        target_columns = ["id", "name"]
        config = Namespace(columns={})

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("id", "name")
        assert result == mock_df

    def test_select_target_columns_no_columns_config(self, mock_df):
        """Test column selection when columns config is missing."""
        target_columns = ["id", "name"]
        config = {}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        mock_df.select.assert_called_once_with("id", "name")
        assert result == mock_df

    def test_select_target_columns_exception_handling(self, mock_df):
        """Test exception handling during column selection."""
        target_columns = ["id", "name"]
        config = {"columns": {}}

        # Mock select to raise an exception
        mock_df.select.side_effect = Exception("Column selection failed")

        with patch("utils.column_selector.logger") as mock_logger:
            with pytest.raises(Exception, match="Column selection failed"):
                ColumnSelector.select_target_columns(mock_df, target_columns, config)

            mock_logger.error.assert_any_call(
                "[COLUMN_SELECTOR] Failed to select columns: Column selection failed"
            )
            mock_logger.error.assert_any_call(
                "[COLUMN_SELECTOR] Attempted to select: ['id', 'name']"
            )

    def test_select_target_columns_no_available_columns(self, mock_df):
        """Test when no target columns are available in dataframe."""
        mock_df.columns = ["other1", "other2"]
        target_columns = ["id", "name"]
        config = {"columns": {}}

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        # Should return original dataframe when no columns match
        mock_df.select.assert_not_called()
        assert result == mock_df

    def test_select_target_columns_duplicate_exploded_columns(self, mock_df):
        """Test handling of duplicate exploded columns."""
        mock_df.columns = ["id", "exploded1", "exploded2"]
        target_columns = ["array_col1", "array_col2"]
        config = {
            "columns": {
                "array_col1": {
                    "type": "array_explode",
                    "explode_columns": {"exploded1": {}, "exploded2": {}},
                },
                "array_col2": {
                    "type": "metadata_extractor",
                    "explode_columns": {
                        "exploded1": {},  # Duplicate
                        "exploded2": {},  # Duplicate
                    },
                },
            }
        }

        result = ColumnSelector.select_target_columns(mock_df, target_columns, config)

        # Should handle duplicates properly
        mock_df.select.assert_called_once()
        call_args = mock_df.select.call_args[0]
        # Should contain exploded1 and exploded2, but duplicates should be handled by list
        assert "exploded1" in call_args
        assert "exploded2" in call_args
        assert result == mock_df
