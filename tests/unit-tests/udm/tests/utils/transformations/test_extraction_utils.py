# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for extraction_utils module."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.transformations.extraction_utils import ExtractionUtils


class TestExtractionUtils:
    """Test cases for ExtractionUtils class."""

    def test_handle_multiple_source_paths_with_valid_paths(self):
        """Test handling multiple source paths with valid data."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.columns = ["col1", "col2"]

        # Mock the extraction and coalesce operations
        mock_temp_df1 = Mock()
        mock_temp_df2 = Mock()
        mock_final_df = Mock()
        mock_cleaned_df = Mock()

        with patch.object(ExtractionUtils, "extract_from_path") as mock_extract:
            mock_extract.side_effect = [mock_temp_df1, mock_temp_df2]

            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.coalesce"
            ) as mock_coalesce:
                with patch(
                    "src.glue.udm.utils.transformations.extraction_utils.col"
                ) as mock_col:
                    mock_col.side_effect = lambda x: f"col({x})"
                    mock_coalesce.return_value = "coalesced_expr"

                    mock_temp_df2.withColumn.return_value = mock_final_df
                    mock_final_df.columns = [
                        "col1",
                        "col2",
                        "test_field",
                        "test_field_temp_0",
                        "test_field_temp_1",
                    ]
                    mock_final_df.drop.return_value = mock_cleaned_df

                    result = ExtractionUtils.handle_multiple_source_paths(
                        mock_df, "test_field", ["path1", "path2"]
                    )

                    assert result == mock_cleaned_df
                    assert mock_extract.call_count == 2
                    mock_extract.assert_any_call(mock_df, "test_field_temp_0", "path1")
                    mock_extract.assert_any_call(
                        mock_temp_df1, "test_field_temp_1", "path2"
                    )
                    mock_final_df.drop.assert_called_once_with(
                        "test_field_temp_0", "test_field_temp_1"
                    )

    def test_handle_multiple_source_paths_empty_paths(self):
        """Test handling empty source paths list."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.lit"
        ) as mock_lit:
            mock_lit.return_value = "lit_none"
            mock_df.withColumn.return_value = mock_result_df

            result = ExtractionUtils.handle_multiple_source_paths(
                mock_df, "test_field", []
            )

            assert result == mock_result_df
            mock_df.withColumn.assert_called_once_with("test_field", "lit_none")
            mock_lit.assert_called_once_with(None)

    def test_handle_multiple_source_paths_no_temp_columns_to_clean(self):
        """Test when no temp columns exist to clean up."""
        mock_df = Mock()
        mock_temp_df = Mock()
        mock_final_df = Mock()

        with patch.object(ExtractionUtils, "extract_from_path") as mock_extract:
            mock_extract.return_value = mock_temp_df

            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.coalesce"
            ) as mock_coalesce:
                with patch(
                    "src.glue.udm.utils.transformations.extraction_utils.col"
                ) as mock_col:
                    mock_col.return_value = "col_expr"
                    mock_coalesce.return_value = "coalesced_expr"

                    mock_temp_df.withColumn.return_value = mock_final_df
                    mock_final_df.columns = [
                        "col1",
                        "col2",
                        "test_field",
                    ]  # No temp columns

                    result = ExtractionUtils.handle_multiple_source_paths(
                        mock_df, "test_field", ["path1"]
                    )

                    assert result == mock_final_df

    def test_extract_from_path_simple_column(self):
        """Test extracting from simple column path."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            mock_col.return_value = "col_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = ExtractionUtils.extract_from_path(mock_df, "target", "simple_col")

            assert result == mock_result_df
            mock_col.assert_called_once_with("simple_col")
            mock_df.withColumn.assert_called_once_with("target", "col_expr")

    def test_extract_from_path_nested_struct_success(self):
        """Test extracting from nested struct path successfully."""
        mock_df = Mock()
        mock_df.columns = ["parent_col", "other_col"]
        mock_result_df = Mock()
        mock_schema = Mock()
        mock_result_df.schema = mock_schema

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            mock_col.return_value = "nested_col_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = ExtractionUtils.extract_from_path(
                mock_df, "target", "parent_col.nested.field"
            )

            assert result == mock_result_df
            mock_col.assert_called_once_with("parent_col.nested.field")
            mock_df.withColumn.assert_called_once_with("target", "nested_col_expr")

    def test_extract_from_path_nested_struct_fallback_to_json(self):
        """Test extracting from nested path with struct failure, fallback to JSON."""
        mock_df = Mock()
        mock_df.columns = ["parent_col", "other_col"]
        mock_result_df = Mock()
        mock_json_result_df = Mock()

        # Mock struct access failure by making schema property raise exception
        def schema_side_effect():
            raise Exception("Struct access failed")

        type(mock_result_df).schema = property(schema_side_effect)

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.get_json_object"
            ) as mock_json:
                mock_col.side_effect = ["nested_col_expr", "parent_col_expr"]
                mock_json.return_value = "json_expr"

                # First call returns mock_result_df, second call returns mock_json_result_df
                mock_df.withColumn.side_effect = [mock_result_df, mock_json_result_df]

                result = ExtractionUtils.extract_from_path(
                    mock_df, "target", "parent_col.nested.field"
                )

                assert result == mock_json_result_df
                mock_json.assert_called_once_with("parent_col_expr", "$.nested.field")

    def test_extract_from_path_nested_parent_column_missing(self):
        """Test extracting from nested path when parent column doesn't exist."""
        mock_df = Mock()
        mock_df.columns = ["other_col"]  # parent_col missing
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.lit"
        ) as mock_lit:
            mock_lit.return_value = "lit_none"
            mock_df.withColumn.return_value = mock_result_df

            result = ExtractionUtils.extract_from_path(
                mock_df, "target", "parent_col.nested.field"
            )

            assert result == mock_result_df
            mock_lit.assert_called_once_with(None)
            mock_df.withColumn.assert_called_once_with("target", "lit_none")

    def test_extract_from_path_exception_handling(self):
        """Test exception handling in extract_from_path."""
        mock_df = Mock()
        mock_result_df = Mock()

        # Mock withColumn to raise exception
        mock_df.withColumn.side_effect = [Exception("Some error"), mock_result_df]

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.lit"
            ) as mock_lit:
                mock_col.return_value = "col_expr"
                mock_lit.return_value = "lit_none"

                result = ExtractionUtils.extract_from_path(
                    mock_df, "target", "simple_col"
                )

                assert result == mock_result_df
                mock_lit.assert_called_once_with(None)
                # Should be called twice - first fails, second succeeds with lit(None)
                assert mock_df.withColumn.call_count == 2

    def test_extract_from_path_nested_json_extraction_exception(self):
        """Test exception during JSON extraction in nested path."""
        mock_df = Mock()
        mock_df.columns = ["parent_col"]
        mock_result_df = Mock()
        mock_final_df = Mock()

        # Mock struct access failure
        def schema_side_effect():
            raise Exception("Struct failed")

        type(mock_result_df).schema = property(schema_side_effect)

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.get_json_object"
            ) as mock_json:
                with patch(
                    "src.glue.udm.utils.transformations.extraction_utils.lit"
                ) as mock_lit:
                    mock_col.side_effect = ["nested_col_expr", "parent_col_expr"]
                    mock_json.return_value = "json_expr"
                    mock_lit.return_value = "lit_none"

                    # First withColumn for struct access returns mock_result_df
                    # Second withColumn for JSON extraction raises exception
                    # Third withColumn for lit(None) returns mock_final_df
                    mock_df.withColumn.side_effect = [
                        mock_result_df,
                        Exception("JSON extraction failed"),
                        mock_final_df,
                    ]

                    result = ExtractionUtils.extract_from_path(
                        mock_df, "target", "parent_col.nested.field"
                    )

                    assert result == mock_final_df
                    mock_lit.assert_called_once_with(None)

    def test_extract_from_path_deeply_nested_path(self):
        """Test extracting from deeply nested path."""
        mock_df = Mock()
        mock_df.columns = ["parent_col"]
        mock_result_df = Mock()
        mock_schema = Mock()
        mock_result_df.schema = mock_schema

        with patch(
            "src.glue.udm.utils.transformations.extraction_utils.col"
        ) as mock_col:
            mock_col.return_value = "deeply_nested_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = ExtractionUtils.extract_from_path(
                mock_df, "target", "parent_col.level1.level2.level3.field"
            )

            assert result == mock_result_df
            mock_col.assert_called_once_with("parent_col.level1.level2.level3.field")

    def test_handle_multiple_source_paths_partial_temp_columns(self):
        """Test cleanup when only some temp columns exist."""
        mock_df = Mock()
        mock_temp_df1 = Mock()
        mock_temp_df2 = Mock()
        mock_final_df = Mock()
        mock_cleaned_df = Mock()

        with patch.object(ExtractionUtils, "extract_from_path") as mock_extract:
            mock_extract.side_effect = [mock_temp_df1, mock_temp_df2]

            with patch(
                "src.glue.udm.utils.transformations.extraction_utils.coalesce"
            ) as mock_coalesce:
                with patch(
                    "src.glue.udm.utils.transformations.extraction_utils.col"
                ) as mock_col:
                    mock_col.side_effect = lambda x: f"col({x})"
                    mock_coalesce.return_value = "coalesced_expr"

                    mock_temp_df2.withColumn.return_value = mock_final_df
                    # Only one temp column exists
                    mock_final_df.columns = [
                        "col1",
                        "col2",
                        "test_field",
                        "test_field_temp_0",
                    ]
                    mock_final_df.drop.return_value = mock_cleaned_df

                    result = ExtractionUtils.handle_multiple_source_paths(
                        mock_df, "test_field", ["path1", "path2"]
                    )

                    assert result == mock_cleaned_df
                    # Should only drop the existing temp column
                    mock_final_df.drop.assert_called_once_with("test_field_temp_0")
