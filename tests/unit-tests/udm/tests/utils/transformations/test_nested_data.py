# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for nested_data module."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.transformations.nested_data import NestedDataTransformations


class TestNestedDataTransformations:
    """Test cases for NestedDataTransformations class."""

    def test_flatten_column_simple_path(self):
        """Test flattening column with simple path."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch("src.glue.udm.utils.transformations.nested_data.col") as mock_col:
            mock_col.return_value = "col_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = NestedDataTransformations.flatten_column(
                mock_df, "target_col", "simple_col"
            )

            assert result == mock_result_df
            mock_col.assert_called_once_with("simple_col")
            mock_df.withColumn.assert_called_once_with("target_col", "col_expr")

    def test_flatten_column_nested_path_struct_success(self):
        """Test flattening column with nested path using struct access."""
        mock_df = Mock()
        mock_df.columns = ["parent_col"]
        mock_result_df = Mock()

        with patch.object(
            NestedDataTransformations,
            "flatten_column",
            wraps=NestedDataTransformations.flatten_column,
        ) as mock_flatten:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                mock_col.return_value = "nested_col_expr"
                mock_df.withColumn.return_value = mock_result_df

                # Call the actual method
                result = NestedDataTransformations.flatten_column(
                    mock_df, "target_col", "parent_col.nested.field"
                )

                assert result == mock_result_df

    def test_flatten_column_nested_path_json_fallback(self):
        """Test flattening column with nested path falling back to JSON extraction."""
        mock_df = Mock()
        mock_df.columns = ["parent_col"]
        mock_result_df = Mock()
        mock_json_result_df = Mock()

        # Mock struct access failure by making withColumn raise exception first
        mock_df.withColumn.side_effect = [
            Exception("Struct failed"),
            mock_json_result_df,
        ]

        with patch("src.glue.udm.utils.transformations.nested_data.col") as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.get_json_object"
            ) as mock_json:
                mock_col.side_effect = ["nested_col_expr", "parent_col_expr"]
                mock_json.return_value = "json_expr"

                result = NestedDataTransformations.flatten_column(
                    mock_df, "target_col", "parent_col.nested.field"
                )

                assert result == mock_json_result_df

    def test_extract_nested_json_to_struct(self):
        """Test _extract_and_convert_to_struct method."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "utils.transformations.nested_data.from_json"
        ) as mock_from_json, patch("utils.transformations.nested_data.col") as mock_col:
            mock_from_json.return_value = "json_expr"
            mock_col.return_value = "col_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = NestedDataTransformations._extract_and_convert_to_struct(
                mock_df, "target_col", "parent_col", "$.data"
            )

            assert result == mock_result_df
            # The method may call withColumn multiple times, so just verify it was called
            assert mock_df.withColumn.call_count >= 1

    def test_extract_json_with_schema_no_data(self):
        """Test _extract_and_convert_to_struct with no sample data."""
        mock_df = Mock()
        mock_temp_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.nested_data.get_json_object"
        ) as mock_json:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.nested_data.lit"
                ) as mock_lit:
                    mock_col.return_value = Mock()
                    mock_json.return_value = "json_expr"
                    mock_lit.return_value = "lit_none"

                    mock_df.withColumn.return_value = mock_temp_df
                    mock_temp_df.select.return_value.filter.return_value.limit.return_value.collect.return_value = (
                        []
                    )

                    # Mock the exception path since PySpark operations will fail with Mocks
                    mock_temp_df.withColumn.side_effect = Exception(
                        "Mock PySpark error"
                    )
                    mock_df.withColumn.return_value = mock_result_df

                    result = NestedDataTransformations._extract_and_convert_to_struct(
                        mock_df, "target_col", "parent_col", "$.data"
                    )

                    assert result == mock_result_df

    def test_extract_json_with_schema_with_valid_json(self):
        """Test _extract_and_convert_to_struct with valid JSON data."""
        mock_df = Mock()
        mock_temp_df = Mock()
        mock_filtered_df = Mock()
        mock_result_df = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='{"key": "value"}')

        with patch(
            "src.glue.udm.utils.transformations.nested_data.get_json_object"
        ) as mock_json:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.nested_data.schema_of_json"
                ) as mock_schema:
                    with patch(
                        "src.glue.udm.utils.transformations.nested_data.from_json"
                    ) as mock_from_json:
                        with patch(
                            "src.glue.udm.utils.transformations.nested_data.when"
                        ) as mock_when:
                            with patch(
                                "src.glue.udm.utils.transformations.nested_data.lit"
                            ) as mock_lit:
                                # Create mock column expressions with proper PySpark methods
                                mock_col_expr = Mock()
                                mock_col_expr.isNotNull.return_value = Mock()
                                mock_col_expr.isNotNull.return_value.__and__ = Mock(
                                    return_value=Mock()
                                )
                                mock_col.return_value = mock_col_expr

                                mock_json.return_value = "json_expr"
                                mock_schema.return_value = "json_schema"
                                mock_from_json.return_value = "from_json_expr"
                                mock_when.return_value = Mock()
                                mock_when.return_value.otherwise.return_value = (
                                    "when_expr"
                                )
                                mock_lit.side_effect = ["lit_json", "lit_none"]

                                mock_df.withColumn.return_value = mock_temp_df
                                mock_temp_df.select.return_value = mock_filtered_df
                                mock_filtered_df.filter.return_value = mock_filtered_df
                                mock_filtered_df.limit.return_value = mock_filtered_df
                                mock_filtered_df.collect.return_value = [mock_row]

                                mock_temp_df.withColumn.return_value = mock_result_df
                                mock_result_df.drop.return_value = mock_result_df

                                result = NestedDataTransformations._extract_and_convert_to_struct(
                                    mock_df, "target_col", "parent_col", "$.data"
                                )

                                assert result == mock_result_df
                                mock_schema.assert_called_once_with("lit_json")

    def test_extract_json_with_schema_exception_handling(self):
        """Test _extract_and_convert_to_struct exception handling."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.nested_data.get_json_object"
        ) as mock_json:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.nested_data.lit"
                ) as mock_lit:
                    mock_col.return_value = "parent_col_expr"
                    mock_json.return_value = "json_expr"
                    mock_lit.return_value = "lit_none"

                    # Mock withColumn to raise exception
                    mock_df.withColumn.side_effect = [
                        Exception("JSON error"),
                        mock_result_df,
                    ]

                    result = NestedDataTransformations._extract_and_convert_to_struct(
                        mock_df, "target_col", "parent_col", "$.data"
                    )

                    assert result == mock_result_df
                    mock_lit.assert_called_with(None)

    def test_flatten_column_nested_path_parent_missing(self):
        """Test flattening column when parent column doesn't exist."""
        mock_df = Mock()
        mock_df.columns = ["other_col"]  # parent_col missing
        mock_result_df = Mock()

        with patch("src.glue.udm.utils.transformations.nested_data.lit") as mock_lit:
            mock_lit.return_value = "lit_none"
            mock_df.withColumn.return_value = mock_result_df

            result = NestedDataTransformations.flatten_column(
                mock_df, "target_col", "parent_col.nested.field"
            )

            assert result == mock_result_df
            mock_lit.assert_called_once_with(None)
            mock_df.withColumn.assert_called_once_with("target_col", "lit_none")

    def test_flatten_column_exception_handling(self):
        """Test exception handling in flatten_column."""
        mock_df = Mock()
        mock_result_df = Mock()

        # Mock withColumn to raise exception
        mock_df.withColumn.side_effect = [Exception("Some error"), mock_result_df]

        with patch("src.glue.udm.utils.transformations.nested_data.col") as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.lit"
            ) as mock_lit:
                mock_col.return_value = "col_expr"
                mock_lit.return_value = "lit_none"

                result = NestedDataTransformations.flatten_column(
                    mock_df, "target_col", "simple_col"
                )

                assert result == mock_result_df
                mock_lit.assert_called_once_with(None)

    def test_flatten_column_with_target_type(self):
        """Test flattening column with specific target type."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch("src.glue.udm.utils.transformations.nested_data.col") as mock_col:
            mock_col.return_value = "col_expr"
            mock_df.withColumn.return_value = mock_result_df

            result = NestedDataTransformations.flatten_column(
                mock_df, "target_col", "simple_col", "integer"
            )

            assert result == mock_result_df
            mock_col.assert_called_once_with("simple_col")

    def test_flatten_column_nested_path_struct_type(self):
        """Test flattening column with nested path for struct type (lines 39-40)."""
        mock_df = Mock()
        mock_df.columns = ["parent_col"]
        mock_result_df = Mock()

        with patch.object(
            NestedDataTransformations, "_extract_and_convert_to_struct"
        ) as mock_extract:
            mock_extract.return_value = mock_result_df

            result = NestedDataTransformations.flatten_column(
                mock_df, "target_col", "parent_col.nested.field", target_type="struct"
            )

            assert result == mock_result_df
            mock_extract.assert_called_once_with(
                mock_df, "target_col", "parent_col", "$.nested.field"
            )

    def test_extract_and_convert_to_struct_empty_sample_data(self):
        """Test _extract_and_convert_to_struct with empty sample data (line 76)."""
        mock_df = Mock()
        mock_temp_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.nested_data.get_json_object"
        ) as mock_json:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.nested_data.lit"
                ) as mock_lit:
                    mock_col.return_value = Mock()
                    mock_json.return_value = "json_expr"
                    mock_lit.return_value = "lit_none"

                    # First withColumn call creates df_with_json, second call for empty sample data path
                    mock_df.withColumn.side_effect = [mock_temp_df, mock_result_df]
                    mock_temp_df.select.return_value.filter.return_value.limit.return_value.collect.return_value = (
                        []
                    )
                    mock_result_df.drop.return_value = mock_result_df

                    result = NestedDataTransformations._extract_and_convert_to_struct(
                        mock_df, "target_col", "parent_col", "$.path"
                    )

                    # Should return df.withColumn(target_column, lit(None)).drop(temp_col)
                    assert result == mock_result_df

    def test_extract_and_convert_to_struct_overall_exception_handling(self):
        """Test _extract_and_convert_to_struct overall exception handling (lines 107-108)."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.nested_data.get_json_object"
        ) as mock_json:
            with patch(
                "src.glue.udm.utils.transformations.nested_data.col"
            ) as mock_col:
                with patch(
                    "src.glue.udm.utils.transformations.nested_data.lit"
                ) as mock_lit:
                    mock_col.return_value = Mock()
                    mock_json.return_value = "json_expr"
                    mock_lit.return_value = "lit_none"

                    # Mock withColumn to raise exception at the very beginning
                    mock_df.withColumn.side_effect = [
                        Exception("Overall error"),
                        mock_result_df,
                    ]

                    result = NestedDataTransformations._extract_and_convert_to_struct(
                        mock_df, "target_col", "parent_col", "$.path"
                    )

                    # Should return df.withColumn(target_column, lit(None))
                    assert result == mock_result_df
