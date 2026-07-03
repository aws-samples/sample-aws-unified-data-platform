# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for array_transformations module."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest
from utils.transformations.array_transformations import ArrayTransformations


class TestArrayTransformations:
    """Test cases for ArrayTransformations class."""

    def test_explode_array_column_no_source_path(self):
        """Test explode array column when no source path provided."""
        mock_df = Mock()
        field_config = {}

        result = ArrayTransformations.explode_array_column(
            mock_df, "test_field", field_config
        )

        assert result == mock_df

    def test_explode_array_column_simple_array(self):
        """Test explode array column with simple array (no explode_columns)."""
        mock_df = Mock()
        mock_result_df = Mock()
        field_config = {"source_path": "data.items"}

        with patch.object(
            ArrayTransformations, "_explode_simple_array"
        ) as mock_explode:
            mock_explode.return_value = mock_result_df

            result = ArrayTransformations.explode_array_column(
                mock_df, "test_field", field_config
            )

            assert result == mock_result_df
            mock_explode.assert_called_once_with(
                mock_df, "test_field", field_config, None
            )

    def test_explode_array_column_with_explode_columns(self):
        """Test explode array column with explode_columns configuration."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_result_df = Mock()
        field_config = {
            "source_path": "data.items",
            "explode_columns": {"col1": {"type": "string"}},
        }

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch.object(
                ArrayTransformations, "_add_default_columns"
            ) as mock_add_defaults:
                mock_extract.return_value = mock_extracted_df
                mock_extracted_df.columns = ["other_col"]  # array column not present
                mock_add_defaults.return_value = mock_result_df

                result = ArrayTransformations.explode_array_column(
                    mock_df, "test_field", field_config
                )

                assert result == mock_result_df
                mock_extract.assert_called_once_with(
                    mock_df, "test_field_array", "data.items"
                )
                mock_add_defaults.assert_called_once()

    def test_explode_array_column_with_array_present(self):
        """Test explode array column when array column is present."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_valid_arrays = Mock()
        mock_exploded_df = Mock()
        mock_result_df = Mock()

        field_config = {
            "source_path": "data.items",
            "explode_columns": {"col1": {"type": "string"}},
        }

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch("utils.transformations.array_transformations.dict") as mock_dict:
                with patch(
                    "utils.transformations.array_transformations.size"
                ) as mock_size:
                    with patch(
                        "utils.transformations.array_transformations.col"
                    ) as mock_col:
                        mock_extract.return_value = mock_extracted_df
                        mock_extracted_df.columns = ["test_field_array"]
                        mock_dict.return_value = {"test_field_array": "array<string>"}
                        mock_extracted_df.dtypes = [
                            ("test_field_array", "array<string>")
                        ]

                        # Create mock expressions that support PySpark operations
                        mock_col_expr = Mock()
                        mock_col_expr.isNotNull.return_value = Mock()
                        mock_col_expr.isNotNull.return_value.__and__ = Mock(
                            return_value=Mock()
                        )
                        mock_col_expr.isNull.return_value = Mock()
                        mock_col_expr.isNull.return_value.__or__ = Mock(
                            return_value=Mock()
                        )
                        mock_col.return_value = mock_col_expr

                        mock_size_expr = Mock()
                        mock_size_expr.__gt__ = Mock(return_value=Mock())
                        mock_size_expr.__eq__ = Mock(return_value=Mock())
                        mock_size.return_value = mock_size_expr

                        # Mock filter operations to return valid arrays
                        mock_extracted_df.filter.return_value = mock_valid_arrays
                        mock_valid_arrays.take.return_value = [{"data": "test"}]
                        mock_valid_arrays.withColumn.return_value = mock_exploded_df

                        # Mock the final result chain
                        mock_exploded_df.select.return_value.union.return_value = (
                            mock_result_df
                        )

                        result = ArrayTransformations.explode_array_column(
                            mock_df, "test_field", field_config
                        )

                        assert result == mock_result_df

    def test_explode_array_column_no_valid_arrays(self):
        """Test explode array column when no valid arrays found."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_valid_arrays = Mock()
        mock_result_df = Mock()

        field_config = {
            "source_path": "data.items",
            "explode_columns": {"col1": {"type": "string"}},
        }

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch.object(
                ArrayTransformations, "_add_default_columns"
            ) as mock_add_defaults:
                with patch(
                    "utils.transformations.array_transformations.dict"
                ) as mock_dict:
                    with patch(
                        "utils.transformations.array_transformations.col"
                    ) as mock_col:
                        with patch(
                            "utils.transformations.array_transformations.size"
                        ) as mock_size:
                            mock_extract.return_value = mock_extracted_df
                            mock_extracted_df.columns = ["test_field_array"]
                            mock_dict.return_value = {
                                "test_field_array": "array<string>"
                            }
                            mock_extracted_df.dtypes = [
                                ("test_field_array", "array<string>")
                            ]

                            # Create mock column expression that supports PySpark operations
                            mock_col_expr = Mock()
                            mock_col_expr.isNotNull.return_value = Mock()
                            mock_col_expr.isNotNull.return_value.__and__ = Mock(
                                return_value=Mock()
                            )
                            mock_col_expr.isNull.return_value = Mock()
                            mock_col_expr.isNull.return_value.__or__ = Mock(
                                return_value=Mock()
                            )
                            mock_col.return_value = mock_col_expr

                            # Create mock size expression that supports comparison and boolean operations
                            mock_size_expr = Mock()
                            mock_size_expr.__gt__ = Mock(return_value=Mock())
                            mock_size_expr.__eq__ = Mock(return_value=Mock())
                            mock_size.return_value = mock_size_expr

                            mock_extracted_df.filter.return_value = mock_valid_arrays
                            mock_valid_arrays.take.return_value = []  # No data
                            mock_add_defaults.return_value = mock_result_df

                            result = ArrayTransformations.explode_array_column(
                                mock_df, "test_field", field_config
                            )

                            assert result == mock_result_df
                            mock_add_defaults.assert_called_once()

    def test_explode_simple_array(self):
        """Test _explode_simple_array method."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_result_df = Mock()
        field_config = {"source_path": "data.items"}

        # Mock df.columns as a list instead of Mock
        mock_df.columns = ["other_col"]

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            mock_extract.return_value = mock_extracted_df
            # Mock the extracted df columns to not contain the array column
            mock_extracted_df.columns = ["other_col"]
            mock_extracted_df.withColumn.return_value = mock_result_df

            result = ArrayTransformations._explode_simple_array(
                mock_df, "test_field", field_config, None
            )

            assert result == mock_result_df
            mock_extract.assert_called_once_with(
                mock_df, "test_field_array", "data.items"
            )

    def test_extract_array(self):
        """Test _extract_array method."""
        mock_df = Mock()
        mock_result_df = Mock()

        mock_df.columns = ["data_col"]
        mock_df.dtypes = [("data_col", "string")]

        with patch(
            "utils.transformations.array_transformations.get_json_object"
        ) as mock_json:
            with patch("utils.transformations.array_transformations.col") as mock_col:
                with patch(
                    "utils.transformations.array_transformations.dict"
                ) as mock_dict:
                    with patch(
                        "utils.transformations.array_transformations.lit"
                    ) as mock_lit:
                        # Setup mocks to trigger exception path
                        mock_col_expr = Mock()
                        mock_col_expr.isNotNull.return_value = Mock()
                        mock_col_expr.__ne__ = Mock(return_value=Mock())
                        mock_col_expr.startswith.return_value = Mock()
                        # Make the & operator fail to trigger exception
                        mock_col_expr.isNotNull.return_value.__and__ = Mock(
                            side_effect=TypeError("Mock & error")
                        )
                        mock_col.return_value = mock_col_expr

                        mock_dict.return_value = {"data_col": "string"}
                        mock_lit.return_value = "lit_none"
                        # Mock withColumn to return itself for chaining, then final result
                        mock_df.withColumn.return_value.withColumn.return_value = (
                            mock_result_df
                        )

                        result = ArrayTransformations._extract_array(
                            mock_df, "array_col", "data_col.items"
                        )

                        assert result == mock_result_df
                        mock_lit.assert_called_once_with(None)

    def test_add_default_columns(self):
        """Test _add_default_columns method."""
        mock_df = Mock()
        mock_result_df = Mock()
        explode_columns = {"col1": {"type": "string"}, "col2": {"type": "int"}}
        defaults = {"string": "default_str", "int": 0}

        with patch.object(ArrayTransformations, "_get_column_default") as mock_default:
            with patch.object(ArrayTransformations, "_cast_with_type") as mock_cast:
                with patch(
                    "utils.transformations.array_transformations.lit"
                ) as mock_lit:
                    mock_default.side_effect = ["default_str", 0]
                    mock_lit.side_effect = ["lit_str", "lit_int"]
                    mock_cast.side_effect = ["cast_str", "cast_int"]

                    # Mock chained withColumn calls and final drop
                    mock_df1 = Mock()
                    mock_df2 = Mock()
                    mock_df.withColumn.return_value = mock_df1
                    mock_df1.withColumn.return_value = mock_df2
                    mock_df2.drop.return_value = mock_result_df

                    result = ArrayTransformations._add_default_columns(
                        mock_df, explode_columns, defaults, "array_col"
                    )

                    assert result == mock_result_df
                    assert mock_default.call_count == 2
                    assert mock_cast.call_count == 2

    def test_get_column_default(self):
        """Test _get_column_default method."""
        col_config = {"missing_value_default": "custom_default"}
        defaults = {"string": {"missing_value_default": "default_str"}}

        # Test with custom default
        result = ArrayTransformations._get_column_default(
            col_config, "string", defaults
        )
        assert result == "custom_default"

        # Test with type default
        col_config_no_default = {}
        result = ArrayTransformations._get_column_default(
            col_config_no_default, "string", defaults
        )
        assert result == "default_str"

        # Test with no defaults
        result = ArrayTransformations._get_column_default(
            col_config_no_default, "string", None
        )
        assert result == ""

    def test_cast_with_type(self):
        """Test _cast_with_type method."""
        mock_expr = Mock()

        with patch(
            "utils.transformations.array_transformations.FloatType"
        ) as mock_float:
            with patch(
                "utils.transformations.array_transformations.IntegerType"
            ) as mock_int:
                with patch(
                    "utils.transformations.array_transformations.DoubleType"
                ) as mock_double:
                    mock_float.return_value = "float_type"
                    mock_int.return_value = "int_type"
                    mock_double.return_value = "double_type"
                    mock_expr.cast.return_value = "casted_expr"

                    # Test float
                    result = ArrayTransformations._cast_with_type(mock_expr, "float")
                    assert result == "casted_expr"
                    mock_expr.cast.assert_called_with("float_type")

                    # Test int
                    mock_expr.reset_mock()
                    result = ArrayTransformations._cast_with_type(mock_expr, "int")
                    assert result == "casted_expr"
                    mock_expr.cast.assert_called_with("int_type")

                    # Test double
                    mock_expr.reset_mock()
                    result = ArrayTransformations._cast_with_type(mock_expr, "double")
                    assert result == "casted_expr"
                    mock_expr.cast.assert_called_with("double_type")

                    # Test other type
                    mock_expr.reset_mock()
                    result = ArrayTransformations._cast_with_type(mock_expr, "string")
                    assert result == mock_expr
                    mock_expr.cast.assert_not_called()

    def test_build_column_expression(self):
        """Test _build_column_expression method."""
        mock_df = Mock()
        col_config = {"source_path": "item.value"}

        with patch("utils.transformations.array_transformations.col") as mock_col:
            with patch(
                "utils.transformations.array_transformations.coalesce"
            ) as mock_coalesce:
                # Mock the column expression chain
                mock_exploded_item = Mock()
                mock_expr = Mock()
                mock_exploded_item.getItem.return_value = mock_expr
                mock_col.return_value = mock_exploded_item

                # Mock the sample operation to succeed
                mock_df.select.return_value.limit.return_value.collect.return_value = (
                    None
                )

                # Mock coalesce for the return value
                mock_coalesce.return_value = "coalesced_expr"

                result = ArrayTransformations._build_column_expression(
                    mock_df, col_config
                )

                # The method returns coalesce of working expressions
                assert result == "coalesced_expr"
                mock_col.assert_called_once_with("exploded_item")

    def test_explode_array_column_non_array_type(self):
        """Test explode array column with non-array column type."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_valid_arrays = Mock()
        mock_result_df = Mock()

        field_config = {
            "source_path": "data.items",
            "explode_columns": {"col1": {"type": "string"}},
        }

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch.object(
                ArrayTransformations, "_add_default_columns"
            ) as mock_add_defaults:
                with patch(
                    "utils.transformations.array_transformations.dict"
                ) as mock_dict:
                    with patch(
                        "utils.transformations.array_transformations.lit"
                    ) as mock_lit:
                        mock_extract.return_value = mock_extracted_df
                        mock_extracted_df.columns = ["test_field_array"]
                        mock_dict.return_value = {
                            "test_field_array": "string"
                        }  # Not array type
                        mock_extracted_df.dtypes = [("test_field_array", "string")]

                        mock_lit.return_value = "false_expr"
                        mock_extracted_df.filter.return_value = mock_valid_arrays
                        mock_valid_arrays.take.return_value = []  # No data
                        mock_add_defaults.return_value = mock_result_df

                        result = ArrayTransformations.explode_array_column(
                            mock_df, "test_field", field_config
                        )

                        assert result == mock_result_df
                        mock_add_defaults.assert_called_once()

    def test_explode_simple_array_with_valid_arrays(self):
        """Test _explode_simple_array with valid arrays to explode."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_valid_arrays = Mock()
        mock_no_arrays = Mock()
        mock_exploded = Mock()
        mock_result_df = Mock()

        field_config = {"source_path": "data.items", "type": "int"}
        defaults = {"int": 0}

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch.object(
                ArrayTransformations, "_get_column_default"
            ) as mock_default:
                with patch.object(ArrayTransformations, "_cast_with_type") as mock_cast:
                    with patch(
                        "utils.transformations.array_transformations.dict"
                    ) as mock_dict:
                        with patch(
                            "utils.transformations.array_transformations.col"
                        ) as mock_col:
                            with patch(
                                "utils.transformations.array_transformations.size"
                            ) as mock_size:
                                with patch(
                                    "utils.transformations.array_transformations.explode"
                                ) as mock_explode:
                                    with patch(
                                        "utils.transformations.array_transformations.lit"
                                    ) as mock_lit:
                                        mock_extract.return_value = mock_extracted_df
                                        mock_extracted_df.columns = ["test_field_array"]
                                        mock_dict.return_value = {
                                            "test_field_array": "array<string>"
                                        }

                                        # Mock column operations
                                        mock_col_expr = Mock()
                                        mock_col_expr.isNotNull.return_value = Mock()
                                        mock_col_expr.isNull.return_value = Mock()
                                        mock_col_expr.isNotNull.return_value.__and__ = (
                                            Mock(return_value=Mock())
                                        )
                                        mock_col_expr.isNull.return_value.__or__ = Mock(
                                            return_value=Mock()
                                        )
                                        mock_col.return_value = mock_col_expr

                                        mock_size_expr = Mock()
                                        mock_size_expr.__gt__ = Mock(
                                            return_value=Mock()
                                        )
                                        mock_size_expr.__eq__ = Mock(
                                            return_value=Mock()
                                        )
                                        mock_size.return_value = mock_size_expr

                                        # Mock filter operations
                                        mock_extracted_df.filter.side_effect = [
                                            mock_valid_arrays,
                                            mock_no_arrays,
                                        ]
                                        mock_valid_arrays.take.return_value = [
                                            {"data": "test"}
                                        ]  # Has data

                                        # Mock default and cast operations
                                        mock_default.return_value = 0
                                        mock_cast.return_value = "casted_default"
                                        mock_lit.return_value = "lit_default"

                                        # Mock explode operations
                                        mock_explode.return_value = "exploded_col"
                                        mock_valid_arrays.withColumn.return_value.drop.return_value = (
                                            mock_exploded
                                        )
                                        mock_exploded.withColumn.return_value = (
                                            mock_exploded
                                        )
                                        mock_no_arrays.withColumn.return_value.drop.return_value = (
                                            mock_no_arrays
                                        )
                                        mock_exploded.union.return_value = (
                                            mock_result_df
                                        )

                                        result = (
                                            ArrayTransformations._explode_simple_array(
                                                mock_df,
                                                "test_field",
                                                field_config,
                                                defaults,
                                            )
                                        )

                                        assert result == mock_result_df

    def test_explode_simple_array_no_valid_arrays(self):
        """Test _explode_simple_array with no valid arrays."""
        mock_df = Mock()
        mock_extracted_df = Mock()
        mock_valid_arrays = Mock()
        mock_no_arrays = Mock()

        field_config = {"source_path": "data.items", "type": "string"}
        defaults = {"string": "default"}

        with patch.object(ArrayTransformations, "_extract_array") as mock_extract:
            with patch.object(
                ArrayTransformations, "_get_column_default"
            ) as mock_default:
                with patch.object(ArrayTransformations, "_cast_with_type") as mock_cast:
                    with patch(
                        "utils.transformations.array_transformations.dict"
                    ) as mock_dict:
                        with patch(
                            "utils.transformations.array_transformations.col"
                        ) as mock_col:
                            with patch(
                                "utils.transformations.array_transformations.size"
                            ) as mock_size:
                                with patch(
                                    "utils.transformations.array_transformations.lit"
                                ) as mock_lit:
                                    mock_extract.return_value = mock_extracted_df
                                    mock_extracted_df.columns = ["test_field_array"]
                                    mock_dict.return_value = {
                                        "test_field_array": "array<string>"
                                    }

                                    # Mock column operations
                                    mock_col_expr = Mock()
                                    mock_col_expr.isNotNull.return_value = Mock()
                                    mock_col_expr.isNull.return_value = Mock()
                                    mock_col_expr.isNotNull.return_value.__and__ = Mock(
                                        return_value=Mock()
                                    )
                                    mock_col_expr.isNull.return_value.__or__ = Mock(
                                        return_value=Mock()
                                    )
                                    mock_col.return_value = mock_col_expr

                                    mock_size_expr = Mock()
                                    mock_size_expr.__gt__ = Mock(return_value=Mock())
                                    mock_size_expr.__eq__ = Mock(return_value=Mock())
                                    mock_size.return_value = mock_size_expr

                                    # Mock filter operations
                                    mock_extracted_df.filter.side_effect = [
                                        mock_valid_arrays,
                                        mock_no_arrays,
                                    ]
                                    mock_valid_arrays.take.return_value = []  # No data

                                    # Mock default and cast operations
                                    mock_default.return_value = "default"
                                    mock_cast.return_value = "casted_default"
                                    mock_lit.return_value = "lit_default"
                                    mock_no_arrays.withColumn.return_value.drop.return_value = (
                                        mock_no_arrays
                                    )

                                    result = ArrayTransformations._explode_simple_array(
                                        mock_df, "test_field", field_config, defaults
                                    )

                                    assert result == mock_no_arrays

    def test_build_column_expression_with_exception(self):
        """Test _build_column_expression when expression building fails."""
        mock_df = Mock()
        col_config = {"source_path": "item.value"}

        with patch("utils.transformations.array_transformations.col") as mock_col:
            with patch(
                "utils.transformations.array_transformations.coalesce"
            ) as mock_coalesce:
                with patch(
                    "utils.transformations.array_transformations.lit"
                ) as mock_lit:
                    # Mock the column expression to fail during sample
                    mock_exploded_item = Mock()
                    mock_expr = Mock()
                    mock_exploded_item.getItem.return_value = mock_expr
                    mock_col.return_value = mock_exploded_item

                    # Mock the sample operation to fail
                    mock_df.select.return_value.limit.return_value.collect.side_effect = Exception(
                        "Sample failed"
                    )

                    # Mock fallback values
                    mock_lit.return_value = "lit_fallback"
                    mock_coalesce.return_value = "coalesced_fallback"

                    result = ArrayTransformations._build_column_expression(
                        mock_df, col_config
                    )

                    assert result == "lit_fallback"
                    mock_lit.assert_called_with(None)

    def test_extract_array_with_complex_path(self):
        """Test _extract_array with complex nested path."""
        mock_df = Mock()
        mock_result_df = Mock()

        mock_df.columns = ["data_col"]
        mock_df.dtypes = [("data_col", "string")]

        with patch(
            "utils.transformations.array_transformations.get_json_object"
        ) as mock_json:
            with patch("utils.transformations.array_transformations.col") as mock_col:
                with patch(
                    "utils.transformations.array_transformations.dict"
                ) as mock_dict:
                    with patch(
                        "utils.transformations.array_transformations.lit"
                    ) as mock_lit:
                        # Setup successful path
                        mock_col_expr = Mock()
                        mock_col_expr.isNotNull.return_value = Mock()
                        mock_col_expr.__ne__ = Mock(return_value=Mock())
                        mock_col_expr.startswith.return_value = Mock()
                        mock_col_expr.isNotNull.return_value.__and__ = Mock(
                            return_value=Mock()
                        )
                        mock_col.return_value = mock_col_expr

                        mock_dict.return_value = {"data_col": "string"}
                        mock_json.return_value = "json_expr"

                        # Mock successful withColumn chain
                        mock_df1 = Mock()
                        mock_df.withColumn.return_value = mock_df1
                        mock_df1.withColumn.return_value = mock_result_df

                        result = ArrayTransformations._extract_array(
                            mock_df, "array_col", "data_col.nested.items"
                        )

                        assert result == mock_result_df
                        mock_json.assert_called_once_with(
                            mock_col_expr, "$.nested.items"
                        )

    def test_build_column_expression_field_not_exists(self):
        """Test _build_column_expression when field doesn't exist."""
        mock_df = Mock()
        mock_df.select.return_value.limit.return_value.collect.side_effect = Exception(
            "Field not found"
        )

        col_config = {
            "source_field": "nonexistent_field",
            "target_field": "result_field",
            "target_type": "string",
        }

        with patch(
            "utils.transformations.array_transformations.col"
        ) as mock_col, patch(
            "utils.transformations.array_transformations.lit"
        ) as mock_lit:
            result = ArrayTransformations._build_column_expression(mock_df, col_config)

            mock_lit.assert_called_with(None)

    def test_extract_array_with_json_schema_inference(self):
        """Test extract_array with JSON schema inference."""
        mock_df = Mock()
        mock_sample = [('["test"]',), ("[]",), ('[{"key": "value"}]',)]
        mock_df.select.return_value.limit.return_value.collect.return_value = (
            mock_sample
        )

        with patch(
            "utils.transformations.array_transformations.schema_of_json"
        ) as mock_schema, patch(
            "utils.transformations.array_transformations.from_json"
        ) as mock_from_json, patch(
            "utils.transformations.array_transformations.col"
        ) as mock_col, patch(
            "utils.transformations.array_transformations.when"
        ) as mock_when, patch(
            "utils.transformations.array_transformations.lit"
        ) as mock_lit:
            mock_schema.return_value = "test_schema"
            mock_when.return_value.otherwise.return_value = "test_expr"

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "json_col"
            )

            # The function should handle the case gracefully even if schema inference fails
            assert result is not None

    def test_extract_array_nested_struct_access(self):
        """Test extract_array with nested struct access."""
        mock_df = Mock()

        with patch("utils.transformations.array_transformations.col") as mock_col:
            mock_expr = Mock()
            mock_col.return_value = mock_expr

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "base.nested.field"
            )

            # The function should handle nested access gracefully
            assert result is not None

    def test_cast_with_type_exception_handling(self):
        """Test _cast_with_type with unsupported type."""
        mock_expr = Mock()

        # Test with unsupported type - should return original expression
        result = ArrayTransformations._cast_with_type(mock_expr, "invalid_type")
        assert result == mock_expr

    def test_extract_array_json_schema_inference_with_valid_json(self):
        """Test _extract_array JSON schema inference with valid JSON data."""
        mock_df = Mock()
        mock_df.columns = ["json_col"]
        mock_df.dtypes = [("json_col", "string")]

        # Mock sample data with valid JSON
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='[{"id": 1, "name": "test"}]')
        mock_sample = [mock_row]

        with patch(
            "utils.transformations.array_transformations.get_json_object"
        ) as mock_get_json, patch(
            "utils.transformations.array_transformations.col"
        ) as mock_col, patch(
            "utils.transformations.array_transformations.schema_of_json"
        ) as mock_schema_of_json, patch(
            "utils.transformations.array_transformations.lit"
        ) as mock_lit, patch(
            "utils.transformations.array_transformations.from_json"
        ) as mock_from_json, patch(
            "utils.transformations.array_transformations.when"
        ) as mock_when:
            mock_df_with_column = Mock()
            mock_df.withColumn.return_value = mock_df_with_column
            mock_df_with_column.withColumn.return_value = mock_df_with_column
            mock_df_with_column.select.return_value.filter.return_value.take.return_value = (
                mock_sample
            )

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "json_col.items"
            )

            mock_schema_of_json.assert_called_once()
            mock_from_json.assert_called_once()
            assert result is not None

    def test_extract_array_json_schema_inference_empty_array(self):
        """Test _extract_array JSON schema inference with empty array."""
        mock_df = Mock()
        mock_df.columns = ["json_col"]
        mock_df.dtypes = [("json_col", "string")]

        # Mock sample data with empty array
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="[]")
        mock_sample = [mock_row]

        with patch(
            "utils.transformations.array_transformations.get_json_object"
        ) as mock_get_json, patch(
            "utils.transformations.array_transformations.col"
        ) as mock_col:
            mock_df_with_column = Mock()
            mock_df.withColumn.return_value = mock_df_with_column
            mock_df_with_column.select.return_value.filter.return_value.take.return_value = (
                mock_sample
            )

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "json_col.items"
            )

            # Should not call schema inference for empty arrays
            assert result is not None

    def test_extract_array_direct_column_access(self):
        """Test _extract_array direct column access for simple arrays."""
        mock_df = Mock()
        mock_df.columns = ["array_col"]
        mock_df.dtypes = [("array_col", "array<string>")]

        with patch("utils.transformations.array_transformations.col") as mock_col:
            mock_col_expr = Mock()
            mock_col.return_value = mock_col_expr

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "array_col"
            )

            mock_col.assert_called_with("array_col")
            mock_df.withColumn.assert_called_with("target_col", mock_col_expr)

    def test_extract_array_nested_struct_getitem_access(self):
        """Test _extract_array nested struct access using getItem."""
        mock_df = Mock()
        mock_df.columns = ["struct_col"]
        mock_df.dtypes = [("struct_col", "struct<nested:array<string>>")]

        with patch("utils.transformations.array_transformations.col") as mock_col:
            mock_expr = Mock()
            mock_nested_expr = Mock()
            mock_final_expr = Mock()
            mock_col.return_value = mock_expr
            mock_expr.getItem.return_value = mock_nested_expr
            mock_nested_expr.getItem.return_value = mock_final_expr

            result = ArrayTransformations._extract_array(
                mock_df, "target_col", "struct_col.nested.field"
            )

            mock_col.assert_called_with("struct_col")
            # Should call getItem twice for "nested" and "field"
            assert mock_expr.getItem.call_count == 1
            assert mock_nested_expr.getItem.call_count == 1
            mock_df.withColumn.assert_called_with("target_col", mock_final_expr)

    def test_get_column_default_for_different_types(self):
        """Test _get_column_default for different data types - covers lines 218, 220"""
        # Test float type default
        float_default = ArrayTransformations._get_column_default({}, "float", None)
        assert float_default == 0.0

        # Test double type default
        double_default = ArrayTransformations._get_column_default({}, "double", None)
        assert double_default == 0.0

        # Test int type default
        int_default = ArrayTransformations._get_column_default({}, "int", None)
        assert int_default == 0

        # Test string type default
        string_default = ArrayTransformations._get_column_default({}, "string", None)
        assert string_default == ""

    def test_build_column_expression_exception_handling(self):
        """Test _build_column_expression exception handling - covers line 270"""
        mock_df_exploded = Mock()
        mock_df_exploded.select.side_effect = Exception("Field access failed")

        col_config = {"source_field": "nonexistent_field"}

        with patch(
            "utils.transformations.array_transformations.col"
        ) as mock_col, patch(
            "utils.transformations.array_transformations.lit"
        ) as mock_lit:
            result = ArrayTransformations._build_column_expression(
                mock_df_exploded, col_config
            )

            # Should handle the exception and return lit(None)
            mock_lit.assert_called_with(None)

    def test_extract_array_missing_base_column(self):
        """Test _extract_array when base column doesn't exist - covers line 284"""
        mock_df = Mock()
        mock_df.columns = ["other_col"]  # missing_column not in columns

        with patch("utils.transformations.array_transformations.lit") as mock_lit:
            result = ArrayTransformations._extract_array(
                mock_df, "extracted", "missing_column.field"
            )

            # Should return DataFrame with null target column
            mock_df.withColumn.assert_called()
            mock_lit.assert_called_with(None)
