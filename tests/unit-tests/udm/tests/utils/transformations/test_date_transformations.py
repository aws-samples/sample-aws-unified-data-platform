# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for date_transformations module."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.transformations.date_transformations import DateTransformations


class TestDateTransformations:
    """Test cases for DateTransformations class."""

    def test_clean_date_strings_default_mapping(self):
        """Test cleaning date strings with default mapping."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.regexp_replace"
            ) as mock_regexp:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.lower"
                ) as mock_lower:
                    mock_col.return_value = "col_expr"
                    mock_lower.return_value = "lower_expr"
                    mock_regexp.side_effect = [
                        "cleaned1",
                        "cleaned2",
                        "cleaned3",
                        "final_cleaned",
                    ]
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.clean_date_strings(mock_df, "date_col")

                    assert result == mock_result_df
                    mock_col.assert_called_once_with("date_col")
                    mock_lower.assert_called_once_with("col_expr")
                    assert mock_regexp.call_count == 4

    def test_clean_date_strings_custom_mapping(self):
        """Test cleaning date strings with custom mapping."""
        mock_df = Mock()
        mock_result_df = Mock()
        custom_mapping = {"invalid": ""}

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.regexp_replace"
            ) as mock_regexp:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.lower"
                ) as mock_lower:
                    mock_col.return_value = "col_expr"
                    mock_lower.return_value = "lower_expr"
                    mock_regexp.side_effect = ["cleaned1", "final_cleaned"]
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.clean_date_strings(
                        mock_df, "date_col", custom_mapping
                    )

                    assert result == mock_result_df
                    assert mock_regexp.call_count == 2

    def test_convert_date_format_with_input_and_target(self):
        """Test converting date format with both input and target formats."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_date"
            ) as mock_to_date:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.date_format"
                ) as mock_date_format:
                    mock_col.return_value = "col_expr"
                    mock_to_date.return_value = "date_expr"
                    mock_date_format.return_value = "formatted_expr"
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.convert_date_format(
                        mock_df, "date_col", "yyyy-MM-dd", "dd/MM/yyyy"
                    )

                    assert result == mock_result_df
                    mock_to_date.assert_called_once_with("col_expr", "yyyy-MM-dd")
                    mock_date_format.assert_called_once_with("date_expr", "dd/MM/yyyy")

    def test_convert_date_format_target_only(self):
        """Test converting date format with target format only."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_date"
            ) as mock_to_date:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.date_format"
                ) as mock_date_format:
                    mock_col.return_value = "col_expr"
                    mock_to_date.return_value = "date_expr"
                    mock_date_format.return_value = "formatted_expr"
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.convert_date_format(
                        mock_df, "date_col", target_format="dd/MM/yyyy"
                    )

                    assert result == mock_result_df
                    mock_to_date.assert_called_once_with("col_expr")
                    mock_date_format.assert_called_once_with("date_expr", "dd/MM/yyyy")

    def test_convert_date_format_no_formats(self):
        """Test converting date format with no formats specified."""
        mock_df = Mock()

        result = DateTransformations.convert_date_format(mock_df, "date_col")

        assert result == mock_df

    def test_convert_timestamp_format_with_input_and_target(self):
        """Test converting timestamp format with both input and target formats."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_timestamp"
            ) as mock_to_timestamp:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.date_format"
                ) as mock_date_format:
                    mock_col.return_value = "col_expr"
                    mock_to_timestamp.return_value = "timestamp_expr"
                    mock_date_format.return_value = "formatted_expr"
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.convert_timestamp_format(
                        mock_df, "ts_col", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm"
                    )

                    assert result == mock_result_df
                    mock_to_timestamp.assert_called_once_with(
                        "col_expr", "yyyy-MM-dd HH:mm:ss"
                    )
                    mock_date_format.assert_called_once_with(
                        "timestamp_expr", "dd/MM/yyyy HH:mm"
                    )

    def test_convert_timestamp_format_target_only(self):
        """Test converting timestamp format with target format only."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_timestamp"
            ) as mock_to_timestamp:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.date_format"
                ) as mock_date_format:
                    mock_col.return_value = "col_expr"
                    mock_to_timestamp.return_value = "timestamp_expr"
                    mock_date_format.return_value = "formatted_expr"
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.convert_timestamp_format(
                        mock_df, "ts_col", target_format="dd/MM/yyyy HH:mm"
                    )

                    assert result == mock_result_df
                    mock_to_timestamp.assert_called_once_with("col_expr")
                    mock_date_format.assert_called_once_with(
                        "timestamp_expr", "dd/MM/yyyy HH:mm"
                    )

    def test_convert_timestamp_format_no_formats(self):
        """Test converting timestamp format with no formats specified."""
        mock_df = Mock()

        result = DateTransformations.convert_timestamp_format(mock_df, "ts_col")

        assert result == mock_df

    def test_standardize_dates_first_day_month(self):
        """Test standardizing dates to first day of month."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_date"
            ) as mock_to_date:
                with patch(
                    "src.glue.udm.utils.transformations.date_transformations.date_format"
                ) as mock_date_format:
                    mock_col.return_value = "col_expr"
                    mock_to_date.return_value = "date_expr"
                    mock_date_format.return_value = "formatted_expr"
                    mock_df.withColumn.return_value = mock_result_df

                    result = DateTransformations.standardize_dates(
                        mock_df, "date_col", "first_day_month"
                    )

                    assert result == mock_result_df
                    mock_to_date.assert_called_once_with("col_expr")
                    mock_date_format.assert_called_once_with("date_expr", "yyyy-MM-01")

    def test_standardize_dates_default_format(self):
        """Test standardizing dates with default format."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.date_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.date_transformations.to_date"
            ) as mock_to_date:
                mock_col.return_value = "col_expr"
                mock_to_date.return_value = "date_expr"
                mock_df.withColumn.return_value = mock_result_df

                result = DateTransformations.standardize_dates(
                    mock_df, "date_col", "other_format"
                )

                assert result == mock_result_df
                mock_to_date.assert_called_once_with("col_expr")
