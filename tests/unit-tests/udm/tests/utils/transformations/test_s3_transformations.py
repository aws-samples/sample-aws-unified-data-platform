# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for s3_transformations module."""

from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.transformations.s3_transformations import S3Transformations


class TestS3Transformations:
    """Test cases for S3Transformations class."""

    def test_extract_from_s3_prefix_valid_column(self):
        """Test extracting from S3 prefix with valid source column."""
        mock_df = Mock()
        mock_df.columns = ["s3_path", "other_col"]
        mock_result_df = Mock()
        mock_path_parts = Mock()
        mock_filename_parts = Mock()

        with patch(
            "src.glue.udm.utils.transformations.s3_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.s3_transformations.regexp_replace"
            ) as mock_regexp:
                with patch(
                    "src.glue.udm.utils.transformations.s3_transformations.split"
                ) as mock_split:
                    with patch(
                        "src.glue.udm.utils.transformations.s3_transformations.size"
                    ) as mock_size:
                        mock_col.return_value = "col_expr"
                        mock_regexp.return_value = "cleaned_path"
                        mock_split.side_effect = [mock_path_parts, mock_filename_parts]
                        mock_path_parts.getItem.return_value = "extracted_value"
                        mock_filename_parts.getItem.return_value = "final_value"
                        mock_df.withColumn.return_value = mock_result_df

                        result = S3Transformations.extract_from_s3_prefix(
                            mock_df, "target_col", "s3_path", 3
                        )

                        assert result == mock_result_df
                        mock_col.assert_called_once_with("s3_path")
                        mock_regexp.assert_called_once_with("col_expr", "^s3://", "")
                        assert mock_split.call_count == 2
                        mock_split.assert_any_call("cleaned_path", "/")
                        mock_split.assert_any_call("extracted_value", "\\.")
                        mock_path_parts.getItem.assert_called_once_with(
                            2
                        )  # position 3 -> index 2
                        mock_filename_parts.getItem.assert_called_once_with(0)
                        mock_df.withColumn.assert_called_once_with(
                            "target_col", "final_value"
                        )

    def test_extract_from_s3_prefix_missing_column(self):
        """Test extracting from S3 prefix when source column doesn't exist."""
        mock_df = Mock()
        mock_df.columns = ["other_col"]  # s3_path missing
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.s3_transformations.lit"
        ) as mock_lit:
            mock_lit.return_value = "lit_none"
            mock_df.withColumn.return_value = mock_result_df

            result = S3Transformations.extract_from_s3_prefix(
                mock_df, "target_col", "s3_path", 3
            )

            assert result == mock_result_df
            mock_lit.assert_called_once_with(None)
            mock_df.withColumn.assert_called_once_with("target_col", "lit_none")

    def test_extract_from_s3_prefix_position_1(self):
        """Test extracting from S3 prefix with position 1."""
        mock_df = Mock()
        mock_df.columns = ["s3_path"]
        mock_result_df = Mock()
        mock_path_parts = Mock()
        mock_filename_parts = Mock()

        with patch(
            "src.glue.udm.utils.transformations.s3_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.s3_transformations.regexp_replace"
            ) as mock_regexp:
                with patch(
                    "src.glue.udm.utils.transformations.s3_transformations.split"
                ) as mock_split:
                    with patch(
                        "src.glue.udm.utils.transformations.s3_transformations.size"
                    ) as mock_size:
                        mock_col.return_value = "col_expr"
                        mock_regexp.return_value = "cleaned_path"
                        mock_split.side_effect = [mock_path_parts, mock_filename_parts]
                        mock_path_parts.getItem.return_value = "bucket_name"
                        mock_filename_parts.getItem.return_value = "final_bucket_name"
                        mock_df.withColumn.return_value = mock_result_df

                        result = S3Transformations.extract_from_s3_prefix(
                            mock_df, "bucket", "s3_path", 1
                        )

                        assert result == mock_result_df
                        mock_path_parts.getItem.assert_called_once_with(
                            0
                        )  # position 1 -> index 0
                        mock_filename_parts.getItem.assert_called_once_with(0)

    def test_extract_from_s3_prefix_high_position(self):
        """Test extracting from S3 prefix with high position number."""
        mock_df = Mock()
        mock_df.columns = ["s3_path"]
        mock_result_df = Mock()
        mock_path_parts = Mock()
        mock_filename_parts = Mock()

        with patch(
            "src.glue.udm.utils.transformations.s3_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.s3_transformations.regexp_replace"
            ) as mock_regexp:
                with patch(
                    "src.glue.udm.utils.transformations.s3_transformations.split"
                ) as mock_split:
                    with patch(
                        "src.glue.udm.utils.transformations.s3_transformations.size"
                    ) as mock_size:
                        mock_col.return_value = "col_expr"
                        mock_regexp.return_value = "cleaned_path"
                        mock_split.side_effect = [mock_path_parts, mock_filename_parts]
                        mock_path_parts.getItem.return_value = "deep_folder"
                        mock_filename_parts.getItem.return_value = "final_deep_folder"
                        mock_df.withColumn.return_value = mock_result_df

                        result = S3Transformations.extract_from_s3_prefix(
                            mock_df, "deep_value", "s3_path", 10
                        )

                        assert result == mock_result_df
                        mock_path_parts.getItem.assert_called_once_with(
                            9
                        )  # position 10 -> index 9
                        mock_filename_parts.getItem.assert_called_once_with(0)
