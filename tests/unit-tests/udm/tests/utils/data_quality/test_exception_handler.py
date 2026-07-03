# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DQExceptionHandler."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from utils.data_quality.exception_handler import DQExceptionHandler


class TestDQExceptionHandler:
    """Test cases for DQExceptionHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_logger = Mock()

        with patch("utils.data_quality.exception_handler.boto3"):
            self.handler = DQExceptionHandler(self.mock_logger)

    def test_init(self):
        """Test DQExceptionHandler initialization."""
        assert self.handler.logger == self.mock_logger

    def test_init_without_logger(self):
        """Test DQExceptionHandler initialization without logger."""
        with patch("utils.data_quality.exception_handler.boto3"):
            handler = DQExceptionHandler()
            assert handler.logger is not None

    @patch("utils.data_quality.exception_handler.current_timestamp")
    @patch("utils.data_quality.exception_handler.lit")
    def test_write_failed_records_to_iceberg_success(self, mock_lit, mock_timestamp):
        """Test write_failed_records_to_iceberg success case."""
        # Mock DataFrame
        mock_failed_df = Mock()
        mock_failed_df.count.return_value = 5

        # Mock DataFrame operations
        mock_df_with_timestamp = Mock()
        mock_df_with_execution_id = Mock()
        mock_failed_df.withColumn.side_effect = [
            mock_df_with_timestamp,
            mock_df_with_execution_id,
        ]

        # Mock data writer
        mock_data_writer = Mock()
        mock_table_path = "s3://bucket/path/table"
        mock_data_writer.write_to_iceberg_table.return_value = mock_table_path

        result = self.handler.write_failed_records_to_iceberg(
            mock_failed_df,
            "test_db",
            "test_table",
            "exec_123",
            mock_data_writer,
            "s3://bucket/path/",
        )

        assert result == mock_table_path
        mock_data_writer.write_to_iceberg_table.assert_called_once()

    def test_write_failed_records_to_iceberg_no_records(self):
        """Test write_failed_records_to_iceberg with no records."""
        mock_failed_df = Mock()
        mock_failed_df.count.return_value = 0

        result = self.handler.write_failed_records_to_iceberg(
            mock_failed_df,
            "test_db",
            "test_table",
            "exec_123",
            Mock(),
            "s3://bucket/path/",
        )

        assert result is None

    def test_write_failed_records_to_iceberg_none_df(self):
        """Test write_failed_records_to_iceberg with None DataFrame."""
        result = self.handler.write_failed_records_to_iceberg(
            None, "test_db", "test_table", "exec_123", Mock(), "s3://bucket/path/"
        )

        assert result is None

    @patch("utils.data_quality.exception_handler.current_timestamp")
    @patch("utils.data_quality.exception_handler.lit")
    def test_write_failed_records_to_iceberg_exception(self, mock_lit, mock_timestamp):
        """Test write_failed_records_to_iceberg with exception."""
        # Mock DataFrame
        mock_failed_df = Mock()
        mock_failed_df.count.return_value = 5

        # Mock DataFrame operations
        mock_df_with_timestamp = Mock()
        mock_df_with_execution_id = Mock()
        mock_failed_df.withColumn.side_effect = [
            mock_df_with_timestamp,
            mock_df_with_execution_id,
        ]

        # Mock data writer with exception
        mock_data_writer = Mock()
        mock_data_writer.write_to_iceberg_table.side_effect = Exception("Write error")

        with pytest.raises(Exception, match="Write error"):
            self.handler.write_failed_records_to_iceberg(
                mock_failed_df,
                "test_db",
                "test_table",
                "exec_123",
                mock_data_writer,
                "s3://bucket/path/",
            )

    def test_check_error_threshold_no_rows(self):
        """Test check_error_threshold with no rows."""
        metrics = {"total_rows": 0, "failed_rows": 0}

        exceeded, message = self.handler.check_error_threshold(metrics, 0.05)

        assert not exceeded
        assert message == "No rows to evaluate"

    def test_check_error_threshold_within_limit(self):
        """Test check_error_threshold within threshold."""
        metrics = {"total_rows": 100, "failed_rows": 3}

        exceeded, message = self.handler.check_error_threshold(metrics, 0.05)

        assert not exceeded
        assert "3.00%" in message
        assert "within threshold" in message

    def test_check_error_threshold_exceeded(self):
        """Test check_error_threshold when exceeded."""
        metrics = {"total_rows": 100, "failed_rows": 10}

        exceeded, message = self.handler.check_error_threshold(metrics, 0.05)

        assert exceeded
        assert "10.00%" in message
        assert "threshold exceeded" in message

    def test_check_error_threshold_default_threshold(self):
        """Test check_error_threshold with default threshold."""
        metrics = {"total_rows": 100, "failed_rows": 6}

        exceeded, message = self.handler.check_error_threshold(metrics)

        assert exceeded
        assert "6.00%" in message

    def test_check_error_threshold_missing_metrics(self):
        """Test check_error_threshold with missing metrics."""
        metrics = {}

        exceeded, message = self.handler.check_error_threshold(metrics, 0.05)

        assert not exceeded
        assert message == "No rows to evaluate"

    def test_check_error_threshold_edge_case(self):
        """Test check_error_threshold at exact threshold."""
        metrics = {"total_rows": 100, "failed_rows": 5}

        exceeded, message = self.handler.check_error_threshold(metrics, 0.05)

        assert not exceeded  # 5% is not > 5%
        assert "5.00%" in message
