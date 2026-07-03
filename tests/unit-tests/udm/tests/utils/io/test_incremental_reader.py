# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for incremental_reader module."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from src.glue.udm.utils.io.incremental_reader import IncrementalReader


class TestIncrementalReader:
    """Test cases for IncrementalReader class."""

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_init(self, mock_boto3):
        """Test IncrementalReader initialization."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        reader = IncrementalReader("test-audit-table")

        assert reader.audit_table_name == "test-audit-table"
        assert reader.dynamodb == mock_dynamodb
        assert reader.table == mock_table
        mock_boto3.resource.assert_called_once_with("dynamodb")
        mock_dynamodb.Table.assert_called_once_with("test-audit-table")

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_found_stage(self, mock_boto3):
        """Test getting last successful execution time from stage job."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock response with successful stage job
        mock_response = {
            "Items": [
                {
                    "table_name": "test_table",
                    "stages": {
                        "stage": {
                            "job_name": "test_job",
                            "status": "Completed",
                            "execution_start_time": "2023-01-01T10:00:00Z",
                        }
                    },
                }
            ]
        }
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result == "2023-01-01T10:00:00Z"
        mock_table.query.assert_called_once()

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_found_curated(self, mock_boto3):
        """Test getting last successful execution time from curated job."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock response with successful curated job
        mock_response = {
            "Items": [
                {
                    "table_name": "test_table",
                    "stages": {
                        "stage": {
                            "job_name": "test_job",
                            "status": "Completed",
                            "execution_start_time": "2023-01-01T12:00:00Z",
                        }
                    },
                }
            ]
        }
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "curation", "test-execution-id"
        )

        assert result == "2023-01-01T12:00:00Z"

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_multiple_items_latest(
        self, mock_boto3
    ):
        """Test getting latest execution time when multiple items exist."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock response with multiple successful jobs
        mock_response = {
            "Items": [
                {
                    "table_name": "test_table",
                    "stages": {
                        "stage": {
                            "job_name": "test_job",
                            "status": "Completed",
                            "execution_start_time": "2023-01-01T15:00:00Z",  # Latest
                        }
                    },
                }
            ]
        }
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result == "2023-01-01T15:00:00Z"

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_no_items(self, mock_boto3):
        """Test getting execution time when no items found."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock empty response
        mock_response = {"Items": []}
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result is None

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_no_matching_job(self, mock_boto3):
        """Test getting execution time when no matching job found."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock empty response (no items found with the query conditions)
        mock_response = {"Items": []}
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result is None

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_no_completed_status(
        self, mock_boto3
    ):
        """Test getting execution time when no completed status found."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock empty response (no items found with Completed status in the query)
        mock_response = {"Items": []}
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result is None

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_no_execution_start_time(
        self, mock_boto3
    ):
        """Test getting execution time when execution_start_time is missing."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock response without execution_start_time
        mock_response = {
            "Items": [
                {
                    "table_name": "test_table",
                    "stages": {
                        "stage": {
                            "job_name": "test_job",
                            "status": "Completed",
                            # Missing execution_start_time
                        }
                    },
                }
            ]
        }
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result is None

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_get_last_successful_execution_start_time_missing_stages(self, mock_boto3):
        """Test getting execution time when stages are missing."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock response without stages
        mock_response = {
            "Items": [
                {
                    "table_name": "test_table"
                    # Missing stages
                }
            ]
        }
        mock_table.query.return_value = mock_response

        reader = IncrementalReader("test-audit-table")
        result = reader.get_last_successful_execution_start_time(
            "test_table", "test_job", "stage"
        )

        assert result is None

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_validate_table_exists_success(self, mock_boto3):
        """Test table validation when table exists."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock successful table load
        mock_table.load.return_value = None

        reader = IncrementalReader("test-audit-table")
        result = reader.validate_table_exists()

        assert result is True
        mock_table.load.assert_called_once()

    @patch("src.glue.udm.utils.io.incremental_reader.boto3")
    def test_validate_table_exists_failure(self, mock_boto3):
        """Test table validation when table doesn't exist."""
        mock_dynamodb = Mock()
        mock_table = Mock()
        mock_boto3.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        # Mock table load exception
        mock_table.load.side_effect = Exception("Table not found")

        reader = IncrementalReader("test-audit-table")
        result = reader.validate_table_exists()

        assert result is False
        mock_table.load.assert_called_once()
