# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for job_utils module."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from utils.job_utils import (
    convert_namespace_to_dict,
    extract_error_message,
    finalize_job_failure,
    finalize_job_success,
    get_curation_targets,
    get_source_name,
)


class TestJobUtils:
    """Test cases for job utility functions."""

    def test_convert_namespace_to_dict_simple(self):
        """Test converting SimpleNamespace to dict."""
        ns = SimpleNamespace()
        ns.name = "test"
        ns.value = 123

        result = convert_namespace_to_dict(ns)

        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["value"] == 123

    def test_convert_namespace_to_dict_nested(self):
        """Test converting nested SimpleNamespace to dict."""
        inner_ns = SimpleNamespace()
        inner_ns.inner_value = "inner"

        outer_ns = SimpleNamespace()
        outer_ns.outer_value = "outer"
        outer_ns.nested = inner_ns

        result = convert_namespace_to_dict(outer_ns)

        assert isinstance(result, dict)
        assert result["outer_value"] == "outer"
        assert isinstance(result["nested"], dict)
        assert result["nested"]["inner_value"] == "inner"

    def test_convert_namespace_to_dict_regular_dict(self):
        """Test that regular dicts are processed correctly."""
        regular_dict = {"key": "value", "number": 42}

        result = convert_namespace_to_dict(regular_dict)

        assert result == regular_dict

    def test_extract_error_message_single_line(self):
        """Test extracting error message from single line exception."""
        exception = Exception("Simple error message")

        result = extract_error_message(exception)

        assert result == "Simple error message"

    def test_extract_error_message_multiline(self):
        """Test extracting error message from multiline exception."""
        exception = Exception("First line error\nSecond line details\nThird line")

        result = extract_error_message(exception)

        assert result == "First line error"

    def test_get_source_name_file_types(self):
        """Test getting source name for file-based sources."""
        cfg = SimpleNamespace()
        cfg.source_type = "xlsx"
        cfg.source = "test_file.xlsx"
        cfg.source_table_name = "table_name"

        result = get_source_name(cfg)

        assert result == "test_file.xlsx"

    def test_get_source_name_table_types(self):
        """Test getting source name for table-based sources."""
        cfg = SimpleNamespace()
        cfg.source_type = "dynamodb"
        cfg.source = "test_file.xlsx"
        cfg.source_table_name = "table_name"

        result = get_source_name(cfg)

        assert result == "table_name"

    def test_finalize_job_success_with_execution_time(self):
        """Test finalizing successful job with execution time."""
        mock_audit_manager = Mock()
        args = {"stepfunction_execution_id": "exec_123", "job_type": "stage"}
        source_name = "test_source"
        output_path = "s3://bucket/path"
        row_count = 100
        job_execution_time = "2024-01-01 12:00:00"

        finalize_job_success(
            mock_audit_manager,
            args,
            source_name,
            output_path,
            row_count,
            job_execution_time,
        )

        mock_audit_manager.end_stage_job.assert_called_once_with(
            stepfunction_execution_id="exec_123",
            source_table_name="test_source",
            job_type="stage",
            status="Completed",
            data_path="s3://bucket/path",
            no_of_rows_processed=100,
            udm_processed_date="2024-01-01 12:00:00",
        )

    def test_finalize_job_success_without_execution_time(self):
        """Test finalizing successful job without execution time."""
        mock_audit_manager = Mock()
        args = {"stepfunction_execution_id": "exec_123", "job_type": "stage"}
        source_name = "test_source"
        output_path = "s3://bucket/path"
        row_count = 100

        finalize_job_success(
            mock_audit_manager, args, source_name, output_path, row_count
        )

        mock_audit_manager.end_stage_job.assert_called_once_with(
            stepfunction_execution_id="exec_123",
            source_table_name="test_source",
            job_type="stage",
            status="Completed",
            data_path="s3://bucket/path",
            no_of_rows_processed=100,
        )

    def test_finalize_job_failure(self):
        """Test finalizing failed job execution."""
        mock_audit_manager = Mock()
        args = {"stepfunction_execution_id": "exec_123", "job_type": "stage"}
        source_name = "test_source"
        exception = Exception("Test error message")

        finalize_job_failure(mock_audit_manager, args, source_name, exception)

        mock_audit_manager.end_stage_job.assert_called_once_with(
            "exec_123",
            source_table_name="test_source",
            job_type="stage",
            status="Failed",
            failure_reason="Test error message",
        )

    def test_get_curation_targets_valid_list(self):
        """Test getting curation targets from valid config."""
        cfg = SimpleNamespace()
        cfg.curation = [
            {"name": "target1", "table": "table1"},
            {"name": "target2", "table": "table2"},
        ]

        result = get_curation_targets(cfg)

        assert len(result) == 2
        assert result[0]["name"] == "target1"
        assert result[1]["name"] == "target2"

    def test_get_curation_targets_invalid_config(self):
        """Test getting curation targets from invalid config."""
        cfg = SimpleNamespace()
        cfg.curation = {"name": "single_target"}  # Not a list

        with pytest.raises(
            ValueError, match="Config error: 'curation' must be an array"
        ):
            get_curation_targets(cfg)

    def test_get_curation_targets_missing_config(self):
        """Test getting curation targets when config is missing."""
        cfg = SimpleNamespace()
        # No curation attribute

        with pytest.raises(
            ValueError, match="Config error: 'curation' must be an array"
        ):
            get_curation_targets(cfg)
