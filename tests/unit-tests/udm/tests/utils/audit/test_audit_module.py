# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for audit_module."""

import os
from datetime import datetime, timezone
from unittest.mock import patch

import boto3
import pytest
from moto import mock_dynamodb
from utils.audit.audit_module import AuditManager


class TestAuditManager:
    """Test cases for AuditManager class."""

    @pytest.fixture
    def audit_manager(self):
        """Create AuditManager instance with mocked DynamoDB."""
        with mock_dynamodb():
            # Set mock AWS credentials for moto (not real credentials)
            os.environ["AWS_ACCESS_KEY_ID"] = "testing"  # nosec B105
            os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # pragma: allowlist secret  # nosec B105
            os.environ["AWS_SECURITY_TOKEN"] = "testing"  # pragma: allowlist secret  # nosec B105
            os.environ["AWS_SESSION_TOKEN"] = "testing"  # pragma: allowlist secret  # nosec B105
            os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

            # Create table
            dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
            table = dynamodb.create_table(
                TableName="test-audit-table",
                KeySchema=[
                    {"AttributeName": "stepfunction_execution_id", "KeyType": "HASH"},
                    {"AttributeName": "table_name", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {
                        "AttributeName": "stepfunction_execution_id",
                        "AttributeType": "S",
                    },
                    {"AttributeName": "table_name", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            # Create audit manager
            manager = AuditManager("test-audit-table")
            yield manager, table

    def test_init(self, audit_manager):
        """Test AuditManager initialization."""
        manager, _ = audit_manager
        assert manager.table_name == "test-audit-table"
        assert manager.table is not None
        assert manager.dynamodb is not None
        assert manager.logger is not None

    def test_start_stage_job_basic(self, audit_manager):
        """Test starting a stage job with basic parameters."""
        manager, table = audit_manager

        # First create the item with basic structure
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},
            }
        )

        result = manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="stage",
            source_table_name="source_table",
            glue_table_name="glue_table",
        )

        # Verify return value is ISO timestamp
        assert isinstance(result, str)
        datetime.fromisoformat(result.replace("Z", "+00:00"))  # Should not raise

        # Verify item was created in DynamoDB
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        assert "Item" in response
        item = response["Item"]
        assert "stages" in item
        assert "stage" in item["stages"]

        stage_data = item["stages"]["stage"]
        assert stage_data["job_name"] == "test-job"
        assert stage_data["status"] == "Running"
        assert stage_data["glue_table_name"] == "glue_table"
        assert "execution_start_time" in stage_data

    def test_start_stage_job_with_curation_new(self, audit_manager):
        """Test starting a curation job when no existing curation exists."""
        manager, table = audit_manager

        # First create the item with basic structure
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},
            }
        )

        result = manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="curation",
            source_table_name="source_table",
            glue_table_name="glue_table",
            curation_name="test_curation",
        )

        # Verify return value
        assert isinstance(result, str)

        # Verify item was created in DynamoDB
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        assert "Item" in response
        item = response["Item"]
        assert "stages" in item
        assert "curation" in item["stages"]

        curation_data = item["stages"]["curation"]
        assert curation_data["job_name"] == "test-job"
        assert curation_data["status"] == "Running"
        assert len(curation_data["tables"]) == 1

        table_entry = curation_data["tables"][0]
        assert table_entry["glue_table_name"] == "glue_table"
        assert table_entry["curation_name"] == "test_curation"
        assert table_entry["status"] == "Running"

    def test_start_stage_job_curation_missing_name(self, audit_manager):
        """Test starting a curation job without curation_name raises error."""
        manager, _ = audit_manager

        with pytest.raises(ValueError, match="curation_name is required"):
            manager.start_stage_job(
                stepfunction_execution_id="exec-123",
                job_name="test-job",
                job_type="curation",
                source_table_name="source_table",
                glue_table_name="glue_table",
            )

    def test_end_stage_job_success(self, audit_manager):
        """Test ending a stage job successfully."""
        manager, table = audit_manager

        # First create a running stage job
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "stage": {
                        "job_name": "test-job",
                        "status": "Running",
                        "execution_start_time": "2024-01-01T10:00:00Z",
                    }
                },
            }
        )

        # End the job
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="stage",
            status="Completed",
            data_path="s3://bucket/output/",
            no_of_rows_processed=1000,
            udm_processed_date="2024-01-01 12:00:00",
        )

        # Verify the job was updated
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        stage_data = response["Item"]["stages"]["stage"]
        assert stage_data["status"] == "Completed"
        assert stage_data["data_path"] == "s3://bucket/output/"
        assert stage_data["no_of_rows_processed"] == 1000
        assert stage_data["udm_processed_date"] == "2024-01-01 12:00:00"
        assert "execution_end_time" in stage_data

    def test_end_stage_job_failure(self, audit_manager):
        """Test ending a stage job with failure."""
        manager, table = audit_manager

        # First create a running stage job
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {"stage": {"job_name": "test-job", "status": "Running"}},
            }
        )

        # End the job with failure
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="stage",
            status="Failed",
            failure_reason="Connection timeout",
        )

        # Verify the job was updated
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        stage_data = response["Item"]["stages"]["stage"]
        assert stage_data["status"] == "Failed"
        assert stage_data["failure_reason"] == "Connection timeout"
        assert "execution_end_time" in stage_data

    def test_end_stage_job_with_curation_single_table(self, audit_manager):
        """Test ending a curation job with single table."""
        manager, table = audit_manager

        # Create existing curation with one table
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "curation": {
                        "start_time": "2024-01-01T10:00:00Z",
                        "job_name": "test-job",
                        "status": "Running",
                        "tables": [
                            {
                                "glue_table_name": "test_table",
                                "curation_name": "test_curation",
                                "status": "Running",
                            }
                        ],
                    }
                },
            }
        )

        # End the curation job
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="curation",
            status="Completed",
            curation_name="test_curation",
            no_of_rows_processed=500,
        )

        # Verify the curation was updated
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["status"] == "Completed"  # Overall status updated
        assert "execution_end_time" in curation_data

        table_entry = curation_data["tables"][0]
        assert table_entry["status"] == "Completed"
        assert table_entry["no_of_rows_processed"] == 500
        assert "execution_end_time" in table_entry

    def test_end_stage_job_curation_missing_name(self, audit_manager):
        """Test ending a curation job without curation_name raises error."""
        manager, _ = audit_manager

        with pytest.raises(ValueError, match="curation_name is required"):
            manager.end_stage_job(
                stepfunction_execution_id="exec-123",
                source_table_name="source_table",
                job_type="curation",
                status="Completed",
            )

    def test_start_curation_job(self, audit_manager):
        """Test starting a curation job."""
        manager, table = audit_manager

        # First create the item with basic structure
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},
            }
        )

        result = manager.start_curation_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_name="curation-job",
        )

        # Verify return value
        assert isinstance(result, str)
        datetime.fromisoformat(result.replace("Z", "+00:00"))  # Should not raise

        # Verify item was created in DynamoDB
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        assert "Item" in response
        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["job_name"] == "curation-job"
        assert curation_data["status"] == "Running"
        assert curation_data["tables"] == []
        assert "execution_start_time" in curation_data

    def test_end_curation_job_success(self, audit_manager):
        """Test ending a curation job successfully."""
        manager, table = audit_manager

        # First create a running curation job
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "curation": {
                        "execution_start_time": "2024-01-01T10:00:00Z",
                        "job_name": "curation-job",
                        "status": "Running",
                        "tables": [],
                    }
                },
            }
        )

        # End the curation job
        manager.end_curation_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            status="Completed",
        )

        # Verify the job was updated
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["status"] == "Completed"
        assert "execution_end_time" in curation_data

    def test_end_curation_job_failure(self, audit_manager):
        """Test ending a curation job with failure."""
        manager, table = audit_manager

        # First create a running curation job
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "curation": {
                        "execution_start_time": "2024-01-01T10:00:00Z",
                        "job_name": "curation-job",
                        "status": "Running",
                        "tables": [],
                    }
                },
            }
        )

        # End the curation job with failure
        manager.end_curation_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            status="Failed",
        )

        # Verify the job was updated
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["status"] == "Failed"
        assert "execution_end_time" in curation_data

    @patch("utils.audit.audit_module.datetime")
    def test_datetime_consistency(self, mock_datetime, audit_manager):
        """Test that datetime is used consistently across methods."""
        manager, table = audit_manager

        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.isoformat = fixed_time.isoformat

        # First create the item with basic structure
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},
            }
        )

        # Start a job
        result = manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="stage",
            source_table_name="source_table",
            glue_table_name="glue_table",
        )

        # Verify the returned time matches our fixed time
        assert result == fixed_time.isoformat()

        # End the job
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="stage",
            status="Completed",
        )

        # Verify both start and end times are set correctly
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        stage_data = response["Item"]["stages"]["stage"]
        assert stage_data["execution_start_time"] == fixed_time.isoformat()
        assert stage_data["execution_end_time"] == fixed_time.isoformat()

    def test_exception_handling_get_item(self, audit_manager):
        """Test exception handling in get_item operations."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Mock the table to raise an exception on get_item
        with patch.object(
            manager.table, "get_item", side_effect=Exception("DynamoDB error")
        ):
            # This should not raise an exception due to broad except clause
            manager.start_stage_job(
                stepfunction_execution_id="exec-123",
                job_name="test-job",
                job_type="curation",
                source_table_name="source_table",
                glue_table_name="glue_table",
                curation_name="test_curation",
            )

            # Verify the job was still created (empty curation structure used)
            response = table.get_item(
                Key={
                    "stepfunction_execution_id": "exec-123",
                    "table_name": "source_table",
                }
            )

            assert "Item" in response
            curation_data = response["Item"]["stages"]["curation"]
            assert len(curation_data["tables"]) == 1

    def test_logging_functionality(self, audit_manager):
        """Test that logging works correctly."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Create a stage job and end it to trigger logging
        manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="stage",
            source_table_name="source_table",
            glue_table_name="glue_table",
        )

        # Mock the logger to verify it's called
        with patch.object(manager.logger, "info") as mock_log:
            manager.end_stage_job(
                stepfunction_execution_id="exec-123",
                source_table_name="source_table",
                job_type="stage",
                status="Completed",
            )

            # Verify logging was called
            mock_log.assert_called_once()
            call_args = mock_log.call_args[0][0]
            assert "stage" in call_args
            assert "source_table" in call_args
            assert "Completed" in call_args

    def test_end_stage_job_with_curation_multiple_tables(self, audit_manager):
        """Test ending a curation job with multiple tables."""
        manager, table = audit_manager

        # Create existing curation with multiple tables
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "curation": {
                        "start_time": "2024-01-01T10:00:00Z",
                        "job_name": "test-job",
                        "status": "Running",
                        "tables": [
                            {
                                "glue_table_name": "table1",
                                "curation_name": "curation1",
                                "status": "Completed",
                            },
                            {
                                "glue_table_name": "table2",
                                "curation_name": "curation2",
                                "status": "Running",
                            },
                        ],
                    }
                },
            }
        )

        # End one of the curation jobs
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="curation",
            status="Completed",
            curation_name="curation2",
        )

        # Verify the specific table was updated and overall status is completed
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["status"] == "Completed"  # All tables completed

        # Find the updated table
        updated_table = next(
            t for t in curation_data["tables"] if t["curation_name"] == "curation2"
        )
        assert updated_table["status"] == "Completed"

    def test_end_stage_job_with_curation_failure_mixed(self, audit_manager):
        """Test ending a curation job with mixed success/failure."""
        manager, table = audit_manager

        # Create existing curation with multiple tables
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {
                    "curation": {
                        "start_time": "2024-01-01T10:00:00Z",
                        "job_name": "test-job",
                        "status": "Running",
                        "tables": [
                            {
                                "glue_table_name": "table1",
                                "curation_name": "curation1",
                                "status": "Completed",
                            },
                            {
                                "glue_table_name": "table2",
                                "curation_name": "curation2",
                                "status": "Running",
                            },
                        ],
                    }
                },
            }
        )

        # End one job with failure
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="curation",
            status="Failed",
            curation_name="curation2",
            failure_reason="Processing error",
        )

        # Verify overall status is Failed due to one failure
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        curation_data = response["Item"]["stages"]["curation"]
        assert curation_data["status"] == "Failed"  # Has failures

    def test_stage_job_get_item_exception(self, audit_manager):
        """Test stage job creation when get_item raises exception."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Mock get_item to raise exception for stage job
        with patch.object(
            manager.table, "get_item", side_effect=Exception("DynamoDB error")
        ):
            manager.start_stage_job(
                stepfunction_execution_id="exec-123",
                job_name="test-job",
                job_type="stage",
                source_table_name="source_table",
                glue_table_name="glue_table",
            )

        # Verify job was created despite exception
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )
        assert "Item" in response
        assert "stages" in response["Item"]
        assert "stage" in response["Item"]["stages"]

    def test_end_stage_job_get_item_exception(self, audit_manager):
        """Test end_stage_job when get_item raises exception."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Create initial job
        manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="stage",
            source_table_name="source_table",
            glue_table_name="glue_table",
        )

        # Mock get_item to raise exception during end_stage_job
        with patch.object(
            manager.table, "get_item", side_effect=Exception("DynamoDB error")
        ):
            manager.end_stage_job(
                stepfunction_execution_id="exec-123",
                source_table_name="source_table",
                job_type="stage",
                status="Completed",
            )

    def test_end_curation_job_get_item_exception(self, audit_manager):
        """Test end_stage_job for curation when get_item raises exception."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Create initial curation job
        manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="curation",
            source_table_name="source_table",
            glue_table_name="glue_table",
            curation_name="test_curation",
        )

        # Mock get_item to raise exception during end_stage_job for curation
        with patch.object(
            manager.table, "get_item", side_effect=Exception("DynamoDB error")
        ):
            manager.end_stage_job(
                stepfunction_execution_id="exec-123",
                source_table_name="source_table",
                job_type="curation",
                status="Completed",
                curation_name="test_curation",
            )

    def test_end_stage_job_curation_with_udm_processed_date(self, audit_manager):
        """Test end_stage_job for curation with udm_processed_date parameter."""
        manager, table = audit_manager

        # First create a basic item structure that the update can work with
        table.put_item(
            Item={
                "stepfunction_execution_id": "exec-123",
                "table_name": "source_table",
                "stages": {},  # Initialize empty stages structure
            }
        )

        # Start curation job
        manager.start_stage_job(
            stepfunction_execution_id="exec-123",
            job_name="test-job",
            job_type="curation",
            source_table_name="source_table",
            glue_table_name="glue_table",
            curation_name="test_curation",
        )

        # End with udm_processed_date
        test_date = "2023-08-01"
        manager.end_stage_job(
            stepfunction_execution_id="exec-123",
            source_table_name="source_table",
            job_type="curation",
            status="Completed",
            curation_name="test_curation",
            udm_processed_date=test_date,
        )

        # Verify udm_processed_date was set
        response = table.get_item(
            Key={"stepfunction_execution_id": "exec-123", "table_name": "source_table"}
        )

        assert "Item" in response
        curation_data = response["Item"]["stages"]["curation"]
        table_entry = curation_data["tables"][0]
        assert table_entry["udm_processed_date"] == test_date
