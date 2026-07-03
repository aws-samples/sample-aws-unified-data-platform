# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Incremental reader module for tracking last successful job execution times."""

from typing import Optional

import boto3


class IncrementalReader:
    """Class for reading incremental job execution data from audit table."""

    def __init__(self, audit_table_name: str):
        """Initialize IncrementalReader with audit table.

        Args:
            audit_table_name: Name of the DynamoDB audit table
        """
        self.audit_table_name = audit_table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(audit_table_name)

    def get_last_successful_execution_start_time(
        self,
        source_table_name: str,
        job_name: str,
        job_type: str,
        stepfunction_execution_id: str = None,
    ) -> Optional[str]:
        """Get the last successful execution start time for a specific table and job.

        Args:
            source_table_name: Name of the source table
            job_name: Name of the job
            job_type: Type of job ('stage' or 'curation')
            stepfunction_execution_id: Current step function execution ID (required for curation)  # pylint: disable=line-too-long

        Returns:
            Last successful execution start time in ISO format, or None if not found
        """
        if job_type == "curation":
            # For curation job: get current step function's stage job start time
            if not stepfunction_execution_id:
                raise ValueError(
                    "stepfunction_execution_id is required for curation job"
                )

            response = self.table.query(
                KeyConditionExpression="stepfunction_execution_id = :exec_id AND table_name = :table_name",  # pylint: disable=line-too-long
                ExpressionAttributeValues={
                    ":exec_id": stepfunction_execution_id,
                    ":table_name": source_table_name,
                },
            )

            if response["Items"]:
                current_item = response["Items"][0]
                stage_data = current_item.get("stages", {}).get("stage", {})
                return stage_data.get("execution_start_time")
            return None
        else:
            # For stage job: get the latest successful step function's stage job start time # pylint: disable=line-too-long
            response = self.table.query(
                IndexName="table_name-status-index",  # Note: ensure this index exists
                KeyConditionExpression="table_name = :table_name AND stepfunction_exec_status = :status",  # pylint: disable=line-too-long
                ExpressionAttributeValues={
                    ":table_name": source_table_name,
                    ":status": "Completed",
                },
                ScanIndexForward=False,
                Limit=1,
            )

            if response["Items"]:
                latest_item = response["Items"][0]
                stage_data = latest_item.get("stages", {}).get("stage", {})
                return stage_data.get("execution_start_time")
            return None

    def validate_table_exists(self) -> bool:
        """Validate that the audit table exists."""
        try:
            self.table.load()
            return True
        except Exception:  # pylint: disable=broad-except
            return False
