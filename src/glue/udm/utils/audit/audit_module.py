# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Audit management module for tracking job execution status.."""

import logging
from datetime import datetime, timezone

import boto3


class AuditManager:
    """Class for managing audit records in DynamoDB."""

    def __init__(self, table_name: str):
        """Initialize AuditManager with DynamoDB table.

        Args:
            table_name: Name of the DynamoDB audit table
        """
        self.table_name = table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)
        self.logger = logging.getLogger(__name__)

    def start_stage_job(self, stepfunction_execution_id: str, **kwargs) -> str:
        """Start audit tracking for a stage job.

        Args:
            stepfunction_execution_id: Step function execution ID
            job_name: Name of the Glue job
            job_type: Type of job (stage/curated)
            source_table_name: Source table name
            glue_table_name: Target Glue table name
            curation_name: Name of curation table (for curated jobs)

        Returns:
            Execution start time in ISO format
        """
        job_name = kwargs["job_name"]
        job_type = kwargs["job_type"]
        source_table_name = kwargs["source_table_name"]
        glue_table_name = kwargs["glue_table_name"]
        curation_name = kwargs.get("curation_name")

        execution_start_time = datetime.now(timezone.utc).isoformat()

        job_data = {
            "job_name": job_name,
            "execution_start_time": execution_start_time,
            "status": "Running",
            "glue_table_name": glue_table_name,
        }

        if job_type == "stage":
            update_expression = "SET stages.#job_type = :job_data"
            expression_attribute_names = {"#job_type": job_type}
            expression_attribute_values = {":job_data": job_data}
        else:  # curated job
            if not curation_name:
                raise ValueError("curation_name is required for curated jobs")

            # Initialize or get existing curation structure
            try:
                response = self.table.get_item(
                    Key={
                        "stepfunction_execution_id": stepfunction_execution_id,
                        "table_name": source_table_name,
                    }
                )
                current_curation = (
                    response.get("Item", {}).get("stages", {}).get("curation", {})
                )
            except Exception:  # pylint: disable=broad-except
                current_curation = {}

            # Initialize curation structure if empty
            if not current_curation:
                current_curation = {
                    "start_time": execution_start_time,
                    "job_name": job_name,
                    "status": "Running",
                    "tables": [],
                }

            # Add table entry
            table_entry = {
                "glue_table_name": glue_table_name,
                "status": "Running",
                "curation_name": curation_name,
                "execution_start_time": kwargs.get(
                    "execution_start_time", execution_start_time
                ),
            }
            current_curation["tables"].append(table_entry)

            update_expression = "SET stages.curation = :curation_data"
            expression_attribute_names = None
            expression_attribute_values = {":curation_data": current_curation}

        # Build update_item parameters
        update_params = {
            "Key": {
                "stepfunction_execution_id": stepfunction_execution_id,
                "table_name": source_table_name,
            },
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
        }

        if expression_attribute_names:
            update_params["ExpressionAttributeNames"] = expression_attribute_names

        self.table.update_item(**update_params)

        return execution_start_time

    def end_stage_job(self, stepfunction_execution_id: str, **kwargs) -> None:
        """End audit tracking for a stage job.

        Args:
            stepfunction_execution_id: Step function execution ID
            source_table_name: Source table name
            job_type: Type of job (stage/curated)
            status: Job completion status
            failure_reason: Reason for failure if applicable
            no_of_rows_processed: Number of rows processed
            data_path: Output data path
            udm_processed_date: Processing date
            curation_name: Name of curation table (for curated jobs)
            no_of_rows_failed_dq_check: Number of rows that failed DQ checks (optional)
        """
        source_table_name = kwargs["source_table_name"]
        job_type = kwargs["job_type"]
        status = kwargs["status"]
        failure_reason = kwargs.get("failure_reason", "")
        no_of_rows_processed = kwargs.get("no_of_rows_processed", 0)
        data_path = kwargs.get("data_path", "")
        udm_processed_date = kwargs.get("udm_processed_date")
        curation_name = kwargs.get("curation_name")
        no_of_rows_failed_dq_check = kwargs.get("no_of_rows_failed_dq_check", 0)

        execution_end_time = datetime.now(timezone.utc).isoformat()

        if job_type == "stage":
            # Handle stage job
            try:
                response = self.table.get_item(
                    Key={
                        "stepfunction_execution_id": stepfunction_execution_id,
                        "table_name": source_table_name,
                    }
                )
                current_job_data = (
                    response.get("Item", {}).get("stages", {}).get(job_type, {})
                )
            except Exception:  # pylint: disable=broad-except
                current_job_data = {}

            # Update job data
            current_job_data.update(
                {
                    "execution_end_time": execution_end_time,
                    "status": status,
                    "failure_reason": failure_reason,
                    "no_of_rows_processed": no_of_rows_processed,
                    "data_path": data_path,
                }
            )

            if udm_processed_date:
                current_job_data["udm_processed_date"] = udm_processed_date

            self.table.update_item(
                Key={
                    "stepfunction_execution_id": stepfunction_execution_id,
                    "table_name": source_table_name,
                },
                UpdateExpression="SET stages.#job_type = :job_data",
                ExpressionAttributeNames={"#job_type": job_type},
                ExpressionAttributeValues={":job_data": current_job_data},
            )
        else:  # curated job
            if not curation_name:
                raise ValueError("curation_name is required for curated jobs")

            # Get current curation structure and update specific table entry
            try:
                response = self.table.get_item(
                    Key={
                        "stepfunction_execution_id": stepfunction_execution_id,
                        "table_name": source_table_name,
                    }
                )
                current_curation = (
                    response.get("Item", {}).get("stages", {}).get("curation", {})
                )
            except Exception:  # pylint: disable=broad-except
                current_curation = {}

            # Update overall curation status
            current_curation["execution_end_time"] = execution_end_time

            # Find and update the specific table entry
            for table_entry in current_curation.get("tables", []):
                if table_entry.get("curation_name") == curation_name:
                    table_entry.update(
                        {
                            "status": status,
                            "failure_reason": failure_reason,
                            "no_of_rows_processed": no_of_rows_processed,
                            "execution_end_time": execution_end_time,
                        }
                    )
                    if udm_processed_date:
                        table_entry["udm_processed_date"] = udm_processed_date
                    # Add DQ metrics if provided
                    if no_of_rows_failed_dq_check > 0:
                        table_entry["no_of_rows_failed_dq_check"] = (
                            no_of_rows_failed_dq_check
                        )
                    break

            # Check if all tables are completed to update overall status
            all_completed = all(
                table.get("status") in ["Completed", "Failed"]
                for table in current_curation.get("tables", [])
            )
            if all_completed:
                has_failures = any(
                    table.get("status") == "Failed"
                    for table in current_curation.get("tables", [])
                )
                current_curation["status"] = "Failed" if has_failures else "Completed"

            self.table.update_item(
                Key={
                    "stepfunction_execution_id": stepfunction_execution_id,
                    "table_name": source_table_name,
                },
                UpdateExpression="SET stages.curation = :curation_data",
                ExpressionAttributeValues={":curation_data": current_curation},
            )

        self.logger.info(
            "Job %s for table %s ended with status: %s",
            job_type, source_table_name, status
        )

    def start_curation_job(
        self, stepfunction_execution_id: str, source_table_name: str, job_name: str
    ) -> str:
        """Initialize curation job audit entry.

        Args:
            stepfunction_execution_id: Step function execution ID
            source_table_name: Source table name
            job_name: Name of the job

        Returns:
            Execution start time in ISO format
        """
        execution_start_time = datetime.now(timezone.utc).isoformat()

        self.table.update_item(
            Key={
                "stepfunction_execution_id": stepfunction_execution_id,
                "table_name": source_table_name,
            },
            UpdateExpression="SET stages.curation = :curation_data",
            ExpressionAttributeValues={
                ":curation_data": {
                    "execution_start_time": execution_start_time,
                    "job_name": job_name,
                    "status": "Running",
                    "tables": [],
                }
            },
        )

        return execution_start_time

    def end_curation_job(
        self, stepfunction_execution_id: str, source_table_name: str, status: str
    ) -> None:
        """Update curation job completion status.

        Args:
            stepfunction_execution_id: Step function execution ID
            source_table_name: Source table name
            status: Job completion status (Completed/Failed)
        """
        execution_end_time = datetime.now(timezone.utc).isoformat()

        self.table.update_item(
            Key={
                "stepfunction_execution_id": stepfunction_execution_id,
                "table_name": source_table_name,
            },
            UpdateExpression=(
                "SET stages.curation.execution_end_time = :end_time, "
                "stages.curation.#status = :status"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":end_time": execution_end_time,
                ":status": status,
            },
        )
