# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Exception Handler - Manages failed records and exception tracking."""

import logging
from typing import Tuple

import boto3
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


class DQExceptionHandler:
    """Handles data quality exceptions and failed records."""

    def __init__(self, logger=None):
        """Initialize exception handler."""
        self.logger = logger or logging.getLogger(__name__)
        self.dynamodb = boto3.resource("dynamodb")

    def write_failed_records_to_iceberg(
        self,
        failed_df: DataFrame,
        database_name: str,
        target_table_name: str,
        execution_id: str,
        data_writer,
        s3_location: str,
    ) -> str:
        """
        Write failed records to Iceberg exception table.

        Args:
            failed_df: DataFrame with failed records
            database_name: Database name for exception table
            target_table_name: Curation target table name
            execution_id: Execution ID
            data_writer: DataWriter instance
            s3_location: S3 location for Iceberg table

        Returns:
            Iceberg table path where records were written
        """
        if failed_df is None or failed_df.count() == 0:
            self.logger.info("No failed records to write")
            return None

        try:
            # Add metadata columns
            failed_df = failed_df.withColumn("exception_timestamp", current_timestamp())
            failed_df = failed_df.withColumn("execution_id", lit(execution_id))

            # Exception table name based on curation target
            exception_table = f"{target_table_name}_exception"

            # Write to Iceberg table
            table_path = data_writer.write_to_iceberg_table(
                dataframe=failed_df,
                database_name=database_name,
                table_name=exception_table,
                s3_location=f"{s3_location.rstrip('/')}/{exception_table}/",
                write_mode="append",
                partition_column="exception_timestamp",
            )

            self.logger.info(
                "Written %s failed records to Iceberg table %s.%s",
                failed_df.count(), database_name, exception_table
            )
            return table_path

        except Exception as e:
            self.logger.error("Error writing failed records to Iceberg: %s", e)
            raise

    def check_error_threshold(
        self, metrics: dict, max_threshold: float = 0.05
    ) -> Tuple[bool, str]:
        """
        Check if error rate exceeds threshold.

        Args:
            metrics: DQ metrics dictionary
            max_threshold: Maximum allowed failure rate (default 5%)

        Returns:
            Tuple of (threshold_exceeded, message)
        """
        total_rows = metrics.get("total_rows", 0)
        failed_rows = metrics.get("failed_rows", 0)

        if total_rows == 0:
            return False, "No rows to evaluate"

        failure_rate = failed_rows / total_rows

        if failure_rate > max_threshold:
            message = (
                f"Error threshold exceeded: {failure_rate:.2%} "
                f"(threshold: {max_threshold:.2%})"
            )
            return True, message

        return False, f"Error rate within threshold: {failure_rate:.2%}"
