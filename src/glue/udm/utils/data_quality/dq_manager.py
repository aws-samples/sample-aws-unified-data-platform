# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Data Quality Manager - Main orchestrator for DQ operations.."""

import logging
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from .dqdl_executor import DQDLExecutor
from .dqdl_rule_builder import DQDLRuleBuilder
from .exception_handler import DQExceptionHandler


class DataQualityManager:
    """Main manager for data quality operations."""

    def __init__(self, glue_context, data_writer=None, logger=None):
        """Initialize Data Quality Manager."""
        self.glue_context = glue_context
        self.data_writer = data_writer
        self.logger = logger or logging.getLogger(__name__)
        self.executor = DQDLExecutor(glue_context, self.logger)
        self.exception_handler = DQExceptionHandler(self.logger)

    def apply_data_quality_checks(
        self,
        dataframe: DataFrame,
        dq_config: dict,
        source_name: str,
        target_name: str,
        execution_id: str,
        table_location: str = None,
    ) -> Tuple[DataFrame, dict]:
        """
        Apply data quality checks to DataFrame.

        Args:
            dataframe: Input DataFrame
            dq_config: Data quality configuration
            source_name: Source table name
            target_name: Target table name
            execution_id: Execution ID

        Returns:
            Tuple of (validated_dataframe, metrics_dict)
        """
        print(f"INFO: Starting data quality checks for {target_name}")

        try:
            # Build DQDL rules from config
            rules = DQDLRuleBuilder.build_rules_from_config(dq_config)

            if not rules:
                print(f"INFO: No data quality rules configured for {target_name}")
                return dataframe, {}

            print(f"INFO: Evaluating {len(rules)} data quality rules")

            # Execute rules
            passed_df, failed_df, metrics = self.executor.evaluate_rules(
                dataframe, rules, f"{source_name}_{target_name}"
            )

            # Handle failed records
            if failed_df is not None and failed_df.count() > 0:
                self._handle_failed_records(
                    failed_df,
                    dq_config,
                    source_name,
                    target_name,
                    execution_id,
                    metrics,
                    table_location,
                )

            # Check error threshold
            max_threshold = dq_config.get("exception_handling", {}).get(
                "max_error_threshold", 0.05
            )
            (
                threshold_exceeded,
                threshold_msg,
            ) = self.exception_handler.check_error_threshold(metrics, max_threshold)

            print(f"INFO: {threshold_msg}")

            if threshold_exceeded:
                print(f"WARNING: Data quality threshold exceeded for {target_name}")
                # Optionally fail the job based on config
                if dq_config.get("exception_handling", {}).get(
                    "fail_on_threshold", False
                ):
                    raise Exception(threshold_msg)

            # Return passed records only
            return passed_df if passed_df is not None else dataframe, metrics

        except Exception as e:
            print(f"ERROR: Error in data quality checks: {e}")
            # Decide whether to fail or continue based on config
            if dq_config.get("fail_on_error", True):
                raise
            return dataframe, {}

    def _handle_failed_records(
        self,
        failed_df: DataFrame,
        dq_config: dict,
        source_name: str,
        target_name: str,
        execution_id: str,
        metrics: dict,
        table_location: str = None,
    ) -> None:
        """Handle failed records - write to Iceberg exception table."""
        exception_config = dq_config.get("exception_handling", {})

        # Write failed records to Iceberg using same database as target
        database_name = exception_config.get("exception_database")

        if database_name and table_location and self.data_writer:
            try:
                # Construct S3 location same as curated job pattern
                exception_s3_location = (
                    f"{table_location.rstrip('/')}/{target_name}_exception/"
                )
                self.exception_handler.write_failed_records_to_iceberg(
                    failed_df,
                    database_name,
                    target_name,
                    execution_id,
                    self.data_writer,
                    exception_s3_location,
                )
            except Exception as e:
                print(f"ERROR: Failed to write exception records to Iceberg: {e}")

    def get_dq_config_for_target(
        self, config: dict, target_name: str
    ) -> Optional[dict]:
        """
        Extract data quality config for specific target.

        Args:
            config: Full configuration object
            target_name: Target table name

        Returns:
            Data quality config dict or None
        """
        # Check if there's a global data_quality config
        global_dq = getattr(config, "data_quality", None)

        # Check if target has specific DQ config
        curation_targets = getattr(config, "curation", [])
        for target in curation_targets:
            if target.get("name") == target_name:
                target_dq = target.get("data_quality")
                if target_dq:
                    return target_dq

        # Return global config if exists
        if global_dq:
            return vars(global_dq) if hasattr(global_dq, "__dict__") else global_dq

        return None
