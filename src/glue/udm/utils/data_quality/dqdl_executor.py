# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""DQDL Executor - Executes data quality rules using AWS Glue DQDL."""

import logging
from functools import reduce
from typing import Tuple

from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DQDLExecutor:
    """Executes DQDL rules on DataFrames."""

    def __init__(self, glue_context, logger=None):
        """Initialize DQDL executor."""
        self.glue_context = glue_context
        self.logger = logger or logging.getLogger(__name__)

    def evaluate_rules(
        self, dataframe: DataFrame, rules: list, ruleset_name: str = "default_ruleset"
    ) -> Tuple[DataFrame, DataFrame, dict]:
        """
        Evaluate DQDL rules on DataFrame.

        Args:
            dataframe: Input DataFrame
            rules: List of DQDL rule strings
            ruleset_name: Name for the ruleset

        Returns:
            Tuple of (passed_df, failed_df, metrics_dict)
        """
        if not rules:
            print("INFO: No data quality rules to evaluate")
            return dataframe, None, {}

        try:
            # Convert DataFrame to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(
                dataframe, self.glue_context, "dq_input_frame"
            )

            # Build ruleset string
            ruleset = "Rules = [" + ", ".join(rules) + "]"
            print(f"INFO: Evaluating ruleset: {ruleset}")
            print(f"INFO: Individual rules: {rules}")

            # Evaluate data quality rules using process_rows method
            dq_result = EvaluateDataQuality().process_rows(
                frame=dynamic_frame,
                ruleset=ruleset,
                publishing_options={
                    "dataQualityEvaluationContext": ruleset_name,
                    "enableDataQualityCloudWatchMetrics": True,
                    "enableDataQualityResultsPublishing": True,
                },
                additional_options={
                    "performanceTuning.caching": "CACHE_NOTHING",
                    "observations.scope": "ALL",
                },
            )

            # Get row-level outcomes with DataQualityEvaluationResult column
            row_level_outcomes = SelectFromCollection.apply(
                dfc=dq_result, key="rowLevelOutcomes"
            )

            # Convert to DataFrame
            result_df = row_level_outcomes.toDF()

            # Split data based on DataQualityEvaluationResult column
            passed_df = result_df.filter(
                result_df.DataQualityEvaluationResult == "Passed"
            )
            failed_df = result_df.filter(
                result_df.DataQualityEvaluationResult == "Failed"
            )

            # Remove DQ columns from passed_df
            dq_columns = [
                "DataQualityRulesPass",
                "DataQualityRulesFail",
                "DataQualityRulesSkip",
                "DataQualityEvaluationResult",
            ]
            for col_name in dq_columns:
                if col_name in passed_df.columns:
                    passed_df = passed_df.drop(col_name)

            # Calculate metrics
            total_count = result_df.count()
            passed_count = passed_df.count()
            failed_count = failed_df.count()

            # Set failed_df to None if no failures
            if failed_count == 0:
                failed_df = None

            metrics = {
                "total_rows": total_count,
                "passed_rows": passed_count,
                "failed_rows": failed_count,
                "total_rules": len(rules),
                "passed_rules": len(rules) if failed_count == 0 else 0,
                "failed_rules": len(rules) if failed_count > 0 else 0,
            }

            print(
                f"INFO: Row-level DQ filtering - Passed: {passed_count}, Failed: {failed_count}"  # pylint: disable=line-too-long
            )

            print(f"INFO: DQ Evaluation complete: {metrics}")

            return passed_df, failed_df, metrics

        except Exception as e:
            print(f"ERROR: Error evaluating data quality rules: {e}")
            raise

    def _extract_metrics(self, dq_result: dict) -> dict:
        """Extract metrics from DQ result."""
        metrics = {
            "total_rules": 0,
            "passed_rules": 0,
            "failed_rules": 0,
            "total_rows": 0,
            "passed_rows": 0,
            "failed_rows": 0,
        }

        try:
            # Extract rule-level metrics
            if "ruleResults" in dq_result:
                for rule_result in dq_result["ruleResults"]:
                    metrics["total_rules"] += 1
                    if rule_result.get("result") == "PASS":
                        metrics["passed_rules"] += 1
                    else:
                        metrics["failed_rules"] += 1

            # Extract row-level metrics
            if "rowLevelOutcomes" in dq_result:
                outcomes = dq_result["rowLevelOutcomes"]
                metrics["total_rows"] = outcomes.count()
                metrics["passed_rows"] = outcomes.filter(
                    lambda x: x["DataQualityEvaluationResult"] == "Passed"
                ).count()
                metrics["failed_rows"] = outcomes.filter(
                    lambda x: x["DataQualityEvaluationResult"] == "Failed"
                ).count()

        except Exception as e:
            print(f"WARNING: Could not extract all metrics: {e}")

        return metrics

    def _filter_rows_by_rules(
        self, dataframe: DataFrame, rules: list, failed_rules: list
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Filter DataFrame rows based on failed DQ rules.

        Args:
            dataframe: Input DataFrame
            rules: List of all DQDL rules
            failed_rules: List of failed rule names

        Returns:
            Tuple of (passed_df, failed_df)
        """
        try:
            # Convert DQDL rules to DataFrame filter conditions
            failed_conditions = []

            for rule in rules:
                # Extract rule name and condition from DQDL rule
                # Example: 'ColumnValues "status" in ["active", "inactive"]'
                if any(failed_rule in rule for failed_rule in failed_rules):
                    condition = self._convert_dqdl_to_filter(rule)
                    if condition is not None:
                        # Negate the condition to get failed records
                        failed_conditions.append(~condition)

            if failed_conditions:
                # Combine all failed conditions with OR

                combined_failed_condition = reduce(
                    lambda a, b: a | b, failed_conditions
                )

                # Split DataFrame
                failed_df = dataframe.filter(combined_failed_condition)
                passed_df = dataframe.filter(~combined_failed_condition)

                return passed_df, failed_df
            else:
                return dataframe, None

        except Exception as e:
            print(
                f"WARNING: Could not filter rows by rules, returning all as passed: {e}"
            )
            return dataframe, None

    def _convert_dqdl_to_filter(self, dqdl_rule: str):
        """
        Convert DQDL rule to Spark DataFrame filter condition.

        Args:
            dqdl_rule: DQDL rule string

        Returns:
            Spark Column condition or None
        """
        try:
            # Handle ColumnValues rules (most common)
            if "ColumnValues" in dqdl_rule and " in " in dqdl_rule:
                # Extract column name and values
                # Example: 'ColumnValues "status" in ["active", "inactive"]'
                parts = dqdl_rule.split('"')
                if len(parts) >= 3:
                    column_name = parts[1]
                    # Extract values from the 'in' clause
                    in_part = dqdl_rule.split(" in ")[1]
                    # Parse the list of values
                    if "[" in in_part and "]" in in_part:
                        values_str = in_part.split("[")[1].split("]")[0]
                        values = [
                            v.strip().strip('"').strip("'")
                            for v in values_str.split(",")
                        ]
                        return col(column_name).isin(values)

            # Handle other rule types as needed
            # For now, return None for unsupported rules
            return None

        except Exception as e:
            print(
                f"WARNING: Could not convert DQDL rule to filter: {dqdl_rule}, Error: {e}"  # pylint: disable=line-too-long
            )
            return None
