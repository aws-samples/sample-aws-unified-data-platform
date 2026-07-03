# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DQDLExecutor."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from utils.data_quality.dqdl_executor import DQDLExecutor


class TestDQDLExecutor:
    """Test cases for DQDLExecutor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.mock_logger = Mock()
        self.executor = DQDLExecutor(self.mock_glue_context, self.mock_logger)

    def test_init(self):
        """Test DQDLExecutor initialization."""
        assert self.executor.glue_context == self.mock_glue_context
        assert self.executor.logger == self.mock_logger

    def test_init_without_logger(self):
        """Test DQDLExecutor initialization without logger."""
        executor = DQDLExecutor(self.mock_glue_context)
        assert executor.glue_context == self.mock_glue_context
        assert executor.logger is not None

    def test_evaluate_rules_no_rules(self):
        """Test evaluate_rules with no rules."""
        mock_df = Mock()

        passed_df, failed_df, metrics = self.executor.evaluate_rules(
            mock_df, [], "test_ruleset"
        )

        assert passed_df == mock_df
        assert failed_df is None
        assert not metrics

    @patch("utils.data_quality.dqdl_executor.DynamicFrame")
    @patch("utils.data_quality.dqdl_executor.EvaluateDataQuality")
    @patch("utils.data_quality.dqdl_executor.SelectFromCollection")
    def test_evaluate_rules_with_rules_success(
        self, mock_select, mock_eval_dq, mock_dynamic_frame
    ):
        """Test evaluate_rules with rules - success case."""
        mock_df = Mock()
        rules = ['Completeness "name" >= 1.0']

        # Mock DynamicFrame creation
        mock_input_frame = Mock()
        mock_dynamic_frame.fromDF.return_value = mock_input_frame

        # Mock EvaluateDataQuality
        mock_dq_instance = Mock()
        mock_eval_dq.return_value = mock_dq_instance
        mock_dq_result = Mock()
        mock_dq_instance.process_rows.return_value = mock_dq_result

        # Mock SelectFromCollection
        mock_outcomes_frame = Mock()
        mock_select.apply.return_value = mock_outcomes_frame

        # Mock result DataFrame
        mock_result_df = Mock()
        mock_outcomes_frame.toDF.return_value = mock_result_df
        mock_result_df.count.return_value = 100

        # Mock filtered DataFrames
        mock_passed_df = Mock()
        mock_failed_df = Mock()
        mock_result_df.filter.side_effect = [mock_passed_df, mock_failed_df]
        mock_passed_df.count.return_value = 95
        mock_failed_df.count.return_value = 0

        # Mock column operations
        mock_passed_df.columns = ["id", "name", "DataQualityEvaluationResult"]
        mock_passed_df.drop.return_value = mock_passed_df

        passed_df, failed_df, metrics = self.executor.evaluate_rules(
            mock_df, rules, "test_ruleset"
        )

        assert passed_df == mock_passed_df
        assert failed_df is None  # Should be None when count is 0
        assert metrics["total_rows"] == 100
        assert metrics["passed_rows"] == 95
        assert metrics["failed_rows"] == 0

    @patch("utils.data_quality.dqdl_executor.DynamicFrame")
    @patch("utils.data_quality.dqdl_executor.EvaluateDataQuality")
    @patch("utils.data_quality.dqdl_executor.SelectFromCollection")
    def test_evaluate_rules_with_failures(
        self, mock_select, mock_eval_dq, mock_dynamic_frame
    ):
        """Test evaluate_rules with rules - failure case."""
        mock_df = Mock()
        rules = ['Completeness "name" >= 1.0']

        # Mock DynamicFrame creation
        mock_input_frame = Mock()
        mock_dynamic_frame.fromDF.return_value = mock_input_frame

        # Mock EvaluateDataQuality
        mock_dq_instance = Mock()
        mock_eval_dq.return_value = mock_dq_instance
        mock_dq_result = Mock()
        mock_dq_instance.process_rows.return_value = mock_dq_result

        # Mock SelectFromCollection
        mock_outcomes_frame = Mock()
        mock_select.apply.return_value = mock_outcomes_frame

        # Mock result DataFrame
        mock_result_df = Mock()
        mock_outcomes_frame.toDF.return_value = mock_result_df
        mock_result_df.count.return_value = 100

        # Mock filtered DataFrames
        mock_passed_df = Mock()
        mock_failed_df = Mock()
        mock_result_df.filter.side_effect = [mock_passed_df, mock_failed_df]
        mock_passed_df.count.return_value = 90
        mock_failed_df.count.return_value = 10

        # Mock column operations
        mock_passed_df.columns = ["id", "name", "DataQualityEvaluationResult"]
        mock_passed_df.drop.return_value = mock_passed_df

        passed_df, failed_df, metrics = self.executor.evaluate_rules(
            mock_df, rules, "test_ruleset"
        )

        assert passed_df == mock_passed_df
        assert failed_df == mock_failed_df
        assert metrics["total_rows"] == 100
        assert metrics["passed_rows"] == 90
        assert metrics["failed_rows"] == 10

    @patch("utils.data_quality.dqdl_executor.DynamicFrame")
    def test_evaluate_rules_exception(self, mock_dynamic_frame):
        """Test evaluate_rules with exception."""
        mock_df = Mock()
        rules = ['Completeness "name" >= 1.0']

        # Mock exception during DynamicFrame creation
        mock_dynamic_frame.fromDF.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            self.executor.evaluate_rules(mock_df, rules, "test_ruleset")

    def test_extract_metrics(self):
        """Test _extract_metrics method."""
        dq_result = {
            "ruleResults": [{"result": "PASS"}, {"result": "FAIL"}, {"result": "PASS"}],
            "rowLevelOutcomes": Mock(),
        }

        # Mock row level outcomes
        mock_outcomes = Mock()
        mock_outcomes.count.return_value = 100
        mock_outcomes.filter.side_effect = [
            Mock(count=lambda: 90),  # Passed
            Mock(count=lambda: 10),  # Failed
        ]
        dq_result["rowLevelOutcomes"] = mock_outcomes

        metrics = self.executor._extract_metrics(dq_result)

        assert metrics["total_rules"] == 3
        assert metrics["passed_rules"] == 2
        assert metrics["failed_rules"] == 1
        assert metrics["total_rows"] == 100
        assert metrics["passed_rows"] == 90
        assert metrics["failed_rows"] == 10

    def test_extract_metrics_exception(self):
        """Test _extract_metrics with exception."""
        dq_result = {"invalid": "data"}

        metrics = self.executor._extract_metrics(dq_result)

        # Should return default metrics on exception
        assert metrics["total_rules"] == 0
        assert metrics["passed_rules"] == 0
        assert metrics["failed_rules"] == 0

    def test_filter_rows_by_rules_no_conditions(self):
        """Test _filter_rows_by_rules with no failed conditions."""
        mock_df = Mock()
        rules = ['ColumnValues "status" in ["active"]']
        failed_rules = []

        passed_df, failed_df = self.executor._filter_rows_by_rules(
            mock_df, rules, failed_rules
        )

        assert passed_df == mock_df
        assert failed_df is None

    def test_filter_rows_by_rules_exception(self):
        """Test _filter_rows_by_rules with exception."""
        mock_df = Mock()
        rules = ["invalid rule"]
        failed_rules = ["test"]

        with patch.object(
            self.executor,
            "_convert_dqdl_to_filter",
            side_effect=Exception("Test error"),
        ):
            passed_df, failed_df = self.executor._filter_rows_by_rules(
                mock_df, rules, failed_rules
            )

            assert passed_df == mock_df
            assert failed_df is None

    @patch("utils.data_quality.dqdl_executor.col")
    def test_convert_dqdl_to_filter_column_values_in(self, mock_col):
        """Test _convert_dqdl_to_filter for ColumnValues in rule."""
        rule = 'ColumnValues "status" in ["active", "inactive"]'

        # Mock column operations
        mock_column = Mock()
        mock_col.return_value = mock_column
        mock_condition = Mock()
        mock_column.isin.return_value = mock_condition

        result = self.executor._convert_dqdl_to_filter(rule)

        mock_col.assert_called_with("status")
        mock_column.isin.assert_called_with(["active", "inactive"])
        assert result == mock_condition

    def test_convert_dqdl_to_filter_unsupported(self):
        """Test _convert_dqdl_to_filter for unsupported rule."""
        rule = 'Completeness "name" >= 1.0'

        result = self.executor._convert_dqdl_to_filter(rule)

        assert result is None

    def test_convert_dqdl_to_filter_exception(self):
        """Test _convert_dqdl_to_filter with exception."""
        rule = "Invalid rule format"

        result = self.executor._convert_dqdl_to_filter(rule)

        assert result is None
