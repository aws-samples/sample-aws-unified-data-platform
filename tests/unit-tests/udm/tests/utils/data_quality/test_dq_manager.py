# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DataQualityManager."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from utils.data_quality.dq_manager import DataQualityManager


class TestDataQualityManager:
    """Test cases for DataQualityManager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.mock_data_writer = Mock()
        self.mock_logger = Mock()

        with patch(
            "utils.data_quality.dq_manager.DQDLExecutor"
        ) as mock_executor_class, patch(
            "utils.data_quality.dq_manager.DQExceptionHandler"
        ) as mock_handler_class:
            self.mock_executor = Mock()
            self.mock_exception_handler = Mock()
            mock_executor_class.return_value = self.mock_executor
            mock_handler_class.return_value = self.mock_exception_handler

            self.dq_manager = DataQualityManager(
                self.mock_glue_context, self.mock_data_writer, self.mock_logger
            )

    def test_init(self):
        """Test DataQualityManager initialization."""
        assert self.dq_manager.glue_context == self.mock_glue_context
        assert self.dq_manager.data_writer == self.mock_data_writer
        assert self.dq_manager.logger == self.mock_logger

    @patch("utils.data_quality.dq_manager.DQDLRuleBuilder")
    def test_apply_data_quality_checks_no_rules(self, mock_rule_builder):
        """Test apply_data_quality_checks with no rules."""
        mock_rule_builder.build_rules_from_config.return_value = []
        mock_df = Mock()

        result_df, metrics = self.dq_manager.apply_data_quality_checks(
            mock_df, {}, "source", "target", "exec_id"
        )

        assert result_df == mock_df
        assert not metrics

    @patch("utils.data_quality.dq_manager.DQDLRuleBuilder")
    def test_apply_data_quality_checks_with_rules(self, mock_rule_builder):
        """Test apply_data_quality_checks with rules."""
        mock_rule_builder.build_rules_from_config.return_value = ["rule1"]
        mock_df = Mock()

        # Mock executor
        self.mock_executor.evaluate_rules.return_value = (
            mock_df,
            None,
            {"total_rows": 100, "failed_rows": 0},
        )

        # Mock exception handler
        self.mock_exception_handler.check_error_threshold.return_value = (
            False,
            "Within threshold",
        )

        result_df, metrics = self.dq_manager.apply_data_quality_checks(
            mock_df, {"enabled": True}, "source", "target", "exec_id"
        )

        assert result_df == mock_df
        assert metrics == {"total_rows": 100, "failed_rows": 0}

    @patch("utils.data_quality.dq_manager.DQDLRuleBuilder")
    def test_apply_data_quality_checks_with_failed_records(self, mock_rule_builder):
        """Test apply_data_quality_checks with failed records."""
        mock_rule_builder.build_rules_from_config.return_value = ["rule1"]
        mock_df = Mock()
        mock_failed_df = Mock()
        mock_failed_df.count.return_value = 5

        # Mock executor
        self.mock_executor.evaluate_rules.return_value = (
            mock_df,
            mock_failed_df,
            {"total_rows": 100, "failed_rows": 5},
        )

        # Mock exception handler
        self.mock_exception_handler.check_error_threshold.return_value = (
            False,
            "Within threshold",
        )

        dq_config = {
            "enabled": True,
            "exception_handling": {"exception_database": "test_db"},
        }

        with patch.object(self.dq_manager, "_handle_failed_records") as mock_handle:
            result_df, metrics = self.dq_manager.apply_data_quality_checks(
                mock_df, dq_config, "source", "target", "exec_id", "s3://bucket/path"
            )

            mock_handle.assert_called_once()

    @patch("utils.data_quality.dq_manager.DQDLRuleBuilder")
    def test_apply_data_quality_checks_threshold_exceeded(self, mock_rule_builder):
        """Test apply_data_quality_checks when threshold exceeded."""
        mock_rule_builder.build_rules_from_config.return_value = ["rule1"]
        mock_df = Mock()

        # Mock executor
        self.mock_executor.evaluate_rules.return_value = (
            mock_df,
            None,
            {"total_rows": 100, "failed_rows": 10},
        )

        # Mock exception handler - threshold exceeded
        self.mock_exception_handler.check_error_threshold.return_value = (
            True,
            "Threshold exceeded",
        )

        dq_config = {"enabled": True, "exception_handling": {"fail_on_threshold": True}}

        with pytest.raises(Exception, match="Threshold exceeded"):
            self.dq_manager.apply_data_quality_checks(
                mock_df, dq_config, "source", "target", "exec_id"
            )

    @patch("utils.data_quality.dq_manager.DQDLRuleBuilder")
    def test_apply_data_quality_checks_exception_handling(self, mock_rule_builder):
        """Test apply_data_quality_checks exception handling."""
        mock_rule_builder.build_rules_from_config.side_effect = Exception("Test error")
        mock_df = Mock()

        # Test with fail_on_error=True (default)
        with pytest.raises(Exception, match="Test error"):
            self.dq_manager.apply_data_quality_checks(
                mock_df, {"enabled": True}, "source", "target", "exec_id"
            )

        # Test with fail_on_error=False
        result_df, metrics = self.dq_manager.apply_data_quality_checks(
            mock_df,
            {"enabled": True, "fail_on_error": False},
            "source",
            "target",
            "exec_id",
        )

        assert result_df == mock_df
        assert not metrics

    def test_handle_failed_records(self):
        """Test _handle_failed_records method."""
        mock_failed_df = Mock()
        dq_config = {"exception_handling": {"exception_database": "test_db"}}

        self.dq_manager._handle_failed_records(
            mock_failed_df,
            dq_config,
            "source",
            "target",
            "exec_id",
            {"failed_rows": 5},
            "s3://bucket/path",
        )

        self.mock_exception_handler.write_failed_records_to_iceberg.assert_called_once()

    def test_handle_failed_records_exception(self):
        """Test _handle_failed_records with exception."""
        mock_failed_df = Mock()
        dq_config = {"exception_handling": {"exception_database": "test_db"}}

        self.mock_exception_handler.write_failed_records_to_iceberg.side_effect = (
            Exception("Write error")
        )

        # Should not raise exception, just log error
        self.dq_manager._handle_failed_records(
            mock_failed_df,
            dq_config,
            "source",
            "target",
            "exec_id",
            {"failed_rows": 5},
            "s3://bucket/path",
        )

    def test_get_dq_config_for_target_global_config(self):
        """Test get_dq_config_for_target with global config."""
        config = SimpleNamespace()
        config.data_quality = {"enabled": True, "rules": {}}
        config.curation = []

        result = self.dq_manager.get_dq_config_for_target(config, "test_target")
        assert result == {"enabled": True, "rules": {}}

    def test_get_dq_config_for_target_target_specific(self):
        """Test get_dq_config_for_target with target-specific config."""
        config = SimpleNamespace()
        config.data_quality = None
        config.curation = [
            {"name": "test_target", "data_quality": {"enabled": True}},
            {"name": "other_target", "data_quality": {"enabled": False}},
        ]

        result = self.dq_manager.get_dq_config_for_target(config, "test_target")
        assert result == {"enabled": True}

    def test_get_dq_config_for_target_no_config(self):
        """Test get_dq_config_for_target with no config."""
        config = SimpleNamespace()
        config.data_quality = None
        config.curation = []

        result = self.dq_manager.get_dq_config_for_target(config, "test_target")
        assert result is None

    def test_get_dq_config_for_target_global_with_dict(self):
        """Test get_dq_config_for_target with global config as dict."""
        config = SimpleNamespace()
        config.data_quality = {"enabled": True, "rules": {}}
        config.curation = []

        result = self.dq_manager.get_dq_config_for_target(config, "test_target")
        assert result == {"enabled": True, "rules": {}}
