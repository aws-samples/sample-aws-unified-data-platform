# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DQDLRuleBuilder."""

import pytest
from utils.data_quality.dqdl_rule_builder import DQDLRuleBuilder


class TestDQDLRuleBuilder:
    """Test cases for DQDLRuleBuilder."""

    def test_interpolate_template_simple(self):
        """Test _interpolate_template with simple parameters."""
        template = 'Completeness "{column}" >= {threshold}'
        params = {"column": "test_col", "threshold": 0.95}

        result = DQDLRuleBuilder._interpolate_template(template, params)
        assert result == 'Completeness "test_col" >= 0.95'

    def test_interpolate_template_with_list_valid_values(self):
        """Test _interpolate_template with valid_values list."""
        template = 'ColumnValues "{column}" in ["{valid_values}"]'
        params = {"column": "status", "valid_values": ["active", "inactive"]}

        result = DQDLRuleBuilder._interpolate_template(template, params)
        assert result == 'ColumnValues "status" in ["active", "inactive"]'

    def test_interpolate_template_with_list_columns(self):
        """Test _interpolate_template with columns list."""
        template = 'IsPrimaryKey "{columns}"'
        params = {"columns": ["id", "name"]}

        result = DQDLRuleBuilder._interpolate_template(template, params)
        assert result == 'IsPrimaryKey "id", "name"'

    def test_build_conditional_rule(self):
        """Test _build_conditional_rule method."""
        template = 'ColumnValues "{column}" >= {min_value}'
        params = {"column": "age", "min_value": 18, "max_value": 65}
        conditions = {
            "min_value_and_max_value": (
                'ColumnValues "{column}" >= {min_value} and '
                'ColumnValues "{column}" <= {max_value}'
            )
        }

        result = DQDLRuleBuilder._build_conditional_rule(template, params, conditions)
        assert result == 'ColumnValues "age" >= 18 and ColumnValues "age" <= 65'

    def test_build_rules_from_config_disabled(self):
        """Test build_rules_from_config with disabled config."""
        dq_config = {"enabled": False}

        result = DQDLRuleBuilder.build_rules_from_config(dq_config)
        assert not result

    def test_build_rules_from_config_empty(self):
        """Test build_rules_from_config with empty config."""
        result = DQDLRuleBuilder.build_rules_from_config({})
        assert not result

    def test_build_rules_from_config_with_custom_template(self):
        """Test build_rules_from_config with custom template."""
        dq_config = {
            "enabled": True,
            "rules": {
                "custom": [
                    {
                        "dqdl_template": 'CustomRule "{column}" > {value}',
                        "column": "score",
                        "value": 80,
                    }
                ]
            },
        }

        result = DQDLRuleBuilder.build_rules_from_config(dq_config)
        assert result == ['CustomRule "score" > 80']

    def test_build_rules_from_config_completeness(self):
        """Test build_rules_from_config with completeness rule."""
        dq_config = {
            "enabled": True,
            "rules": {"completeness": [{"column": "name", "threshold": 0.95}]},
        }

        result = DQDLRuleBuilder.build_rules_from_config(dq_config)
        assert result == ['Completeness "name" >= 0.95']

    def test_process_rule_type_completeness(self):
        """Test _process_rule_type for completeness."""
        rule_config = {"column": "email"}

        result = DQDLRuleBuilder._process_rule_type("completeness", rule_config)
        assert result == 'Completeness "email" >= 1.0'

    def test_process_rule_type_uniqueness_single(self):
        """Test _process_rule_type for uniqueness with single column."""
        rule_config = {"column": "id"}

        result = DQDLRuleBuilder._process_rule_type("uniqueness", rule_config)
        assert result == 'IsUnique "id"'

    def test_process_rule_type_uniqueness_multiple(self):
        """Test _process_rule_type for uniqueness with multiple columns."""
        rule_config = {"columns": ["id", "name"]}

        result = DQDLRuleBuilder._process_rule_type("uniqueness", rule_config)
        assert result == 'IsPrimaryKey "id", "name"'

    def test_process_rule_type_uniqueness_no_columns(self):
        """Test _process_rule_type for uniqueness with no columns."""
        rule_config = {}

        result = DQDLRuleBuilder._process_rule_type("uniqueness", rule_config)
        assert result is None

    def test_process_rule_type_validity(self):
        """Test _process_rule_type for validity."""
        rule_config = {"column": "status", "valid_values": ["active", "inactive"]}

        result = DQDLRuleBuilder._process_rule_type("validity", rule_config)
        assert result == 'ColumnValues "status" in ["active", "inactive"]'

    def test_process_rule_type_accuracy_both(self):
        """Test _process_rule_type for accuracy with min and max."""
        rule_config = {"column": "age", "min_value": 18, "max_value": 65}

        result = DQDLRuleBuilder._process_rule_type("accuracy", rule_config)
        assert result == 'ColumnValues "age" >= 18 and ColumnValues "age" <= 65'

    def test_process_rule_type_accuracy_min_only(self):
        """Test _process_rule_type for accuracy with min only."""
        rule_config = {"column": "age", "min_value": 18}

        result = DQDLRuleBuilder._process_rule_type("accuracy", rule_config)
        assert result == 'ColumnValues "age" >= 18'

    def test_process_rule_type_accuracy_max_only(self):
        """Test _process_rule_type for accuracy with max only."""
        rule_config = {"column": "age", "max_value": 65}

        result = DQDLRuleBuilder._process_rule_type("accuracy", rule_config)
        assert result == 'ColumnValues "age" <= 65'

    def test_process_rule_type_accuracy_no_column(self):
        """Test _process_rule_type for accuracy with no column."""
        rule_config = {"min_value": 18}

        result = DQDLRuleBuilder._process_rule_type("accuracy", rule_config)
        assert result is None

    def test_process_rule_type_row_count_both(self):
        """Test _process_rule_type for row_count with min and max."""
        rule_config = {"min_count": 100, "max_count": 1000}

        result = DQDLRuleBuilder._process_rule_type("row_count", rule_config)
        assert result == "RowCount >= 100 and RowCount <= 1000"

    def test_process_rule_type_row_count_min_only(self):
        """Test _process_rule_type for row_count with min only."""
        rule_config = {"min_count": 100}

        result = DQDLRuleBuilder._process_rule_type("row_count", rule_config)
        assert result == "RowCount >= 100"

    def test_process_rule_type_row_count_max_only(self):
        """Test _process_rule_type for row_count with max only."""
        rule_config = {"max_count": 1000}

        result = DQDLRuleBuilder._process_rule_type("row_count", rule_config)
        assert result == "RowCount <= 1000"

    def test_process_rule_type_row_count_none(self):
        """Test _process_rule_type for row_count with no values."""
        rule_config = {}

        result = DQDLRuleBuilder._process_rule_type("row_count", rule_config)
        assert result is None

    def test_process_rule_type_consistency(self):
        """Test _process_rule_type for consistency."""
        rule_config = {"condition": "col1 = col2"}

        result = DQDLRuleBuilder._process_rule_type("consistency", rule_config)
        assert result == 'CustomSql "consistency_check" "col1 = col2" >= 1.0'

    def test_process_rule_type_business_rules(self):
        """Test _process_rule_type for business_rules."""
        rule_config = {"name": "revenue_check", "sql": "revenue > 0", "threshold": 0.95}

        result = DQDLRuleBuilder._process_rule_type("business_rules", rule_config)
        assert result == 'CustomSql "revenue_check" "revenue > 0" >= 0.95'

    def test_process_rule_type_column_length(self):
        """Test _process_rule_type for column_length."""
        rule_config = {"column": "name", "length": 50}

        result = DQDLRuleBuilder._process_rule_type("column_length", rule_config)
        assert result == 'ColumnLength "name" <= 50'

    def test_process_rule_type_column_values(self):
        """Test _process_rule_type for column_values."""
        rule_config = {"column": "score", "operator": ">", "value": 80}

        result = DQDLRuleBuilder._process_rule_type("column_values", rule_config)
        assert result == 'ColumnValues "score" > 80'

    def test_process_rule_type_data_freshness(self):
        """Test _process_rule_type for data_freshness."""
        rule_config = {"column": "created_date", "max_days": 7}

        result = DQDLRuleBuilder._process_rule_type("data_freshness", rule_config)
        assert result == 'DataFreshness "created_date" <= 7 days'

    def test_process_rule_type_column_correlation(self):
        """Test _process_rule_type for column_correlation."""
        rule_config = {"column1": "price", "column2": "value"}

        result = DQDLRuleBuilder._process_rule_type("column_correlation", rule_config)
        assert result == 'ColumnCorrelation "price" "value" >= 0.5'

    def test_process_rule_type_referential_integrity(self):
        """Test _process_rule_type for referential_integrity."""
        rule_config = {
            "column": "user_id",
            "reference_table": "users",
            "reference_column": "id",
        }

        result = DQDLRuleBuilder._process_rule_type(
            "referential_integrity", rule_config
        )
        assert result == 'ReferentialIntegrity "user_id" "users.id"'

    def test_process_rule_type_standard_deviation(self):
        """Test _process_rule_type for standard_deviation."""
        rule_config = {"column": "score", "operator": "<", "value": 10}

        result = DQDLRuleBuilder._process_rule_type("standard_deviation", rule_config)
        assert result == 'StandardDeviation "score" < 10'

    def test_process_rule_type_mean(self):
        """Test _process_rule_type for mean."""
        rule_config = {"column": "rating", "operator": ">=", "value": 3.5}

        result = DQDLRuleBuilder._process_rule_type("mean", rule_config)
        assert result == 'Mean "rating" >= 3.5'

    def test_process_rule_type_sum(self):
        """Test _process_rule_type for sum."""
        rule_config = {"column": "amount", "operator": ">", "value": 1000}

        result = DQDLRuleBuilder._process_rule_type("sum", rule_config)
        assert result == 'Sum "amount" > 1000'

    def test_process_rule_type_distinct_values_count(self):
        """Test _process_rule_type for distinct_values_count."""
        rule_config = {"column": "category", "operator": ">=", "count": 5}

        result = DQDLRuleBuilder._process_rule_type(
            "distinct_values_count", rule_config
        )
        assert result == 'DistinctValuesCount "category" >= 5'

    def test_process_rule_type_entropy(self):
        """Test _process_rule_type for entropy."""
        rule_config = {"column": "data", "operator": ">", "value": 2.5}

        result = DQDLRuleBuilder._process_rule_type("entropy", rule_config)
        assert result == 'Entropy "data" > 2.5'

    def test_process_rule_type_custom_sql(self):
        """Test _process_rule_type for custom_sql."""
        rule_config = {"name": "custom_check", "sql": "COUNT(*) > 0"}

        result = DQDLRuleBuilder._process_rule_type("custom_sql", rule_config)
        assert result == 'CustomSql "custom_check" "COUNT(*) > 0" >= 1.0'

    def test_process_rule_type_unknown(self):
        """Test _process_rule_type for unknown rule type."""
        rule_config = {"column": "test"}

        result = DQDLRuleBuilder._process_rule_type("unknown_rule", rule_config)
        assert result is None

    def test_build_rules_from_config_non_list_rule(self):
        """Test build_rules_from_config with non-list rule config."""
        dq_config = {
            "enabled": True,
            "rules": {"row_count": {"min_count": 100}},  # Single dict instead of list
        }

        result = DQDLRuleBuilder.build_rules_from_config(dq_config)
        assert result == ["RowCount >= 100"]

    def test_build_rules_from_config_empty_rule(self):
        """Test build_rules_from_config with empty rule."""
        dq_config = {
            "enabled": True,
            "rules": {"completeness": [None, {"column": "name"}]},
        }

        result = DQDLRuleBuilder.build_rules_from_config(dq_config)
        assert result == ['Completeness "name" >= 1.0']
