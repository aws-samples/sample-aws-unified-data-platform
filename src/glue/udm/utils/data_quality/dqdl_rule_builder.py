# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""DQDL Rule Builder - Template-based, fully config-driven."""


class DQDLRuleBuilder:
    """Builds DQDL rule strings from configuration using templates."""

    @staticmethod
    def _interpolate_template(template: str, params: dict) -> str:
        """Interpolate template with parameters."""
        result = template
        for key, value in params.items():
            placeholder = f"{{{key}}}"
            if placeholder in result:
                if isinstance(value, list):
                    # Handle list values (e.g., for columns or valid_values)
                    if key == "valid_values":
                        # Format as quoted list: "val1", "val2"
                        formatted = '", "'.join(str(v) for v in value)
                        result = result.replace(placeholder, formatted)
                    else:
                        # Format as quoted column list: "col1", "col2"
                        formatted = '", "'.join(str(v) for v in value)
                        result = result.replace(placeholder, formatted)
                else:
                    result = result.replace(placeholder, str(value))
        return result

    @staticmethod
    def _build_conditional_rule(template: str, params: dict, conditions: dict) -> str:
        """Build rule with conditional logic based on parameters."""
        # Handle conditional templates (e.g., min/max ranges)
        for condition_key, condition_template in conditions.items():
            if all(params.get(k) is not None for k in condition_key.split("_and_")):
                return DQDLRuleBuilder._interpolate_template(condition_template, params)

        # Fallback to default template
        return DQDLRuleBuilder._interpolate_template(template, params)

    @staticmethod
    def build_rules_from_config(dq_config: dict) -> list:
        """Build all DQDL rules from configuration using templates."""
        rules = []

        if not dq_config or not dq_config.get("enabled", False):
            return rules

        rule_configs = dq_config.get("rules", {})

        # Process each rule type
        for rule_type, rule_list in rule_configs.items():
            if not isinstance(rule_list, list):
                # Handle non-list configs (e.g., row_count as dict)
                rule_list = [rule_list]

            for rule_config in rule_list:
                if not rule_config:
                    continue

                # Get template for this rule type
                template_config = rule_config.get("dqdl_template")

                if template_config:
                    # Use custom template from config
                    rule_str = DQDLRuleBuilder._interpolate_template(
                        template_config, rule_config
                    )
                    if rule_str:
                        rules.append(rule_str)
                else:
                    # Use built-in rule type processing
                    rule_str = DQDLRuleBuilder._process_rule_type(
                        rule_type, rule_config
                    )
                    if rule_str:
                        rules.append(rule_str)

        return rules

    @staticmethod
    def _process_rule_type(rule_type: str, rule_config: dict) -> str:
        """Process rule using built-in templates."""
        # Built-in templates for common rule types
        templates = {
            "completeness": {
                "template": 'Completeness "{column}" >= {threshold}',
                "defaults": {"threshold": 1.0},
            },
            "uniqueness": {
                "single": 'IsUnique "{column}"',
                "multiple": 'IsPrimaryKey "{columns}"',
            },
            "validity": {"template": 'ColumnValues "{column}" in ["{valid_values}"]'},
            "accuracy": {
                "both": (
                    'ColumnValues "{column}" >= {min_value} and '
                    'ColumnValues "{column}" <= {max_value}'
                ),
                "min": 'ColumnValues "{column}" >= {min_value}',
                "max": 'ColumnValues "{column}" <= {max_value}',
            },
            "consistency": {
                "template": 'CustomSql "{name}" "{condition}" >= {threshold}',
                "defaults": {"threshold": 1.0, "name": "consistency_check"},
            },
            "business_rules": {
                "template": 'CustomSql "{name}" "{sql}" >= {threshold}',
                "defaults": {"threshold": 1.0},
            },
            "row_count": {
                "both": "RowCount >= {min_count} and RowCount <= {max_count}",
                "min": "RowCount >= {min_count}",
                "max": "RowCount <= {max_count}",
            },
            "column_length": {
                "template": 'ColumnLength "{column}" {operator} {length}',
                "defaults": {"operator": "<="},
            },
            "column_values": {"template": 'ColumnValues "{column}" {operator} {value}'},
            "data_freshness": {
                "template": 'DataFreshness "{column}" <= {max_days} days'
            },
            "column_correlation": {
                "template": 'ColumnCorrelation "{column1}" "{column2}" >= {threshold}',
                "defaults": {"threshold": 0.5},
            },
            "referential_integrity": {
                "template": (
                    'ReferentialIntegrity "{column}" '
                    '"{reference_table}.{reference_column}"'
                )
            },
            "standard_deviation": {
                "template": 'StandardDeviation "{column}" {operator} {value}'
            },
            "mean": {"template": 'Mean "{column}" {operator} {value}'},
            "sum": {"template": 'Sum "{column}" {operator} {value}'},
            "distinct_values_count": {
                "template": 'DistinctValuesCount "{column}" {operator} {count}'
            },
            "entropy": {"template": 'Entropy "{column}" {operator} {value}'},
            "custom_sql": {
                "template": 'CustomSql "{name}" "{sql}" {operator} {threshold}',
                "defaults": {"operator": ">=", "threshold": 1.0},
            },
        }

        template_def = templates.get(rule_type)
        if not template_def:
            return None

        # Apply defaults
        params = dict(rule_config)
        if "defaults" in template_def:
            for key, default_value in template_def["defaults"].items():
                if key not in params:
                    params[key] = default_value

        # Handle special cases
        if rule_type == "uniqueness":
            columns = params.get("columns") or [params.get("column")]
            if not columns or not columns[0]:
                return None
            if len(columns) == 1:
                return f'IsUnique "{columns[0]}"'
            else:
                cols = '", "'.join(columns)
                return f'IsPrimaryKey "{cols}"'

        elif rule_type == "accuracy":
            column = params.get("column")
            min_val = params.get("min_value")
            max_val = params.get("max_value")
            if not column:
                return None
            if min_val is not None and max_val is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["both"], params
                )
            elif min_val is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["min"], params
                )
            elif max_val is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["max"], params
                )
            return None

        elif rule_type == "row_count":
            min_count = params.get("min_count")
            max_count = params.get("max_count")
            if min_count is not None and max_count is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["both"], params
                )
            elif min_count is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["min"], params
                )
            elif max_count is not None:
                return DQDLRuleBuilder._interpolate_template(
                    template_def["max"], params
                )
            return None

        # Default template processing
        if "template" in template_def:
            return DQDLRuleBuilder._interpolate_template(
                template_def["template"], params
            )

        return None
