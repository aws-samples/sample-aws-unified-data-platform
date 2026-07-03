# Data Quality Module

Config-driven AWS Glue DQDL integration for UDM pipeline.

## Features

- **Config-Driven**: Define rules in YAML config files
- **Non-Intrusive**: Only applies when enabled in config
- **DQDL-Based**: Uses AWS Glue Data Quality Definition Language
- **Exception Handling**: Failed records to Iceberg exception tables
- **Threshold Management**: Configurable error thresholds
- **Row-Level Filtering**: Native Glue DQ row-level outcomes

## Quick Start

### Enable DQ for a Table

```yaml
curation:
  - name: my_table
    data_quality:
      enabled: true
      exception_handling:
        exception_database: "my_database"
        max_error_threshold: 0.05
        fail_on_threshold: false
      rules:
        # Add rules here
```

### Common Rule Patterns

#### Completeness (Required Fields)
```yaml
completeness:
  - column: "id"
    threshold: 1.0  # 100% required
  - column: "name"
    threshold: 0.95  # 95% required
```

#### Uniqueness (Primary Keys)
```yaml
uniqueness:
  - column: "id"  # Single column
  - columns: ["id", "date"]  # Composite key
```

#### Validity (Allowed Values)
```yaml
validity:
  - column: "status"
    valid_values: ["active", "inactive", "pending"]
```

#### Accuracy (Numeric Ranges)
```yaml
accuracy:
  - column: "amount"
    min_value: 0
    max_value: 1000000
  - column: "quantity"
    min_value: 0  # Only minimum
```

#### Consistency (Date Logic)
```yaml
consistency:
  - name: "date_order"
    condition: "start_date <= end_date"
    threshold: 1.0
```

#### Business Rules (Custom SQL)
```yaml
business_rules:
  - name: "capacity_check"
    sql: "capacity >= 0 OR capacity IS NULL"
    threshold: 1.0
```

#### Row Count
```yaml
row_count:
  min_count: 1
  max_count: 1000000
```

#### Column Length
```yaml
column_length:
  - column: "contract_name"
    operator: "<="
    length: 100
```

#### Custom DQDL Templates (Any Rule Type)
```yaml
custom_rules:
  # Column data type validation
  - dqdl_template: 'ColumnDataType "{column}" = "{data_type}"'
    column: "amount"
    data_type: "decimal"
  
  # Pattern matching
  - dqdl_template: 'ColumnValues "{column}" matches "{pattern}"'
    column: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
  
  # Column exists check
  - dqdl_template: 'ColumnExists "{column}"'
    column: "required_field"
```

## Integration Code

```python
# Import
from utils.data_quality import DataQualityManager

# Initialize (in main function)
dq_manager = DataQualityManager(glue_context, data_writer, logger)

# Apply (in target processing loop, after filter, before transform)
dq_config = dq_manager.get_dq_config_for_target(cfg, target_name)
if dq_config and dq_config.get("enabled", False):
    target_dataframe, dq_metrics = dq_manager.apply_data_quality_checks(
        dataframe=target_dataframe,
        dq_config=dq_config,
        source_name=source_name,
        target_name=target_name,
        execution_id=args["stepfunction_execution_id"],
        table_location=args["table_location"]
    )
    logger.info(f"DQ metrics: {dq_metrics}")
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable/disable DQ checks |
| `fail_on_error` | `true` | Fail job if DQ evaluation errors |
| `max_error_threshold` | `0.05` | Max % of failed rows (5%) |
| `fail_on_threshold` | `false` | Fail job if threshold exceeded |
| `exception_database` | Required | Database for exception tables |

## DQ Metrics Returned

```python
{
    "total_rules": 5,
    "passed_rules": 4,
    "failed_rules": 1,
    "total_rows": 1000,
    "passed_rows": 950,
    "failed_rows": 50
}
```

## Complete Configuration Example

```yaml
data_quality:
  enabled: true
  exception_handling:
    exception_database: "aws_samples_udm_curated_tf"
    max_error_threshold: 0.05
    fail_on_threshold: false
  rules:
    completeness:
      - column: "contract_uuid"
        threshold: 1.0
      - column: "contract_name"
        threshold: 0.95
    uniqueness:
      - columns: ["contract_uuid", "documents_hash"]
    validity:
      - column: "status"
        valid_values: ["active", "inactive", "pending"]
    accuracy:
      - column: "base_rate_value"
        min_value: 0
    consistency:
      - name: "date_check"
        condition: "contract_start_date <= effective_date"
        threshold: 0.95
    column_length:
      - column: "contract_name"
        operator: "<="
        length: 200
    custom_rules:
      - dqdl_template: 'ColumnValues "{column}" matches "{pattern}"'
        column: "contract_name"
        pattern: "^[A-Za-z]+$"
      - dqdl_template: 'ColumnExists "{column}"'
        column: "contract_uuid"
```

## Custom DQDL Rules Guide

### Overview

The data quality framework is **fully config-driven** - you can add any DQDL rule type without code changes using custom templates.

### Two Ways to Define Rules

#### 1. Built-in Rule Types (Simplified)

Use predefined rule types with simplified syntax:

```yaml
rules:
  completeness:
    - column: "contract_uuid"
      threshold: 1.0
```

#### 2. Custom DQDL Templates (Any Rule Type)

Use `dqdl_template` to define any DQDL rule directly:

```yaml
rules:
  custom_rules:
    - dqdl_template: 'ColumnLength "contract_name" <= {max_length}'
      max_length: 100
```

### Built-in Rule Types

The framework includes templates for common rule types:

| Rule Type | Template | Parameters |
|-----------|----------|------------|
| `completeness` | `Completeness "{column}" >= {threshold}` | column, threshold |
| `uniqueness` | `IsUnique "{column}"` or `IsPrimaryKey` | column(s) |
| `validity` | `ColumnValues "{column}" in [...]` | column, valid_values |
| `accuracy` | `ColumnValues "{column}" >= {min} and <= {max}` | column, min_value, max_value |
| `consistency` | `CustomSql "{name}" "{condition}" >= {threshold}` | name, condition, threshold |
| `business_rules` | `CustomSql "{name}" "{sql}" >= {threshold}` | name, sql, threshold |
| `row_count` | `RowCount >= {min} and <= {max}` | min_count, max_count |
| `column_length` | `ColumnLength "{column}" {operator} {length}` | column, operator, length |
| `column_values` | `ColumnValues "{column}" {operator} {value}` | column, operator, value |
| `data_freshness` | `DataFreshness "{column}" <= {max_days} days` | column, max_days |
| `column_correlation` | `ColumnCorrelation "{column1}" "{column2}" >= {threshold}` | column1, column2, threshold |
| `referential_integrity` | `ReferentialIntegrity "{column}" "{ref_table}.{ref_column}"` | column, reference_table, reference_column |
| `standard_deviation` | `StandardDeviation "{column}" {operator} {value}` | column, operator, value |
| `mean` | `Mean "{column}" {operator} {value}` | column, operator, value |
| `sum` | `Sum "{column}" {operator} {value}` | column, operator, value |
| `distinct_values_count` | `DistinctValuesCount "{column}" {operator} {count}` | column, operator, count |
| `entropy` | `Entropy "{column}" {operator} {value}` | column, operator, value |
| `custom_sql` | `CustomSql "{name}" "{sql}" {operator} {threshold}` | name, sql, operator, threshold |

### Adding New Rule Types (No Code Changes)

#### Method 1: Use Custom Template in Config

```yaml
rules:
  # Any new DQDL rule type from AWS documentation
  custom_rules:
    - dqdl_template: 'ColumnDataType "{column}" = "{expected_type}"'
      column: "amount"
      expected_type: "decimal"
    
    - dqdl_template: 'RowCountMatch "{table1}" "{table2}"'
      table1: "source_table"
      table2: "target_table"
    
    - dqdl_template: 'SchemaMatch "{table1}" "{table2}"'
      table1: "source_table"
      table2: "target_table"
```

#### Method 2: Use Built-in Templates with Custom Parameters

```yaml
rules:
  # Statistical rules
  standard_deviation:
    - column: "base_rate_value"
      operator: "<"
      value: 100
  
  mean:
    - column: "capacity_value"
      operator: ">"
      value: 0
  
  # Referential integrity
  referential_integrity:
    - column: "contract_uuid"
      reference_table: "contracts"
      reference_column: "uuid"
```

### Template Syntax

Templates use `{parameter_name}` placeholders that are replaced with values from the config:

```yaml
# Template definition
dqdl_template: 'RuleName "{column}" {operator} {value}'

# Parameters
column: "my_column"
operator: ">="
value: 100

# Result
RuleName "my_column" >= 100
```

### Special Handling

**Lists** are automatically formatted:
```yaml
# For valid_values
valid_values: ["active", "inactive"]
# Becomes: 'active', 'inactive'

# For columns
columns: ["col1", "col2"]
# Becomes: "col1", "col2"
```

## Exception Handling

### Row-Level vs Dataset-Level Rules

#### Row-Level Rules (Can identify failed records)
- `ColumnValues`
- `ColumnLength`
- `Completeness`
- `IsUnique`
- Custom pattern matching

#### Dataset-Level Rules (Skipped for row evaluation)
- `ColumnExists`
- `ColumnCount`
- `RowCount`
- `SchemaMatch`
- `RowCountMatch`

Dataset-level rules appear in `DataQualityRulesSkip` column in exception tables.

### Exception Table Structure

Failed records are written to `{target_table_name}_exception` with additional columns:
- `DataQualityRulesPass`: Array of passed rules
- `DataQualityRulesFail`: Array of failed rules
- `DataQualityRulesSkip`: Array of skipped rules
- `DataQualityEvaluationResult`: "Failed"
- `exception_timestamp`: When record was written
- `execution_id`: Job execution ID

### Data Flow

1. **DQ Evaluation**: AWS Glue Data Quality evaluates all rules
2. **Row-Level Filtering**: Records split based on `DataQualityEvaluationResult`
3. **Passed Records**: Clean data (DQ columns removed) → Final curated table
4. **Failed Records**: Original data + DQ columns → Exception table
5. **Audit Tracking**: Metrics logged to audit table

## Audit Integration

DQ metrics automatically tracked in audit table:

```python
# Extract DQ metrics
no_of_rows_failed_dq_check = dq_metrics.get("failed_rows", 0)

# Pass to audit
audit_manager.end_stage_job(
    stepfunction_execution_id,
    source_table_name=source_name,
    job_type="curated",
    status="Completed",
    no_of_rows_processed=final_row_count,
    curation_name=target_name,
    no_of_rows_failed_dq_check=no_of_rows_failed_dq_check
)
```

## Advanced Examples

### Statistical Rules

```yaml
rules:
  # Statistical validation
  mean:
    - column: "base_rate_value"
      operator: "between"
      value: "0 and 1000"
  
  standard_deviation:
    - column: "capacity_value"
      operator: "<"
      value: 500
  
  sum:
    - column: "total_amount"
      operator: ">"
      value: 0
  
  distinct_values_count:
    - column: "status"
      operator: "<="
      count: 10
  
  entropy:
    - column: "category"
      operator: ">"
      value: 0.5
```

### Complex Custom Rules

```yaml
rules:
  custom_rules:
    # Schema validation
    - dqdl_template: 'SchemaMatch "{source_table}" "{target_table}"'
      source_table: "staging.contracts"
      target_table: "curated.contracts"
    
    # Row count comparison
    - dqdl_template: 'RowCountMatch "{table1}" "{table2}" within {tolerance}%'
      table1: "source_table"
      table2: "target_table"
      tolerance: 5
    
    # Data type validation
    - dqdl_template: 'ColumnDataType "{column}" in ["{types}"]'
      column: "amount"
      types: "decimal, double, float"
    
    # Pattern matching
    - dqdl_template: 'ColumnValues "{column}" matches "{regex}"'
      column: "email"
      regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

### Multi-Column Rules

```yaml
rules:
  # Column correlation
  column_correlation:
    - column1: "quantity"
      column2: "total_amount"
      threshold: 0.8
  
  # Custom multi-column validation
  custom_rules:
    - dqdl_template: 'ColumnPairValues "{col1}" and "{col2}" satisfy "{condition}"'
      col1: "start_date"
      col2: "end_date"
      condition: "col1 <= col2"
    
    - dqdl_template: 'ColumnSum "{col1}" + "{col2}" = "{col3}"'
      col1: "base_amount"
      col2: "tax_amount"
      col3: "total_amount"
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DQ not running | Check `enabled: true` in config |
| Job fails on DQ | Set `fail_on_error: false` temporarily |
| Too many failures | Adjust `max_error_threshold` |
| Invalid DQDL syntax | Check rule format against AWS docs |
| Rule appears in skip | Dataset-level rule (normal behavior) |
| Template not interpolated | Ensure parameters are at same level as `dqdl_template` |
| List formatting wrong | Use `valid_values` or `columns` for automatic formatting |

## Best Practices

1. **Start with Built-in Types**: Use simplified syntax for common rules
2. **Use Custom Templates for New Types**: Add any AWS DQDL rule without code changes
3. **Test Templates**: Verify DQDL syntax in dev environment first
4. **Document Custom Rules**: Add comments explaining complex templates
5. **Monitor Exception Tables**: Review failed records regularly
6. **Set Appropriate Thresholds**: Balance data quality vs pipeline reliability
7. **Use Row-Level Rules**: For record-level filtering and exception handling
8. **Review Skipped Rules**: Dataset-level rules won't filter individual records

## Disable DQ

```yaml
data_quality:
  enabled: false
```

Or remove the `data_quality` section entirely.

## AWS DQDL Documentation

For complete DQDL rule types and syntax:
https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types.html