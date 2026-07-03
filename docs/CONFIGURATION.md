# UDM Utility - Configuration Guide

## Overview

The UDM Utility uses YAML-based configuration files to define data processing pipelines, event triggers, downstream integrations, and notifications. This guide provides detailed information on all configuration options.

## Configuration File Locations

All configuration files are located in `infrastructure/udm/config/`:

```
config/
├── pipeline/{env}/              # Pipeline configurations
│   ├── table1-{env}.yaml
│   ├── table2-{env}.yaml
│   └── ...
├── eventbridge-rules/{env}/     # EventBridge trigger rules
│   └── rules.yaml
├── downstream/{env}/            # Downstream service integrations
│   └── config.yaml
└── notifications/{env}/         # SNS notification settings
    └── notifications.yaml
```

Where `{env}` is one of: `tf`, `tst`, `qa`, `rt`

## Pipeline Configuration

### File: `config/pipeline/{env}/{table-name}-{env}.yaml`

#### Basic Structure

```yaml
source: "source_identifier"
source_type: "dynamodb" | "xlsx" | "json"
source_table_name: "table_name"           # For DynamoDB
source_s3_path: "s3://bucket/path"        # For file sources
source_incremental_col: "timestamp_col"   # For incremental loads

stage:
  database_name: "database_name"
  target_table_name: "table_name"
  s3:
    target_bucket: "s3://bucket/"
    partition_col: "partition_column"
    load_method: "overwrite" | "append"

curation:
  - name: "curation_job_name"
    database_name: "curated_database"
    target_table_name: "curated_table"
    filter_source_data: "SQL WHERE clause"
    load_method: "upsert" | "append" | "overwrite"
    primary_keys:
      - "key1"
      - "key2"
    columns:
      - column1
      - column2

defaults:
  string:
    missing_value_default: ""
  int:
    missing_value_default: 0
  # ... other type defaults

columns:
  column_name:
    type: "string"
    # ... column-specific settings
```

#### Source Configuration

**DynamoDB Source:**
```yaml
source: "my_dynamodb_table"
source_type: "dynamodb"
source_table_name: "actual-dynamodb-table-name"
source_incremental_col: "updated_timestamp"  # Optional
```

**S3 JSON Source:**
```yaml
source: "my_json_data"
source_type: "json"
source_s3_path: "s3://my-bucket/data/file.json"
```

**S3 Excel Source:**
```yaml
source: "my_excel_data"
source_type: "xlsx"
source_s3_path: "s3://my-bucket/data/file.xlsx"
```

#### Stage Configuration

```yaml
stage:
  database_name: "aws_samples_udm_raw_tf"
  target_table_name: "my_table_raw"
  s3:
    target_bucket: "s3://aws-samples-raw-123456789-us-west-2-tf/"
    partition_col: "udm_processed_date"
    load_method: "overwrite"
```

**Load Methods:**
- `overwrite`: Replace all data
- `append`: Add new data without deduplication

#### Curation Configuration

```yaml
curation:
  - name: "my_table_curated"
    database_name: "aws_samples_udm_curated_tf"
    target_table_name: "my_table_curated"
    filter_source_data: "status = 'active'"  # Optional SQL filter
    load_method: "upsert"
    primary_keys:
      - "id"
      - "version"
    columns:
      - id
      - name
      - email
      - created_date
      - status
```

**Load Methods:**
- `upsert`: Update existing records, insert new ones (requires primary_keys)
- `append`: Add all records
- `overwrite`: Replace all data

#### Column Transformations

**String Type:**
```yaml
columns:
  email:
    type: "string"
    lowercase: true
    skip_cleaning: false
    missing_value_default: ""
    corrections:
      "old_value": "new_value"
```

**Numeric Types:**
```yaml
columns:
  quantity:
    type: "int"
    missing_value_default: 0
  
  price:
    type: "float"
    missing_value_default: 0.0
  
  total:
    type: "double"
    missing_value_default: 0.0
```

**Date/Timestamp Types:**
```yaml
columns:
  created_date:
    type: "date"
    date_overrides:
      "0000-00-00": null
      "1900-01-01": null
  
  updated_timestamp:
    type: "timestamp"
    date_overrides:
      "0": null
```

**Value Normalization:**
```yaml
columns:
  status:
    type: "value_normalization"
    normalization_key: "status_mapping"
    missing_value_default: "Unknown"

defaults:
  value_normalization:
    normalization_key:
      status_mapping:
        "active": "Active"
        "inactive": "Inactive"
        "pending": "Pending"
```

**Nested Data (Struct):**
```yaml
columns:
  address:
    type: "struct"
    source_path: "user.address"
    missing_value_default: null
```

**Array Explode:**
```yaml
columns:
  tags:
    type: "array_explode"
    explode_empty_arrays: false
    missing_value_default: null
```

#### Complete Example

```yaml
source: "contract_metadata_table"
source_type: "dynamodb"
source_table_name: "contract-data-metadata-tf"
source_incremental_col: "updated_timestamp"

stage:
  database_name: "aws_samples_udm_raw_tf"
  target_table_name: "contract_metadata_raw"
  s3:
    target_bucket: "s3://aws-samples-raw-123456789-us-west-2-tf/"
    partition_col: "udm_processed_date"
    load_method: "overwrite"

curation:
  - name: "contract_metadata_curated"
    database_name: "aws_samples_udm_curated_tf"
    target_table_name: "contract_metadata_curated"
    filter_source_data: "status != 'deleted'"
    load_method: "upsert"
    primary_keys:
      - "contract_id"
    columns:
      - contract_id
      - contract_name
      - vendor_name
      - status
      - start_date
      - end_date
      - total_amount
      - created_timestamp

defaults:
  string:
    missing_value_default: ""
  int:
    missing_value_default: 0
  float:
    missing_value_default: 0.0
  date:
    date_overrides:
      "0000-00-00": null
  timestamp:
    date_overrides:
      "0": null
  value_normalization:
    missing_value_default: "Unknown"
    normalization_key:
      status_mapping:
        "active": "Active"
        "inactive": "Inactive"
        "draft": "Draft"

columns:
  contract_id:
    type: "string"
  
  contract_name:
    type: "string"
    skip_cleaning: false
  
  vendor_name:
    type: "string"
    lowercase: false
  
  status:
    type: "value_normalization"
    normalization_key: "status_mapping"
  
  start_date:
    type: "date"
  
  end_date:
    type: "date"
  
  total_amount:
    type: "float"
  
  created_timestamp:
    type: "timestamp"
```

## EventBridge Rules Configuration

### File: `config/eventbridge-rules/{env}/rules.yaml`

#### S3 Object Created Trigger

```yaml
eventbridge_rules:
  - name: "s3-file-upload-trigger"
    rule_type: "s3_object_created"
    s3_bucket_name: "source-bucket-name"
    s3_key_prefix: "data/contracts/"  # Optional
    target_step_function: "aws_samples_udm_pipeline"
    enabled: true
    input_parameters:
      tables_list:
        - "contract_metadata-tf"
        - "document_metadata-tf"
      process_type: "incremental"
```

#### Step Function Completion Trigger

```yaml
eventbridge_rules:
  - name: "upstream-pipeline-completion"
    rule_type: "step_function_completion"
    source_step_function_arn: "arn:aws:states:us-west-2:123456789:stateMachine:upstream-pipeline"
    completion_status: "SUCCEEDED"
    target_step_function: "aws_samples_udm_pipeline"
    enabled: true
    input_parameters:
      tables_list:
        - "processed_data-tf"
```

#### Scheduled Trigger

```yaml
scheduled_rules:
  - name: "daily-udm-pipeline"
    schedule_expression: "cron(0 2 * * ? *)"  # 2 AM UTC daily
    target_step_function: "aws_samples_udm_pipeline"
    enabled: true
    tables_list:
      - "contract_metadata-tf"
      - "invoice_metadata-tf"
```

**Cron Expression Examples:**
- `cron(0 2 * * ? *)` - Daily at 2 AM UTC
- `cron(0 */6 * * ? *)` - Every 6 hours
- `cron(0 0 ? * MON *)` - Every Monday at midnight
- `cron(0 0 1 * ? *)` - First day of every month

#### Environment-Specific Enablement

```yaml
eventbridge_rules:
  - name: "production-only-trigger"
    rule_type: "s3_object_created"
    s3_bucket_name: "prod-bucket"
    target_step_function: "aws_samples_udm_pipeline"
    enabled:
      tf: false
      tst: false
      qa: true
      rt: true
    input_parameters:
      tables_list:
        - "production_data"
```

## Downstream Configuration

### File: `config/downstream/{env}/config.yaml`

#### Step Function Downstream

```yaml
downstream_configs:
  - tables_list:
      - "contract_metadata_curated"
      - "invoice_metadata_curated"
    service_type: "step_function"
    service_arn: "arn:aws:states:us-west-2:123456789:stateMachine:analytics-pipeline"
    additional_parameters:
      process_type: "incremental"
      priority: "high"
```

#### Lambda Downstream

```yaml
downstream_configs:
  - tables_list:
      - "document_metadata_curated"
    service_type: "lambda"
    service_arn: "arn:aws:lambda:us-west-2:123456789:function:document-processor"
    additional_parameters:
      batch_size: 100
```

#### SNS Downstream

```yaml
downstream_configs:
  - tables_list:
      - "contract_metadata_curated"
    service_type: "sns"
    service_arn: "arn:aws:sns:us-west-2:123456789:contract-updates"
    additional_parameters:
      notification_type: "contract_update"
```

#### SQS Downstream

```yaml
downstream_configs:
  - tables_list:
      - "invoice_metadata_curated"
    service_type: "sqs"
    service_arn: "arn:aws:sqs:us-west-2:123456789:invoice-processing-queue"
    additional_parameters:
      message_group_id: "invoice_processing"
```

#### Trigger for All Tables

```yaml
downstream_configs:
  - tables_list: []  # Empty list = trigger for all tables
    service_type: "sns"
    service_arn: "arn:aws:sns:us-west-2:123456789:all-tables-complete"
```

## Notifications Configuration

### File: `config/notifications/{env}/notifications.yaml`

#### Email Notifications

```yaml
sns_topics:
  aws_samples_udm_pipeline_failure:
    - protocol: "email"
      endpoint: "team@example.com"
    - protocol: "email"
      endpoint: "oncall@example.com"
  
  aws_samples_udm_pipeline_downstream:
    - protocol: "email"
      endpoint: "data-team@example.com"
```

#### Lambda Notifications

```yaml
sns_topics:
  aws_samples_udm_pipeline_failure:
    - protocol: "lambda"
      endpoint: "error-handler-function"
```

#### SQS Notifications

```yaml
sns_topics:
  aws_samples_udm_pipeline_downstream:
    - protocol: "sqs"
      endpoint: "arn:aws:sqs:us-west-2:123456789:notification-queue"
```

## Configuration Best Practices

### 1. Naming Conventions

- Use descriptive names for tables and jobs
- Include environment suffix in table names: `table-name-{env}`
- Use snake_case for configuration keys
- Use kebab-case for resource names

### 2. Environment Management

- Maintain separate configs for each environment
- Use environment variables in Terraform for dynamic values
- Test configurations in TF environment first
- Document environment-specific differences

### 3. Performance Optimization

- Use appropriate partition columns (usually date-based)
- Choose correct load methods (upsert vs append vs overwrite)
- Enable incremental processing when possible
- Limit column transformations to necessary ones

### 4. Error Handling

- Set appropriate missing value defaults
- Use date_overrides for invalid dates
- Implement data quality checks
- Monitor CloudWatch logs

### 5. Security

- Store sensitive values in AWS Secrets Manager
- Use IAM roles with least privilege
- Enable encryption for all data
- Audit access to configuration files

### 6. Documentation

- Comment complex transformations
- Document business logic in configs
- Maintain changelog for config changes
- Version control all configuration files

## Configuration Validation

Before deploying, validate configurations:

```bash
# Validate YAML syntax
yamllint config/

# Validate Terraform
cd infrastructure/udm
terraform validate

# Test pipeline with minimal data
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"tables_list": ["test-table"]}'
```

## Troubleshooting Configuration Issues

### Issue: Pipeline Not Triggering

**Check:**
1. EventBridge rule is enabled
2. Event pattern matches actual events
3. Target Step Function ARN is correct
4. IAM permissions are configured

### Issue: Transformation Errors

**Check:**
1. Column names match source data
2. Type conversions are valid
3. Missing value defaults are set
4. Source path for nested data is correct

### Issue: Downstream Not Triggered

**Check:**
1. Table names match completed tables
2. Service ARNs are correct
3. Lambda has permissions to invoke services
4. Configuration file is synced to S3

## Configuration Examples Repository

See `infrastructure/udm/config/` for complete working examples of all configuration types.
