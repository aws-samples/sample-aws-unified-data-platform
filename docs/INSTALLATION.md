# UDM Utility - Installation & Setup Guide

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Configuration](#configuration)
4. [Deployment](#deployment)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Tools

- **Terraform**: >= 1.5
- **Python**: 3.12
- **AWS CLI**: >= 2.0
- **Git**: Latest version

### AWS Requirements

- AWS Account with administrative access
- IAM permissions for:
  - S3, Glue, Lambda, Step Functions, EventBridge
  - DynamoDB, SNS, CloudWatch, Athena
  - IAM role creation
- VPC with private subnets (optional, for VPC-enabled resources)

### Optional Tools

- **S3 Backend**: For remote Terraform state management (recommended)
- **GitHub**: For CI/CD integration
- **Docker**: For local testing

## Initial Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd udm-utility
```

### 2. Configure AWS Credentials

```bash
aws configure
# Or use AWS SSO
aws sso login --profile your-profile
export AWS_PROFILE=your-profile
```

### 3. Set Environment Variables

```bash
export AWS_REGION=us-west-2
export ENV_SUFFIX=tf  # or tst, qa, rt
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### 4. Initialize Terraform

```bash
cd infrastructure/udm
terraform init
```

## Configuration

### 1. Update Terraform Variables

Edit `infrastructure/udm/tfvars/{env}.tfvars`:

```hcl
region     = "us-west-2"
env_suffix = "tf"
```

### 2. Create Pipeline Configuration

Create `infrastructure/udm/config/pipeline/tf/your-table-tf.yaml`:

```yaml
source: "your_source_table"
source_type: "dynamodb"

stage:
  database_name: "aws_samples_udm_raw_tf"
  target_table_name: "your_table_raw"
  s3:
    target_bucket: "s3://aws-samples-raw-${account_id}-${region}-tf/"
    partition_col: "udm_processed_date"
    load_method: "overwrite"

curation:
  - name: "your_table_curated"
    database_name: "aws_samples_udm_curated_tf"
    target_table_name: "your_table_curated"
    s3:
      target_bucket: "s3://aws-samples-curated-${account_id}-${region}-tf/"
      load_method: "upsert"
    primary_keys:
      - "id"
    columns:
      - id
      - name
      - created_date

columns:
  id:
    type: "string"
  name:
    type: "string"
    lowercase: true
  created_date:
    type: "timestamp"
```

### 3. Configure EventBridge Rules (Optional)

Edit `infrastructure/udm/config/eventbridge-rules/tf/rules.yaml`:

```yaml
eventbridge_rules:
  - name: "your-table-s3-trigger"
    rule_type: "s3_object_created"
    s3_bucket_name: "your-source-bucket"
    s3_key_prefix: "data/"
    target_step_function: "aws_samples_udm_pipeline"
    enabled: true
    input_parameters:
      tables_list:
        - "your-table-tf"

scheduled_rules:
  - name: "daily-udm-pipeline"
    schedule_expression: "cron(0 2 * * ? *)"
    target_step_function: "aws_samples_udm_pipeline"
    enabled: true
    tables_list:
      - "your-table-tf"
```

### 4. Configure Downstream Services (Optional)

Edit `infrastructure/udm/config/downstream/tf/config.yaml`:

```yaml
downstream_configs:
  - tables_list: ["your-table-tf"]
    service_type: "step_function"
    service_arn: "arn:aws:states:${region}:${account_id}:stateMachine:downstream-pipeline"
    additional_parameters:
      process_type: "incremental"
```

### 5. Configure Notifications

Edit `infrastructure/udm/config/notifications/tf/notifications.yaml`:

```yaml
sns_topics:
  aws_samples_udm_pipeline_failure:
    - protocol: "email"
      endpoint: "your-email@example.com"
  
  aws_samples_udm_pipeline_downstream:
    - protocol: "email"
      endpoint: "your-email@example.com"
```

## Deployment

### Option 1: Manual Deployment

```bash
cd infrastructure/udm

# Plan
terraform plan -var-file=tfvars/tf.tfvars -out=tfplan

# Review plan
terraform show tfplan

# Apply
terraform apply tfplan

# Sync config files to S3
aws s3 sync ./config/pipeline/tf s3://aws-samples-config-${AWS_ACCOUNT_ID}-${AWS_REGION}-tf/udm/pipeline/ --delete
aws s3 sync ./config/downstream/tf s3://aws-samples-config-${AWS_ACCOUNT_ID}-${AWS_REGION}-tf/udm/downstream/ --delete
```

### Option 2: CI/CD Deployment

1. **Configure GitHub Secrets**:
   - `AWS_ROLE_ARN` (OIDC role for GitHub Actions)

2. **Configure GitHub Variables** (per environment):
   - `ACCOUNT_ID`
   - `REGION`
   - `SUFFIX`
   - `DEPLOYMENT_ROLE_ARN`

3. **Push to Branch**:
   ```bash
   git checkout -b feature/udm-setup
   git add .
   git commit -m "Configure UDM pipeline"
   git push origin feature/udm-setup
   ```

4. **Create Pull Request**: CI/CD will run Terraform plan

5. **Merge to Branch**:
   - Merge to `tst` → Deploys to TF and TST
   - Merge to `main` → Deploys to QA
   - Merge to `rt` → Deploys to RT

## Verification

### 1. Verify Infrastructure

```bash
# Check S3 buckets
aws s3 ls | grep aws-samples

# Check Glue databases
aws glue get-databases | grep udm

# Check Step Functions
aws stepfunctions list-state-machines | grep udm-pipeline

# Check Lambda functions
aws lambda list-functions | grep udm

# Check EventBridge rules
aws events list-rules | grep udm
```

### 2. Test Pipeline Execution

```bash
# Start Step Function execution
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:aws-samples-udm-pipeline-tf \
  --input '{"tables_list": ["your-table-tf"]}' \
  --name test-execution-$(date +%s)

# Monitor execution
aws stepfunctions describe-execution \
  --execution-arn <execution-arn>

# Check CloudWatch logs
aws logs tail /aws/stepfunctions/aws-samples-udm-pipeline-tf --follow
```

### 3. Verify Data Processing

```bash
# Check raw data in S3
aws s3 ls s3://aws-samples-raw-${AWS_ACCOUNT_ID}-${AWS_REGION}-tf/your_table_raw/

# Check curated data in S3
aws s3 ls s3://aws-samples-curated-${AWS_ACCOUNT_ID}-${AWS_REGION}-tf/your_table_curated/

# Query with Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM aws_samples_udm_curated_tf.your_table_curated LIMIT 10" \
  --result-configuration "OutputLocation=s3://your-athena-results-bucket/"
```

### 4. Check Audit Table

```bash
# Query DynamoDB audit table
aws dynamodb scan \
  --table-name aws-samples-udm-audit-table-tf \
  --limit 10
```

## Troubleshooting

### Common Issues

#### Issue: Terraform Init Fails

**Error**: Backend configuration not found

**Solution**:
```bash
# Check terraform.tf backend configuration
# Ensure S3 backend bucket exists or use local backend
terraform init
```

#### Issue: Glue Job Fails

**Error**: Table not found

**Solution**:
1. Verify source table exists
2. Check pipeline config has correct table name
3. Review Glue job logs in CloudWatch

```bash
aws logs tail /aws-glue/jobs/logs-v2/ --follow
```

#### Issue: Step Function Fails

**Error**: Insufficient permissions

**Solution**:
1. Check IAM role has required permissions
2. Review Step Function execution logs
3. Verify resource ARNs in configuration

```bash
aws stepfunctions describe-execution --execution-arn <arn>
```

#### Issue: EventBridge Rule Not Triggering

**Solution**:
1. Verify rule is enabled in config
2. Check rule pattern matches event
3. Test rule manually

```bash
aws events put-events \
  --entries '[{"Source":"aws.s3","DetailType":"Object Created","Detail":"{}"}]'
```

#### Issue: Lambda Function Timeout

**Solution**:
1. Increase timeout in Terraform
2. Optimize Lambda code
3. Check CloudWatch logs

```bash
aws logs tail /aws/lambda/aws-samples-udm-downstream-trigger-tf --follow
```

### Debug Mode

Enable debug logging in Glue jobs:

```python
# Add to Glue job script
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
```

### Getting Help

1. Check CloudWatch Logs for detailed error messages
2. Review DynamoDB audit table for execution history
3. Verify all configuration files are synced to S3
4. Test with minimal configuration first
5. Review infrastructure/udm/README.md for detailed documentation

## Next Steps

After successful deployment:

1. **Monitor Pipeline**: Set up CloudWatch dashboards
2. **Optimize Performance**: Adjust Glue worker counts
3. **Add More Sources**: Create additional pipeline configs
4. **Set Up Alerts**: Configure SNS notifications
5. **Document Custom Logic**: Add comments to transformations
6. **Schedule Maintenance**: Plan for Iceberg table optimization

## Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
