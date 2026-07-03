# UDM Utility - Architecture Documentation

## Overview

The UDM (Unified Data Model) Utility implements a serverless, event-driven data processing framework on AWS using a medallion architecture pattern. It provides a scalable, configurable solution for transforming data from various sources into a standardized format.

## Architecture Patterns

### Medallion Architecture

The utility implements a three-layer medallion architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER (Semantic)                     │
│                     Athena Views for Analytics                   │
│                    Business-Ready Data Models                    │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (Curated)                      │
│                   Cleaned & Transformed Data                     │
│              Business Logic Applied, Deduplicated                │
│                      Apache Iceberg Tables                       │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER (Raw)                         │
│                    Raw Data from Sources                         │
│                 Minimal Transformation Applied                   │
│                      Apache Iceberg Tables                       │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│              DynamoDB | S3 (JSON/Excel) | APIs                   │
└─────────────────────────────────────────────────────────────────┘
```

### Event-Driven Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   S3 Event   │────▶│ EventBridge  │────▶│     Step     │
│              │     │     Rule     │     │   Function   │
└──────────────┘     └──────────────┘     └──────────────┘
                                                   │
┌──────────────┐     ┌──────────────┐            │
│  Scheduled   │────▶│ EventBridge  │────────────┤
│    Cron      │     │     Rule     │            │
└──────────────┘     └──────────────┘            │
                                                   │
┌──────────────┐     ┌──────────────┐            │
│ Step Function│────▶│ EventBridge  │────────────┤
│  Completion  │     │     Rule     │            │
└──────────────┘     └──────────────┘            │
                                                   ▼
                                          ┌──────────────┐
                                          │  UDM Pipeline│
                                          │ Orchestration│
                                          └──────────────┘
```

## Component Architecture

### 1. Orchestration Layer

**AWS Step Functions**

```json
{
  "StartAt": "Initialize",
  "States": {
    "Initialize": {
      "Type": "Task",
      "Resource": "arn:aws:states:::dynamodb:putItem",
      "Next": "ProcessTables"
    },
    "ProcessTables": {
      "Type": "Map",
      "ItemsPath": "$.tables_list",
      "Iterator": {
        "StartAt": "StageJob",
        "States": {
          "StageJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync"
          },
          "CuratedJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync"
          }
        }
      },
      "Next": "NotifySuccess"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "End": true
    }
  }
}
```

**Key Features:**
- Parallel processing of multiple tables
- Built-in error handling and retries
- Audit trail in DynamoDB
- SNS notifications on success/failure

### 2. Processing Layer

**AWS Glue Jobs**

#### Stage Job (Bronze Layer)
- **Purpose**: Ingest raw data from sources
- **Input**: DynamoDB tables, S3 files (JSON/Excel)
- **Output**: Apache Iceberg tables in S3
- **Features**:
  - Incremental loading support
  - Schema inference
  - Partition management
  - Data validation

#### Curated Job (Silver Layer)
- **Purpose**: Transform and cleanse data
- **Input**: Bronze layer Iceberg tables
- **Output**: Curated Iceberg tables
- **Features**:
  - Column transformations
  - Value normalization
  - Deduplication
  - Business logic application
  - Upsert/append/overwrite modes

**Glue Job Architecture:**

```
┌─────────────────────────────────────────────────────────────────┐
│                         Glue Job                                 │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │   Config     │───▶│ Transformation│───▶│    Writer    │     │
│  │   Reader     │    │    Engine     │    │   (Iceberg)  │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
│         │                    │                    │             │
│         ▼                    ▼                    ▼             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  S3 Config   │    │  Data Quality│    │  S3 Iceberg  │     │
│  │    Files     │    │   Validation │    │   Warehouse  │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### 3. Integration Layer

**AWS Lambda Functions**

#### Downstream Trigger Lambda
```python
def lambda_handler(event, context):
    # Parse SNS message
    tables_completed = parse_sns_message(event)
    
    # Load downstream config
    config = load_config_from_s3()
    
    # Trigger downstream services
    for service_config in config:
        if matches_tables(tables_completed, service_config):
            trigger_service(service_config)
```

**Supported Downstream Services:**
- AWS Step Functions
- AWS Lambda
- Amazon SNS
- Amazon SQS

#### Semantic View Creator Lambda
```python
def lambda_handler(event, context):
    # Get curated tables
    tables = event['tables_list']
    
    # Create Athena views
    for table in tables:
        create_athena_view(table)
```

### 4. Storage Layer

**Amazon S3 with Apache Iceberg**

```
s3://bucket/
├── database/
│   └── table/
│       ├── data/
│       │   ├── partition1/
│       │   │   └── data-file.parquet
│       │   └── partition2/
│       │       └── data-file.parquet
│       └── metadata/
│           ├── v1.metadata.json
│           ├── v2.metadata.json
│           └── snap-123.avro
```

**Iceberg Benefits:**
- ACID transactions
- Schema evolution
- Time travel queries
- Hidden partitioning
- Efficient updates/deletes

### 5. Event Layer

**Amazon EventBridge**

**Event Pattern Examples:**

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["source-bucket"]
    },
    "object": {
      "key": [{
        "prefix": "data/"
      }]
    }
  }
}
```

```json
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "status": ["SUCCEEDED"],
    "stateMachineArn": ["arn:aws:states:*:*:stateMachine:upstream-pipeline"]
  }
}
```

### 6. Monitoring Layer

**CloudWatch Logs & Metrics**

```
┌─────────────────────────────────────────────────────────────────┐
│                      CloudWatch Monitoring                       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │ Step Function│    │  Glue Jobs   │    │   Lambda     │     │
│  │     Logs     │    │     Logs     │    │    Logs      │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
│         │                    │                    │             │
│         └────────────────────┴────────────────────┘             │
│                              │                                  │
│                              ▼                                  │
│                    ┌──────────────────┐                         │
│                    │  CloudWatch      │                         │
│                    │  Dashboards      │                         │
│                    └──────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

**DynamoDB Audit Table**

```
┌─────────────────────────────────────────────────────────────────┐
│                      Audit Table Schema                          │
│                                                                  │
│  Hash Key: stepfunction_execution_id                            │
│  Range Key: table_name                                          │
│                                                                  │
│  Attributes:                                                     │
│  - execution_start_time                                         │
│  - execution_end_time                                           │
│  - status (RUNNING, SUCCEEDED, FAILED)                          │
│  - error_message                                                │
│  - processed_records                                            │
│  - glue_job_run_ids                                             │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### End-to-End Processing Flow

```
1. Trigger Event
   │
   ├─ S3 Object Created
   ├─ Scheduled Cron
   └─ Step Function Completion
   │
   ▼
2. EventBridge Rule
   │
   ├─ Match event pattern
   ├─ Transform input
   └─ Target Step Function
   │
   ▼
3. Step Function Execution
   │
   ├─ Initialize (write to audit table)
   ├─ For each table:
   │  ├─ Start Stage Glue Job
   │  ├─ Wait for completion
   │  ├─ Start Curated Glue Job
   │  └─ Wait for completion
   └─ Publish success to SNS
   │
   ▼
4. Stage Glue Job (Bronze)
   │
   ├─ Load config from S3
   ├─ Read from source (DynamoDB/S3)
   ├─ Apply minimal transformations
   ├─ Write to Iceberg table (raw bucket)
   └─ Update Glue catalog
   │
   ▼
5. Curated Glue Job (Silver)
   │
   ├─ Load config from S3
   ├─ Read from Bronze Iceberg table
   ├─ Apply transformations:
   │  ├─ Type conversions
   │  ├─ Value normalization
   │  ├─ Deduplication
   │  └─ Business logic
   ├─ Write to Iceberg table (curated bucket)
   └─ Update Glue catalog
   │
   ▼
6. Semantic View Creation (Gold)
   │
   ├─ Lambda triggered by SNS
   ├─ Create Athena views
   └─ Update semantic database
   │
   ▼
7. Downstream Integration
   │
   ├─ Lambda triggered by SNS
   ├─ Load downstream config
   ├─ Match completed tables
   └─ Trigger downstream services:
      ├─ Start Step Functions
      ├─ Invoke Lambda functions
      └─ Publish to SNS topics
```

## Configuration Architecture

### YAML-Based Configuration

```
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Hierarchy                       │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Environment Level (tf/tst/qa/rt)             │  │
│  │                                                            │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │           Pipeline Configurations                   │  │  │
│  │  │  - Source definitions                               │  │  │
│  │  │  - Transformation rules                             │  │  │
│  │  │  - Target specifications                            │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │           EventBridge Rules                         │  │  │
│  │  │  - Trigger patterns                                 │  │  │
│  │  │  - Schedule expressions                             │  │  │
│  │  │  - Target configurations                            │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │         Downstream Configurations                   │  │  │
│  │  │  - Service mappings                                 │  │  │
│  │  │  - Trigger conditions                               │  │  │
│  │  │  - Parameters                                       │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │            Notification Settings                    │  │  │
│  │  │  - SNS subscriptions                                │  │  │
│  │  │  - Alert configurations                             │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration Loading Flow

```
Terraform Deployment
   │
   ├─ Read YAML configs
   ├─ Create EventBridge rules
   ├─ Create SNS subscriptions
   └─ Upload configs to S3
   │
   ▼
Runtime Execution
   │
   ├─ Glue Job starts
   ├─ Load config from S3
   ├─ Parse YAML
   ├─ Apply transformations
   └─ Write output
```

## Security Architecture

### IAM Roles & Permissions

```
┌─────────────────────────────────────────────────────────────────┐
│                      IAM Role Structure                          │
│                                                                  │
│  Step Function Role                                             │
│  ├─ Start Glue jobs                                             │
│  ├─ Write to DynamoDB audit table                               │
│  ├─ Publish to SNS topics                                       │
│  └─ Write to CloudWatch Logs                                    │
│                                                                  │
│  Glue Job Role                                                  │
│  ├─ Read from DynamoDB tables                                   │
│  ├─ Read/Write S3 buckets                                       │
│  ├─ Update Glue catalog                                         │
│  ├─ Read KMS keys                                               │
│  └─ Write to CloudWatch Logs                                    │
│                                                                  │
│  Lambda Execution Role                                          │
│  ├─ Read from S3 config bucket                                  │
│  ├─ Start Step Functions                                        │
│  ├─ Invoke Lambda functions                                     │
│  ├─ Publish to SNS/SQS                                          │
│  └─ Write to CloudWatch Logs                                    │
│                                                                  │
│  EventBridge Role                                               │
│  ├─ Start Step Functions                                        │
│  └─ Pass execution role                                         │
└─────────────────────────────────────────────────────────────────┘
```

### Encryption

- **S3**: Server-side encryption with KMS
- **DynamoDB**: Encryption at rest with KMS
- **SNS**: KMS encryption for messages
- **CloudWatch Logs**: KMS encryption

## Scalability & Performance

### Horizontal Scaling

- **Glue Jobs**: Auto-scaling workers (2-10 workers)
- **Lambda**: Concurrent executions (up to 1000)
- **Step Functions**: Parallel map state for multiple tables
- **EventBridge**: Unlimited event throughput

### Performance Optimization

1. **Partitioning**: Data partitioned by date for efficient queries
2. **Iceberg**: Optimized file formats and metadata management
3. **Incremental Processing**: Only process changed data
4. **Parallel Execution**: Multiple tables processed simultaneously
5. **Caching**: Glue catalog caching for metadata

### Cost Optimization

- **Glue**: Use G.1X workers, enable job bookmarks
- **S3**: Lifecycle policies for old data
- **Lambda**: Right-sized memory allocation
- **Step Functions**: Minimize state transitions
- **Iceberg**: Compaction to reduce file count

## Disaster Recovery

### Backup Strategy

- **S3**: Versioning enabled, cross-region replication
- **DynamoDB**: Point-in-time recovery enabled
- **Glue Catalog**: Backed up via AWS Backup
- **Terraform State**: Remote backend with versioning

### Recovery Procedures

1. **Data Loss**: Restore from S3 versioning or backups
2. **Infrastructure Failure**: Redeploy via Terraform
3. **Configuration Loss**: Restore from Git repository
4. **Execution Failure**: Retry from Step Function

## Monitoring & Observability

### Key Metrics

- **Step Function**: Execution duration, success rate
- **Glue Jobs**: Job duration, DPU hours, records processed
- **Lambda**: Invocation count, duration, errors
- **S3**: Object count, storage size
- **DynamoDB**: Read/write capacity, throttles

### Alerting

- **CloudWatch Alarms**: For metric thresholds
- **SNS Notifications**: For pipeline failures
- **EventBridge**: For event-driven alerts

## Future Enhancements

1. **Real-time Streaming**: Add Kinesis/Kafka support
2. **Data Quality Framework**: Advanced validation rules
3. **Lineage Tracking**: End-to-end data lineage
4. **Self-Service UI**: Web interface for configuration
5. **ML Integration**: Automated data profiling
6. **Multi-Region**: Active-active deployment
7. **Cost Analytics**: Detailed cost tracking per pipeline
