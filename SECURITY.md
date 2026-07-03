# Security Best Practices

The following are recommended security hardening measures to consider when deploying this solution in production.

**S3 Bucket Encryption** — This solution uses customer-managed KMS keys (CMKs) for DynamoDB, SNS, and Step Function log encryption. S3 buckets currently use SSE-S3 (AES256). For production, upgrade S3 buckets to SSE-KMS with a customer-managed key for consistent encryption controls and centralized key audit.

**S3 `force_destroy` Setting** — Disable `force_destroy = true` on all S3 buckets in production to prevent accidental data loss during `terraform destroy`.

**AWS KMS Key Policy** — Replace the root account `kms:*` grant with explicit administrator principals scoped to specific IAM roles.

**Amazon CloudWatch Logs KMS Encryption** — Create explicit CloudWatch Log Groups with KMS encryption and a retention policy. This solution's logs contain operational data (table names, execution IDs, analysis summaries), no secrets or PII.

**Amazon S3 Access Logging** — Enable server access logging on all S3 buckets to track data access patterns and detect unauthorized activity.

**Amazon VPC Deployment for Lambda** — Deploy Lambda functions into a VPC with private subnets if they access VPC-bound resources. This solution accesses only AWS APIs, so VPC deployment is optional.

**Lambda Dead Letter Queues** — Configure Amazon SQS dead letter queues on Lambda functions for asynchronous invocations. In this solution, Step Functions handles retries and error states natively.

**AWS CloudTrail** — Verify that CloudTrail is enabled to capture all API calls for auditing and forensic analysis.

**AWS Config Rules** — Monitor for configuration drift and compliance violations on provisioned resources (S3 encryption, IAM policies, KMS key settings).

**DynamoDB Deletion Protection** — Enable deletion protection on the audit table in production to prevent accidental table removal.

# Dismissed Vulnerabilities (False Positives)

| CVE | Package | Reason |
|-----|---------|--------|
| CVE-2026-25087 | pyarrow (transitive) | Affects only Arrow C++ with pre-buffering via `RecordBatchFileReader::PreBufferMetadata`. The advisory confirms Python bindings are not vulnerable. pyarrow is a transitive dependency of pandas/pyspark and is not invoked directly. |

# Reporting a Vulnerability

If you discover a potential security issue in this project, we ask that you notify AWS Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.
