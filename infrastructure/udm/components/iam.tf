# IAM Role for Glue Jobs using Terraform AWS Modules
module "aws_samples_udm_glue_job_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "5.48.0"

  create_role       = true
  role_name         = "aws-samples-udm-glue-job-role-${var.env_suffix}"
  role_requires_mfa = false

  trusted_role_services = [
    "glue.amazonaws.com"
  ]

  custom_role_policy_arns = []

  tags = {
    Name        = "aws-samples-udm-glue-job-role-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Attach inline policies to Glue role
resource "aws_iam_role_policy" "aws_samples_udm_glue_job_policy" {
  name = "aws-samples-udm-glue-job-policy-${var.env_suffix}"
  role = module.aws_samples_udm_glue_job_role.iam_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAndDatabases"
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:GetCatalogs",
          "glue:CreateDatabase",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetPartitions",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreateTable",
          "glue:BatchCreatePartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/default",
          aws_glue_catalog_database.udm_raw_database.arn,
          aws_glue_catalog_database.udm_curated_database.arn,
          aws_glue_catalog_database.udm_transformed_database.arn,
          aws_glue_catalog_database.udm_semantic_database.arn
        ]
      },
      {
        Sid    = "GlueTables"
        Effect = "Allow"
        Action = [
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchDeleteTable",
          "glue:BatchDeleteTableVersion",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:CreateTable",
          "glue:DeletePartition",
          "glue:DeleteTable",
          "glue:DeleteTableVersion",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:SearchTables",
          "glue:UpdatePartition",
          "glue:UpdateTable",
          "glue:GetUserDefinedFunctions"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/default/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_raw_database.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_curated_database.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_transformed_database.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_semantic_database.name}/*"
        ]
      },
      {
        Sid      = "GlueSecurityConfiguration"
        Effect   = "Allow"
        Action   = ["glue:GetSecurityConfiguration", "glue:GetSecurityConfigurations"]
        Resource = ["*"]
      },
      {
        Sid    = "DynamoDBReadAll"
        Effect = "Allow"
        Action = [
          "dynamodb:BatchGetItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:DescribeTable",
          "dynamodb:ListTables"
        ]
        Resource = [
          "arn:aws:dynamodb:${var.region}:${data.aws_caller_identity.current.account_id}:table/*-${var.env_suffix}"
        ]
      },
      {
        Sid    = "DynamoDBWriteAuditTable"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:Query"
        ]
        Resource = [
          aws_dynamodb_table.aws_samples_udm_audit_table.arn,
          "${aws_dynamodb_table.aws_samples_udm_audit_table.arn}/index/table_name-status-index"
        ]
      },
      {
        Sid    = "KMSDynamoDBEncryption"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey"
        ]
        Resource = [
          aws_kms_key.dynamodb_encryption.arn
        ]
      },
      {
        Sid    = "S3ReadModuleBuckets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          module.udm_config_bucket.s3_bucket_arn,
          "${module.udm_config_bucket.s3_bucket_arn}/*",
          module.udm_raw_bucket.s3_bucket_arn,
          "${module.udm_raw_bucket.s3_bucket_arn}/*",
          module.udm_curated_bucket.s3_bucket_arn,
          "${module.udm_curated_bucket.s3_bucket_arn}/*",
          module.udm_business_glossary_bucket.s3_bucket_arn,
          "${module.udm_business_glossary_bucket.s3_bucket_arn}/*",
          "arn:aws:s3:::${local.tf_artifacts_bucket}",
          "arn:aws:s3:::${local.tf_artifacts_bucket}/*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/*"
        ]
      },
      {
        Sid    = "S3WriteModuleBuckets"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${module.udm_config_bucket.s3_bucket_arn}/*",
          "${module.udm_raw_bucket.s3_bucket_arn}/*",
          "${module.udm_curated_bucket.s3_bucket_arn}/*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
        ]
      },
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Resource = [
          "arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
        ]
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
      },
      {
        Sid    = "GlueDataQuality"
        Effect = "Allow"
        Action = [
          "glue:PublishDataQuality",
          "glue:GetDataQualityResult",
          "glue:BatchGetDataQualityResult",
          "glue:GetDataQualityRuleRecommendationRun",
          "glue:GetDataQualityRuleset"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:dataQualityRuleset/*"
        ]
      }
    ]
  })
}