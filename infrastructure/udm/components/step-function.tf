# Step Function Parameters
locals {
  step_function_parameters = {
    config_bucket_name         = module.udm_config_bucket.s3_bucket_id
    stage_bucket_name          = module.udm_raw_bucket.s3_bucket_id
    curated_bucket_name        = module.udm_curated_bucket.s3_bucket_id
    audit_table_name           = aws_dynamodb_table.aws_samples_udm_audit_table.name
    stage_job_name             = "aws-samples-udm-stage-${var.env_suffix}"
    curated_job_name           = "aws-samples-udm-curated-${var.env_suffix}"
    downstream_sns_topic_arn   = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
    udm_pipeline_sns_topic_arn = aws_sns_topic.aws_samples_udm_pipeline_failure.arn
  }
}

# Step Function State Machine
resource "aws_sfn_state_machine" "aws_samples_udm_pipeline" {
  name     = "aws-samples-udm-pipeline-${var.env_suffix}"
  role_arn = aws_iam_role.aws_samples_udm_pipeline_role.arn

  definition = templatefile("${path.module}/../../../src/step-function-asls/udm/aws-samples-udm-pipeline-state-machine.asl.json.tpl", {
    env_name                   = var.env_suffix
    config_bucket_name         = local.step_function_parameters.config_bucket_name
    stage_bucket_name          = local.step_function_parameters.stage_bucket_name
    curated_bucket_name        = local.step_function_parameters.curated_bucket_name
    audit_table_name           = local.step_function_parameters.audit_table_name
    stage_job_name             = local.step_function_parameters.stage_job_name
    curated_job_name           = local.step_function_parameters.curated_job_name
    downstream_sns_topic_arn   = local.step_function_parameters.downstream_sns_topic_arn
    udm_pipeline_sns_topic_arn = local.step_function_parameters.udm_pipeline_sns_topic_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  tags = {
    Name        = "aws-samples-udm-pipeline-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Step Function IAM Role
resource "aws_iam_role" "aws_samples_udm_pipeline_role" {
  name = "aws-samples-udm-pipeline-role-${var.env_suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "aws-samples-udm-pipeline-role-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# CloudWatch Log Group for Step Function
resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/stepfunctions/aws-samples-udm-pipeline-${var.env_suffix}"
  retention_in_days = 365
  kms_key_id        = aws_kms_key.step_function_logs.arn

  tags = {
    Name        = "step-function-logs-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# KMS Key for Step Function Logs
resource "aws_kms_key" "step_function_logs" {
  description             = "KMS key for Step Function logs encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = [
          "kms:Describe*",
          "kms:Get*",
          "kms:List*",
          "kms:PutKeyPolicy",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:EnableKeyRotation",
          "kms:DisableKeyRotation",
          "kms:ScheduleKeyDeletion",
          "kms:CancelKeyDeletion"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow CloudWatch Logs"
        Effect = "Allow"
        Principal = {
          Service = "logs.${var.region}.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/stepfunctions/aws-samples-udm-pipeline-${var.env_suffix}"
          }
        }
      },
      {
        Sid    = "Allow Step Function Role"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.aws_samples_udm_pipeline_role.arn
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name        = "step-function-logs-key-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# KMS Key Alias for Step Function Logs
resource "aws_kms_alias" "step_function_logs_alias" {
  name          = "alias/aws-samples-udm-step-function-logs-${var.env_suffix}"
  target_key_id = aws_kms_key.step_function_logs.key_id
}

# Step Function IAM Policy
resource "aws_iam_role_policy" "aws_samples_udm_pipeline_policy" {
  name = "aws-samples-udm-pipeline-policy-${var.env_suffix}"
  role = aws_iam_role.aws_samples_udm_pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:job/${local.step_function_parameters.stage_job_name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:job/${local.step_function_parameters.curated_job_name}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = aws_dynamodb_table.aws_samples_udm_audit_table.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.dynamodb_encryption.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [
          aws_sns_topic.aws_samples_udm_pipeline_failure.arn,
          aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups"
        ]
        Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          aws_cloudwatch_log_group.step_function_logs.arn,
          "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.step_function_logs.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "arn:aws:s3:::${local.step_function_parameters.config_bucket_name}/udm/pipeline/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ]
        Resource = ["*"]
      }
    ]
  })
}