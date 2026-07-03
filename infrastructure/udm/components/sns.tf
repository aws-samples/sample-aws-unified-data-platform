# Load notification configuration
locals {
  notifications_config = yamldecode(file("${path.module}/../config/notifications/${var.env_suffix}/notifications.yaml"))
  sns_topics_config    = local.notifications_config.sns_topics
}

# KMS Key for SNS Topic Encryption
resource "aws_kms_key" "sns_encryption_key" {
  description             = "KMS key for SNS topic encryption"
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
        Sid    = "Allow SNS service"
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService" = "sns.${var.region}.amazonaws.com"
          }
        }
      },
      {
        Sid    = "AllowStepFunctionRoleUseOfTheKey"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.aws_samples_udm_pipeline_role.arn
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name        = "sns-encryption-key-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# KMS Key Alias
resource "aws_kms_alias" "sns_encryption_key_alias" {
  name          = "alias/aws-samples-udm-sns-encryption-${var.env_suffix}"
  target_key_id = aws_kms_key.sns_encryption_key.key_id
}

# Step Function Pipeline Failure SNS Topic
resource "aws_sns_topic" "aws_samples_udm_pipeline_failure" {
  name              = "aws-samples-udm-pipeline-failure-${var.env_suffix}"
  display_name      = "UDM Pipeline Failure Notifications"
  kms_master_key_id = aws_kms_key.sns_encryption_key.arn

  tags = {
    Name        = "aws-samples-udm-pipeline-failure-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# SNS Topic Policy
resource "aws_sns_topic_policy" "aws_samples_udm_pipeline_failure_policy" {
  arn = aws_sns_topic.aws_samples_udm_pipeline_failure.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.aws_samples_udm_pipeline_failure.arn
      }
    ]
  })
}

# SNS Topic Subscriptions for pipeline failure notifications
resource "aws_sns_topic_subscription" "aws_samples_udm_pipeline_failure_subscriptions" {
  for_each = {
    for idx, notification in lookup(local.sns_topics_config, "aws_samples_udm_pipeline_failure", []) :
    "failure_${idx}" => notification
  }

  topic_arn = aws_sns_topic.aws_samples_udm_pipeline_failure.arn
  protocol  = each.value.protocol
  endpoint  = each.value.endpoint
}

# Step Function Pipeline Trigger Downstream SNS topic
resource "aws_sns_topic" "aws_samples_udm_pipeline_downstream" {
  name              = "aws-samples-udm-pipeline-downstream-${var.env_suffix}"
  display_name      = "UDM Pipeline Success Notifications"
  kms_master_key_id = aws_kms_key.sns_encryption_key.arn

  tags = {
    Name        = "aws-samples-udm-pipeline-downstream-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# SNS Topic Policy
resource "aws_sns_topic_policy" "aws_samples_udm_pipeline_downstream_policy" {
  arn = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
      }
    ]
  })
}

# SNS Topic Subscriptions for pipeline downstream notifications
resource "aws_sns_topic_subscription" "aws_samples_udm_pipeline_downstream_subscriptions" {
  for_each = {
    for idx, notification in lookup(local.sns_topics_config, "aws_samples_udm_pipeline_downstream", []) :
    "downstream_${idx}" => notification
  }

  topic_arn = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
  protocol  = each.value.protocol
  endpoint  = each.value.endpoint
}

# SNS Topic Subscription to trigger downstream Lambda
resource "aws_sns_topic_subscription" "aws_samples_udm_pipeline_downstream_lambda" {
  topic_arn = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
  protocol  = "lambda"
  endpoint  = module.aws_samples_udm_trigger_downstream_lambda.lambda_function_arn
}

# Lambda permission for SNS to invoke the function
resource "aws_lambda_permission" "aws_samples_udm_trigger_downstream_sns_invoke" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = module.aws_samples_udm_trigger_downstream_lambda.lambda_function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.aws_samples_udm_pipeline_downstream.arn
}