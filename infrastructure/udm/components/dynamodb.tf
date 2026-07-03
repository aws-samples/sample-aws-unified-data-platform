# UDM Audit Table
resource "aws_dynamodb_table" "aws_samples_udm_audit_table" {
  name         = "aws-samples-udm-audit-table-${var.env_suffix}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "stepfunction_execution_id"
  range_key    = "table_name"

  attribute {
    name = "stepfunction_execution_id"
    type = "S"
  }

  attribute {
    name = "table_name"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb_encryption.arn
  }

  tags = {
    Name        = "aws-samples-udm-audit-table-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }

  lifecycle {
    ignore_changes = [attribute, global_secondary_index]
  }
}

# KMS Key for DynamoDB Encryption
resource "aws_kms_key" "dynamodb_encryption" {
  description             = "KMS key for DynamoDB table encryption"
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
        Sid    = "Allow DynamoDB Service"
        Effect = "Allow"
        Principal = {
          Service = "dynamodb.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey",
          "kms:CreateGrant"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name        = "dynamodb-encryption-key-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

resource "aws_kms_alias" "dynamodb_encryption_alias" {
  name          = "alias/aws-samples-udm-dynamodb-${var.env_suffix}"
  target_key_id = aws_kms_key.dynamodb_encryption.key_id
}

# Currently Terraform does not support creating a GSI with multiple HASH keys.
# Therefore, we use a null_resource with a local-exec provisioner to run the AWS CLI command to create the GSI after the table is created.
resource "null_resource" "create_gsi" {
  depends_on = [aws_dynamodb_table.aws_samples_udm_audit_table]

  provisioner "local-exec" {
    command = <<-EOT
      GSI_STATUS=$(aws dynamodb describe-table \
        --region ${var.region} \
        --table-name '${aws_dynamodb_table.aws_samples_udm_audit_table.name}' \
        --query 'Table.GlobalSecondaryIndexes[?IndexName==`table_name-status-index`].IndexStatus' \
        --output text)

      if [ -z "$GSI_STATUS" ] || [ "$GSI_STATUS" = "None" ]; then
        echo "GSI does not exist. Creating..."
        if aws dynamodb update-table \
          --region ${var.region} \
          --table-name '${aws_dynamodb_table.aws_samples_udm_audit_table.name}' \
          --attribute-definitions '{"AttributeName":"table_name","AttributeType":"S"}' '{"AttributeName":"stepfunction_exec_status","AttributeType":"S"}' '{"AttributeName":"stepfunction_exec_start","AttributeType":"S"}' \
          --global-secondary-index-updates '{"Create":{"IndexName":"table_name-status-index","KeySchema":[{"AttributeName":"table_name","KeyType":"HASH"},{"AttributeName":"stepfunction_exec_status","KeyType":"HASH"},{"AttributeName":"stepfunction_exec_start","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}}'; then
          echo "GSI creation command succeeded. Waiting for GSI to be ACTIVE..."
        else
          echo "ERROR: GSI creation command failed. Exit code: $?"
          exit 1
        fi
      else
        echo "GSI already exists. Verifying status..."
      fi

      while true; do
        STATUS=$(aws dynamodb describe-table \
          --region ${var.region} \
          --table-name '${aws_dynamodb_table.aws_samples_udm_audit_table.name}' \
          --query 'Table.GlobalSecondaryIndexes[?IndexName==`table_name-status-index`].IndexStatus' \
          --output text)

        if [ "$STATUS" = "ACTIVE" ]; then
          echo "GSI is now ACTIVE"
          break
        elif [ -z "$STATUS" ] || [ "$STATUS" = "None" ]; then
          echo "GSI not found, waiting..."
        elif [ "$STATUS" = "CREATING" ]; then
          echo "GSI is being created..."
        else
          echo "GSI status: $STATUS"
        fi

        sleep 20
      done
    EOT
  }

  triggers = {
    always_run = timestamp()
  }
}