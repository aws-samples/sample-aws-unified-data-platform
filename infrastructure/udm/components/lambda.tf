# Lambda Functions using Terraform AWS Modules
module "aws_samples_udm_trigger_downstream_lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.14.0"

  function_name = "aws-samples-udm-downstream-trigger-${var.env_suffix}"
  description   = "Triggers downstream Step Functions based on completed UDM tables"
  runtime       = "python3.12"
  handler       = "downstream_trigger_lambda.lambda_handler"
  timeout       = 300

  source_path = "${path.module}/../../../src/lambdas/udm/downstream_trigger"

  layers = [
    module.pyyaml_layer.lambda_layer_arn
  ]

  environment_variables = {
    LOG_LEVEL        = "INFO"
    CONFIG_S3_BUCKET = module.udm_config_bucket.s3_bucket_id
    CONFIG_FILE_PATH = "udm/downstream/config.yaml"
  }

  attach_policy_statements = true
  policy_statements = {
    s3_read = {
      effect = "Allow"
      actions = [
        "s3:GetObject"
      ]
      resources = [
        "${module.udm_config_bucket.s3_bucket_arn}/*"
      ]
    }
    step_functions = {
      effect = "Allow"
      actions = [
        "states:StartExecution"
      ]
      resources = [
        "arn:aws:states:${var.region}:${data.aws_caller_identity.current.account_id}:stateMachine:*-${var.env_suffix}"
      ]
    }
    lambda_invoke = {
      effect = "Allow"
      actions = [
        "lambda:InvokeFunction"
      ]
      resources = [
        "arn:aws:lambda:${var.region}:${data.aws_caller_identity.current.account_id}:function:*-${var.env_suffix}"
      ]
    }
  }

  tags = {
    Name        = "aws-samples-udm-downstream-trigger-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "aws_samples_udm_create_semantic_view_lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.14.0"

  function_name = "aws-samples-udm-semantic-view-create-${var.env_suffix}"
  description   = "Creates semantic views based on metadata table configuration"
  runtime       = "python3.12"
  handler       = "semantic_view_create_lambda.lambda_handler"
  timeout       = 600

  source_path = "${path.module}/../../../src/lambdas/udm/semantic_view"

  layers = [
    module.pyyaml_layer.lambda_layer_arn,
    module.udm_helper_layer.lambda_layer_arn
  ]

  environment_variables = {
    LOG_LEVEL             = "INFO"
    ATHENA_RESULTS_BUCKET = "athena-query-results-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
    ENVIRONMENT           = var.env_suffix
  }

  attach_policy_statements = true
  policy_statements = {
    athena = {
      effect = "Allow"
      actions = [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ]
      resources = [
        "arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
      ]
    }
    glue_databases = {
      effect = "Allow"
      actions = [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions",
        "glue:UpdateTable",
        "glue:CreateTable"
      ]
      resources = [
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/default",
        aws_glue_catalog_database.udm_curated_database.arn,
        aws_glue_catalog_database.udm_semantic_database.arn
      ]
    }
    glue_tables = {
      effect = "Allow"
      actions = [
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetPartitions"
      ]
      resources = [
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/default/*",
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_curated_database.name}/*",
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.udm_semantic_database.name}/*"
      ]
    }
    s3_athena_results = {
      effect = "Allow"
      actions = [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject"
      ]
      resources = [
        "arn:aws:s3:::athena-query-results-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}",
        "arn:aws:s3:::athena-query-results-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}/*"
      ]
    }
    s3_curated = {
      effect = "Allow"
      actions = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      resources = [
        module.udm_curated_bucket.s3_bucket_arn,
        "${module.udm_curated_bucket.s3_bucket_arn}/*"
      ]
    }
  }

  tags = {
    Name        = "aws-samples-udm-semantic-view-create-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}