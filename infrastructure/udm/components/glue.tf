##########################################
# Stage Job Raw - Source to S3 Ingestion
##########################################

# Stage Glue Script
resource "aws_s3_object" "udm_stage_script" {
  bucket        = local.artifacts_bucket
  key           = "${local.glue_script_s3_path}/aws_samples_udm_stage.py"
  source        = "${local.glue_script_src_path}/stage/aws_samples_udm_stage.py"
  force_destroy = true
  etag          = filemd5("${local.glue_script_src_path}/stage/aws_samples_udm_stage.py")

  depends_on = [module.artifacts_bucket]
}

# Stage Glue Job
resource "aws_glue_job" "udm_stage_job" {
  name                   = local.step_function_parameters.stage_job_name
  role_arn               = module.aws_samples_udm_glue_job_role.iam_role_arn
  max_retries            = 0
  security_configuration = var.glue_security_configuration_name != "" ? var.glue_security_configuration_name : null

  worker_type       = "G.1X"
  number_of_workers = 2

  glue_version = "5.0"
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts_bucket}/${local.glue_script_s3_path}/aws_samples_udm_stage.py"
  }

  execution_property {
    max_concurrent_runs = 10
  }

  default_arguments = {
    "--extra-py-files"                   = local.utils_dir_exists ? "s3://${module.udm_config_bucket.s3_bucket_id}/udm/utils/utils.zip" : ""
    "--additional-python-modules"        = "pyyaml,boto3,aws-lambda-powertools,openpyxl"
    "--environment"                      = var.env_suffix
    "--config_path"                      = local.config_s3_path
    "--TempDir"                          = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/temp/"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${module.udm_raw_bucket.s3_bucket_id}/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }

  tags = {
    Name        = local.step_function_parameters.stage_job_name
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

###############################
# Curated Job Raw to Curation
###############################

# Curated Glue Script
resource "aws_s3_object" "udm_curated_script" {
  bucket        = local.artifacts_bucket
  key           = "${local.glue_script_s3_path}/aws_samples_udm_curated.py"
  source        = "${local.glue_script_src_path}/curation/aws_samples_udm_curated.py"
  force_destroy = true
  etag          = filemd5("${local.glue_script_src_path}/curation/aws_samples_udm_curated.py")

  depends_on = [module.artifacts_bucket]
}

# Curated Glue Job
resource "aws_glue_job" "udm_curated_job" {
  name                   = local.step_function_parameters.curated_job_name
  role_arn               = module.aws_samples_udm_glue_job_role.iam_role_arn
  max_retries            = 0
  security_configuration = var.glue_security_configuration_name != "" ? var.glue_security_configuration_name : null

  worker_type       = "G.1X"
  number_of_workers = 2

  glue_version = "5.0"
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts_bucket}/${local.glue_script_s3_path}/aws_samples_udm_curated.py"
  }

  execution_property {
    max_concurrent_runs = 10
  }

  default_arguments = {
    "--extra-py-files"                   = local.utils_dir_exists ? "s3://${module.udm_config_bucket.s3_bucket_id}/udm/utils/utils.zip" : ""
    "--additional-python-modules"        = "pyyaml,boto3,aws-lambda-powertools,openpyxl"
    "--environment"                      = var.env_suffix
    "--config_path"                      = local.config_s3_path
    "--TempDir"                          = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/temp/"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${module.udm_curated_bucket.s3_bucket_id}/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }

  tags = {
    Name        = local.step_function_parameters.curated_job_name
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

#####################################
# Raw, Curated & Semantic Databases
#####################################
# Raw Database
resource "aws_glue_catalog_database" "udm_raw_database" {
  name        = "aws_samples_udm_raw_${var.env_suffix}"
  description = "UDM Raw database for staging data"

  tags = {
    Name        = "aws_samples_udm_raw_${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Curated Database
resource "aws_glue_catalog_database" "udm_curated_database" {
  name        = "aws_samples_udm_curated_${var.env_suffix}"
  description = "UDM Curated database for processed data"

  tags = {
    Name        = "aws_samples_udm_curated_${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Transformed Database
resource "aws_glue_catalog_database" "udm_transformed_database" {
  name        = "aws_samples_udm_transformed_${var.env_suffix}"
  description = "UDM Transformed database for processed data"

  tags = {
    Name        = "aws_samples_udm_transformed_${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Semantic Database
resource "aws_glue_catalog_database" "udm_semantic_database" {
  name        = "aws_samples_udm_semantic_${var.env_suffix}"
  description = "UDM Semantic database for processed data"

  tags = {
    Name        = "aws_samples_udm_semantic_${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}