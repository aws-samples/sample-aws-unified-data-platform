locals {
  # Compute artifacts bucket name if not provided
  artifacts_bucket = var.artifacts_bucket != "" ? var.artifacts_bucket : "awssamples-tfartifacts-bucket-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"

  # Glue script paths
  glue_script_s3_path  = "udm/glue"
  glue_script_src_path = "${path.module}/../../../src/glue/udm"

  # Legacy config path (for backward compatibility)
  tf_artifacts_bucket = local.artifacts_bucket
  config_s3_path      = "s3://${local.artifacts_bucket}/config.yaml"

  # Source paths
  global_utils_src_path = "${path.module}/../../../src/global_utils"
  udm_whl_src_path      = "${path.module}/../../../src/udm/udm_whl/dist"
  lambda_layer_src_path = "${path.module}/../../../src/lambda-layers/udm"

  # Check if utils directory exists
  utils_dir_exists = length(fileset("${path.module}/../../../src/glue/udm", "utils/*.py")) > 0
}