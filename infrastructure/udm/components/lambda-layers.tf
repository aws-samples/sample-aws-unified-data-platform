# Lambda Layers using Terraform AWS Modules
module "pyyaml_layer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.14.0"

  create_layer = true
  layer_name   = "aws-samples-udm-pyyaml-layer-${var.env_suffix}"
  description  = "PyYAML library for Lambda functions"

  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["x86_64"]

  source_path = [
    {
      path             = "${local.lambda_layer_src_path}/pyyaml"
      pip_requirements = true
      prefix_in_zip    = "python"
    }
  ]

  tags = {
    Name        = "aws-samples-udm-pyyaml-layer-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "udm_helper_layer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.14.0"

  create_layer = true
  layer_name   = "aws-samples-udm-helper-layer-${var.env_suffix}"
  description  = "UDM helper utilities for Lambda functions"

  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["x86_64"]

  source_path = [
    {
      path          = "${path.module}/../../../src/lambdas/udm/helper/security_helper.py"
      prefix_in_zip = "python"
    }
  ]

  tags = {
    Name        = "aws-samples-udm-helper-layer-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}
