# UDM whl
resource "aws_s3_object" "udm_whl" {
  bucket        = local.artifacts_bucket
  key           = "${local.glue_script_s3_path}/udm-0.1.0-py3-none-any.whl"
  source        = "${local.udm_whl_src_path}/udm-0.1.0-py3-none-any.whl"
  force_destroy = true
  etag          = filemd5("${local.udm_whl_src_path}/udm-0.1.0-py3-none-any.whl")

  depends_on = [module.artifacts_bucket]
}

module "udm_config_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = "aws-samples-config-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
  force_destroy = true

  attach_deny_insecure_transport_policy = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = {
    Name        = "aws-samples-config-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "udm_raw_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = "aws-samples-raw-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
  force_destroy = true

  attach_deny_insecure_transport_policy = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = {
    Name        = "aws-samples-raw-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "udm_curated_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = "aws-samples-curated-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
  force_destroy = true

  attach_deny_insecure_transport_policy = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = {
    Name        = "aws-samples-curated-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "udm_business_glossary_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = "aws-samples-business-glossary-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
  force_destroy = true

  attach_deny_insecure_transport_policy = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = {
    Name        = "aws-samples-business-glossary-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

module "udm_transformed_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = "aws-samples-transformed-${data.aws_caller_identity.current.account_id}-${var.region}-${var.env_suffix}"
  force_destroy = true

  attach_deny_insecure_transport_policy = true

  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = {
    Name        = "aws-samples-transformed-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}

# Enable EventBridge Notifications for Business Glossary Bucket
resource "aws_s3_bucket_notification" "udm_business_glossary_bucket_eventbridge" {
  bucket      = module.udm_business_glossary_bucket.s3_bucket_id
  eventbridge = true
}
