# Artifacts bucket for Glue scripts, Lambda packages, etc.
module "artifacts_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.2.2"

  bucket        = local.artifacts_bucket
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
    Name        = local.artifacts_bucket
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}
