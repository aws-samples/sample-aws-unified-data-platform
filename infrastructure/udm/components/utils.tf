# Create utils.zip from utils directory (only if directory exists)
resource "terraform_data" "udm_utils_zip" {
  count = local.utils_dir_exists ? 1 : 0

  triggers_replace = {
    source_hash = sha1(join("", [for f in fileset("${path.module}/../../../src/glue/udm/utils", "**/*.py") : filesha1("${path.module}/../../../src/glue/udm/utils/${f}")]))
  }

  provisioner "local-exec" {
    command = "cd ${path.module}/../../../src/glue/udm && zip -r utils.zip utils/ -x '*__pycache__*' '*.pyc' '*.pyo' '.DS_Store'"
  }
}

# Upload utils.zip to S3 config bucket (only if directory exists)
resource "aws_s3_object" "udm_utils_zip" {
  count       = local.utils_dir_exists ? 1 : 0
  bucket      = module.udm_config_bucket.s3_bucket_id
  key         = "udm/utils/utils.zip"
  source      = "${path.module}/../../../src/glue/udm/utils.zip"
  source_hash = terraform_data.udm_utils_zip[0].triggers_replace.source_hash
  depends_on  = [terraform_data.udm_utils_zip]

  tags = {
    Name        = "udm-utils-zip-${var.env_suffix}"
    Environment = var.env_suffix
    Workstream  = "UDM"
  }
}