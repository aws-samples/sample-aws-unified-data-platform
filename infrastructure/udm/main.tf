module "UDM" {
  source = "./components"

  region     = var.region
  env_suffix = var.env_suffix
}

provider "aws" {
  region = var.region

  retry_mode  = "adaptive"
  max_retries = 10

  default_tags {
    tags = {
      Project     = "UDM"
      Environment = lower(var.env_suffix)
      Workstream  = "UDM"
    }
  }

}