variable "region" {
  description = "AWS region for resource deployment"
  type        = string
}

variable "env_suffix" {
  description = "Environment suffix (e.g., tf, tst, qa, rt)"
  type        = string
}

variable "artifacts_bucket" {
  description = "S3 bucket for storing artifacts (Glue scripts, Lambda packages, etc.)"
  type        = string
  default     = "" # Will be computed if not provided
}

variable "glue_security_configuration_name" {
  description = "Name of the Glue security configuration to use"
  type        = string
  default     = "" # Optional - leave empty if not using security configuration
}

