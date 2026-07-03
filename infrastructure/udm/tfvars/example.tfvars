# Example Terraform Variables for UDM Utility
# Copy this file to tfvars/tf.tfvars and customize for your environment

# AWS Region
region = "us-east-1"

# Environment suffix (tf, tst, qa, rt, prod, etc.)
env_suffix = "dev"

# Optional: Artifacts bucket for Glue scripts and Lambda packages
# If not provided, will default to: awssamples-tfartifacts-bucket-{account_id}-{region}-{env_suffix}
# artifacts_bucket = "my-custom-artifacts-bucket"

# Optional: Glue security configuration name
# Leave empty or comment out if not using Glue security configuration
# glue_security_configuration_name = "my-glue-security-config"
