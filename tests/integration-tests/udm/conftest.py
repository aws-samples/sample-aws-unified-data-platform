# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os

import pytest
import yaml


@pytest.fixture(scope="session")
def udm_config():
    """Load UDM test configuration from YAML file"""
    config_file = os.path.join(os.path.dirname(__file__), "udm_config.yaml")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Get from environment variables (set by GitHub workflow)
    env = os.getenv("TEST_ENV", "qa")
    region = os.getenv("AWS_REGION", "us-east-1")
    account_id = os.getenv("AWS_ACCOUNT_ID", "111122223333")

    # Replace placeholders
    config_str = yaml.dump(config)
    config_str = config_str.replace("{env}", env)
    config_str = config_str.replace("{region}", region)
    updated_config = yaml.safe_load(config_str)

    # Add runtime values from environment variables
    updated_config["runtime"] = {
        "region": region,
        "environment": env,
        "account_id": account_id,
        "output_bucket": f"udm-curated-output-bucket-{env}",
    }

    return updated_config
