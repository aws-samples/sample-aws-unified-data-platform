# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Configuration reader module for reading YAML configs from S3."""

from types import SimpleNamespace
from typing import Any, Dict

import boto3
import yaml


def dict_to_namespace(dict_obj: Dict) -> SimpleNamespace:
    """Recursively convert dictionary to SimpleNamespace for dot notation access."""
    if not isinstance(dict_obj, dict):
        return dict_obj

    for key, value in dict_obj.items():
        if isinstance(value, dict):
            dict_obj[key] = dict_to_namespace(value)

    return SimpleNamespace(**dict_obj)


def load_s3_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from S3 bucket."""
    try:
        # Parse S3 path
        if not config_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path format: {config_path}")

        path_parts = config_path[5:].split("/", 1)  # Remove 's3://' and split
        bucket_name = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""

        # Read from S3
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        yaml_content = response["Body"].read().decode("utf-8")

        # Parse YAML content
        config = yaml.safe_load(yaml_content)
        return config

    except Exception as e:
        raise ValueError(
            f"Failed to load configuration from {config_path}: {str(e)}"
        ) from e


def load_configs(config_file_path: str) -> SimpleNamespace:
    """Load configuration and return as SimpleNamespace for dot notation access.

    Args:
        config_file_path: Full S3 path to config file

    Returns:
        SimpleNamespace object with dot notation access to config values
    """
    # Load configuration from S3
    config = load_s3_config(config_file_path)

    # Handle empty/None config
    if config is None:
        config = {}

    # Convert to namespace for dot notation access
    return dict_to_namespace(config)
