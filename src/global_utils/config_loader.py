# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# pylint: disable=C0301
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Optional

import boto3
import yaml

from . import spark_global_vars
from .logger import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Config manager class for dev and user configs"""

    def __init__(self, env: str = None, dev: bool = False):
        if env is None:
            raise ValueError("Environment must be specified")
        self.env = env
        self.dev = dev

    def is_running_locally(self):
        """Check if the code is running locally"""
        return bool(
            not os.getenv("AWS_LAMBDA_FUNCTION_NAME") and not "USE_PROXY" in os.environ
        )

    def dict_to_namespace(self, dict_obj: Dict):
        """Recursively convert dictionary to SimpleNamespace for dot notation access"""
        if not isinstance(dict_obj, dict):
            return dict_obj

        for key, value in dict_obj.items():
            if isinstance(value, dict):
                dict_obj[key] = self.dict_to_namespace(value)

        return SimpleNamespace(**dict_obj)

    @staticmethod
    def load_s3_config(config_path: str):
        """Load configuration from S3 bucket"""
        try:
            # == Parse S3 path ==
            bucket_name = config_path.split("/")[2]
            key = "/".join(config_path.split("/")[3:])

            # == Read from S3 ==
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)

            yaml_content = response["Body"].read().decode("utf-8")

            # == Parse YAML content ==
            config = yaml.safe_load(yaml_content)

            return config

        except Exception as e:
            raise Exception(f"Failed to load configuration: {str(e)}") from e

    @staticmethod
    def load_local_config(config_path: str):
        """Load configuration from local file"""
        try:
            with open(config_path, "r", encoding="UTF-8") as file:
                config = yaml.safe_load(file)

            return config

        except Exception as e:
            raise Exception(f"Failed to load configuration: {str(e)}") from e

    def load_config(self, config_path: str = None) -> SimpleNamespace:
        """Load configuration from either local file or S3 bucket"""
        try:
            if config_path:
                if config_path.startswith("s3://"):
                    # == Read from S3 ==
                    config = self.load_s3_config(config_path)
                else:
                    # == Read from local file ==
                    config = self.load_local_config(config_path)

            elif not self.is_running_locally():
                # == Get S3 path from environment variable ==
                s3_path = (
                    os.environ.get("CONFIG_S3_PATH")
                    or os.environ.get("CUSTOMER_CONFIG_S3_PATH")
                    or spark_global_vars.args_broadcast.get("CONFIG_S3_PATH")
                    or spark_global_vars.args_broadcast.get("CUSTOMER_CONFIG_S3_PATH")
                )  # in spark, we don't get environ vars on worker nodes. Need bcast
                if not s3_path:
                    raise ValueError("CONFIG_S3_PATH environment variable not set")

                config = self.load_s3_config(s3_path)

            else:
                # == default to a standard local file ==
                # in vscode, you can set PROJECT_ROOT in .env
                config_path = Path(
                    os.path.join(
                        Path(os.environ.get("PROJECT_ROOT", ".")),
                        # TODO: warning. __file__ will get confused when packages
                        # are
                        # installed better use an env variable
                        "configs",
                        self.env,
                        "config.yaml",
                    )
                )
                logger.info("Trying local config with %s", config_path)
                config = self.load_local_config(config_path)

            # == Set dev mode to restrict logging during testing ==
            config["dev"] = self.dev

            # == Convert to namespace for dot notation access ==
            cfg = self.dict_to_namespace(config)

            # == Apply runtime configs ==
            # cfg = self.runtime_configs(cfg)
            return cfg

        except Exception as e:
            raise Exception(f"Failed to load configuration: {str(e)}") from e

    # def runtime_configs(self, cfg):
    #     """Helper function to get runtime configurations"""

    #     # == Get Blueprint ARN ==
    #     client = boto3.client("bedrock-data-automation", region_name=cfg.region)
    #     response = client.list_blueprints()
    #     blueprint_arn = [
    #         blueprint["blueprintArn"]
    #         for blueprint in response["blueprints"]
    #         if blueprint["blueprintName"]
    #         == cfg.ingestion.invoice.extraction.bda.blueprint_name
    #     ]

    #     # == Set the configuration blueprint ARN ==
    #     cfg.ingestion.invoice.extraction.bda.blueprint_arn = blueprint_arn[0]

    #     return cfg


class ConfigSingleton:
    """Singleton class to provide a single configuration instance across the application"""

    _instance = None
    _config = None

    @classmethod
    def get_instance(
        cls,
        env: Optional[str] = None,
        config_path: Optional[str] = None,
        dev: bool = False,
    ):
        """Get or create the singleton instance with configuration"""
        if cls._instance is None or env is not None or config_path is not None:
            if env is None:
                env = os.environ.get("ENVIRONMENT")
            config_loader = ConfigLoader(env=env, dev=dev)
            cls._config = config_loader.load_config(config_path)
            cls._instance = cls()
        return cls._instance

    @property
    def config(self):
        """Get the configuration"""
        return self._config


def load_configs(env: str = None, config_path: str = None, dev: bool = False):
    """Helper function to get configuration"""
    return ConfigSingleton.get_instance(
        env=env, config_path=config_path, dev=dev
    ).config


if __name__ == "__main__":
    # == Example usage ==

    # == Read local config ==
    cfg = load_configs(env="tst")

    # == Read User Config ==
    user_cfg = load_configs(
        env=cfg.env,
        config_path=os.path.join(
            "s3://",
            cfg.foundation.artifacts_bucket,
            cfg.ingestion.user_config_key,
        ),
    )
    logger.info(user_cfg)
