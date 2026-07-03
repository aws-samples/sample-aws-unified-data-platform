# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os

import boto3
from botocore.config import Config

from .config_loader import load_configs
from .logger import get_logger

logger = get_logger(__name__)
cfg = load_configs(os.environ.get("ENV"))

retry_config = Config(
    region_name=cfg.region, retries={"max_attempts": 10, "mode": "standard"}
)


class ClientModules:

    @staticmethod
    def create_bedrock_client():
        session = boto3.session.Session()
        bedrock_client = session.client("bedrock", config=retry_config)
        logger.info("Bedrock client created.")
        return bedrock_client

    @staticmethod
    def create_bedrock_runtime_client():
        session = boto3.session.Session()
        bedrock_runtime_client = session.client("bedrock-runtime", config=retry_config)
        logger.info("Bedrock runtime client created.")
        return bedrock_runtime_client

    @staticmethod
    def create_athena_client():
        session = boto3.session.Session()
        athena_client = session.client("athena", config=retry_config)
        logger.info("Athena client created.")
        return athena_client

    @staticmethod
    def create_s3_client():
        session = boto3.session.Session()
        s3_client = session.client("s3", config=retry_config)
        logger.info("S3 client created.")
        return s3_client
