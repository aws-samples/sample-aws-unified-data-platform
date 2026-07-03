# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Helper functions for AWS Glue jobs.

This module provides utility functions to simplify working with AWS Glue jobs,
including argument handling, job configuration, and common Glue operations.
"""

import os

import boto3
from awsglue.utils import getResolvedOptions

from .logger import get_logger

logger = get_logger(__name__)


def get_resolved_options_safe(args, args_list) -> dict:
    """
    Safely retrieve Glue arguments. Allows for providing optional
    arguments to Glue jobs.

    This function uses the `getResolvedOptions` function from the `awsglue.utils`
    module to retrieve the resolved arguments passed to the Glue job. It logs the
    retrieved arguments and returns them as a dictionary.

    Returns:
        dict: A dictionary containing the resolved arguments passed to the Glue job.

    Example:
        >>> import sys
        >>> args = ['JOB_NAME', 'ENV', 'CONFIG_S3_PATH']
        >>> resolved_args = glue_resolve_args(args, sys.argv)
        >>>
    """
    logger.info(">> Resolving Argument Variables: START")
    avail_args = get_avail_args(args, args_list)
    logger.info(f"Resolved Arguments: {avail_args}")
    logger.info(">> Resolving Argument Variables: COMPLETE")

    args = getResolvedOptions(args, avail_args)
    return args


def get_avail_args(args: dict, args_list: list[str]) -> list[str]:
    """
    Safely retrieve Glue arguments. Allows for providing optional
    arguments to Glue jobs.

    This function checks if all required arguments are present in the provided
    arguments list. It logs warnings for any missing arguments and returns a list
    of available arguments.

    Args:
        args_list (list): List of argument names to check for (without the '--' prefix)
        args (list): Raw command-line arguments (typically sys.argv)

    Returns:
        list: List of argument names that are available in the provided arguments

    Example:
        >>> import sys
        >>> required_args = ['JOB_NAME', 'ENV', 'CONFIG_S3_PATH']
        >>> available_args = glue_resolve_args(required_args, sys.argv)
        >>>
    """
    logger.info(">> Resolving Argument Variables: START")
    available_args_list = []
    for item in args_list:
        if "--" + item in args:
            available_args_list.append(item)
        else:
            logger.info(f"WARNING: Missing argument, {item}")
    logger.info(f"AVAILABLE arguments: {available_args_list}")
    logger.info(">> Resolving Argument Variables: COMPLETE")
    return available_args_list


def get_glue_job_run_arn(processed_args: dict, cfg) -> str:
    """
    Constructs the ARN of the currently running AWS Glue job.

    Args:
        processed_args (dict): Dictionary containing at least
        'JOB_NAME' and 'JOB_RUN_ID', typically returned by getResolvedOptions().
        cfg: Configuration object with `region` attribute.

    Returns:
        str: The full Glue job run ARN.
    """
    job_name = processed_args["JOB_NAME"]
    region = cfg.region

    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]

    # Manually extract the JOB_RUN_ID
    try:
        job_run_id = os.environ.get("JOB_RUN_ID")
        if job_run_id is None:
            job_run_id = os.environ.get("AUTO_DEBUGGING_METADATA_FILE_KEY", "").split(
                "/"
            )[3]
    except Exception as e:
        logger.error(f"Error extracting JOB_RUN_ID: {e}")
        raise RuntimeError("JOB_RUN_ID not found in job arguments") from e

    return f"arn:aws:glue:{region}:{account_id}:job/{job_name}/{job_run_id}"
