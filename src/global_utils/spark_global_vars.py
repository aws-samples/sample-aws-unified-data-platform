# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# pylint: disable=invalid-name
"""
Env variables are not set on worker units in spark --
this module is a workaround to set them through broadcast variables

Usage:

Load this module before all others

At entry script (like glue script), get env variables through input args.

Then set

from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()


args_broadcast = sc.broadcast(args)

"""

import os

from .logger import get_logger

logger = get_logger(__name__)

args_broadcast = None


def get_spark_args():
    global args_broadcast
    if args_broadcast is not None:  # args_broadcast is set at driver node
        return args_broadcast.values
    return {}


def get_env():
    env = os.environ.get("ENV") or get_spark_args().get("ENV")
    if env is None:
        logger.warning("env is None")
        raise ValueError("env is None")
    return env
