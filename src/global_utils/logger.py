# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import logging
import os
import sys
from typing import Optional

from aws_lambda_powertools import Logger as PowertoolsLogger


class CustomFormatter(logging.Formatter):
    """Formats logs to custom specifications."""

    format_template: str = "%(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    def format(self, record: logging.LogRecord):
        level = record.levelname
        message = record.getMessage()

        formatted_message = f"{level}: {message}"

        # Add stack trace if there's an exception
        if record.exc_info:
            # Get the formatted exception info (stack trace)
            stack_trace = self.formatException(record.exc_info)
            formatted_message = f"{formatted_message}\n{stack_trace}"

        # Add source file information
        formatted_message = f"{formatted_message} ({record.filename}:{record.lineno})"

        return formatted_message


class LoggerSingleton:

    _instance = None
    _logger = None

    @classmethod
    def get_logger(cls, name: str, level: str = "INFO", **kwargs):
        if cls._instance is None:
            cls._logger = init_logger(name, level, **kwargs)
            cls._instance = cls()
        return cls._instance

    @property
    def logger(self):
        """Get the logger"""
        return self._logger


def get_logger(name: str, level: str = "INFO", service: Optional[str] = None, **kwargs):
    return LoggerSingleton.get_logger(name, level, service=service, **kwargs).logger


def init_logger(
    name: str, level: str = "INFO", service: Optional[str] = None, **kwargs
) -> PowertoolsLogger:
    """
    Create and configure a PowertoolsLogger.

    Args:
        name: Logger name
        level: Log level
        service: Service name (defaults to logger name if not provided)
        sample_rate: Sample rate for logging
        **kwargs: Additional kwargs to pass to PowertoolsLogger

    Returns:
        Configured PowertoolsLogger instance
    """
    service = service or name

    logger = PowertoolsLogger(
        service=service,
        level=level,
        serialize_stacktrace=True,
        log_uncaught_exceptions=True,
        **kwargs,
    )

    # If this is running in a Lambda function
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        return logger

    # If this is running in a Glue Job
    if os.environ.get("USE_PROXY"):
        logger.append_keys(
            glue_job_name="NOT FOUND HERE",
            # get_glue_job_name(), "NOT FOUND HERE
            # - use awsglue.utils.GetResolvedOptions",
            glue_job_run_id="NOT FOUND HERE",
        )
        return logger

    # If this is running locally
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    logger.addHandler(handler)

    return logger


def inject_lambda_context(logger: PowertoolsLogger, context):
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME") and os.environ.get("AWS_REGION"):
        logger.append_keys(
            function_name=context.function_name,
            function_arn=context.invoked_function_arn,
            function_request_id=context.aws_request_id,
            function_memory_size=context.memory_limit_in_mb,
        )


def get_glue_job_name():
    """
    Get resource information from the environment.
    """
    # If it's a python shell type
    if os.environ.get("GLUE_INSTALLATION"):
        # TODO: Implement actual gathering of glue job name
        # Check the script name, as it's the same as the job name
        index = sys.argv.index("--scriptLocation")
        script_name = sys.argv[index + 1].split("/")[-1].replace(".py", "")
        return script_name

    # If it's a glue ETL type
    if os.environ.get("SPARK_HOME"):
        # TODO: Implement actual gathering of glue job name
        index = sys.argv.index("--JOB_NAME")
        # Return the next value after the argument name
        return sys.argv[index + 1]

    return "GLUE_JOB_NAME_NOT_FOUND"


def get_glue_job_run_id():
    # If it's a glue ETL type
    if os.environ.get("SPARK_HOME"):
        # TODO: Implement actual gathering of glue job name
        # Check the script name, as it's the same as the job name
        job_run_id = os.environ.get("AUTO_DEBUGGING_METADATA_FILE_KEY", "").split("/")[
            3
        ]
        return job_run_id

    # Python shell types have no method to get the job run id
    return "GLUE_JOB_RUN_ID_NOT_FOUND"
