# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import os
import sys
import traceback
import uuid
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Union

import boto3
from aws_lambda_powertools import Logger


class UdmMonitoringHandler:
    """
    Handler class for UDM monitoring and alerting functionality.
    """

    def __init__(self, logger: Logger, config: SimpleNamespace):
        """
        Initialize the UdmMonitoringHandler.

        Args:
            logger: Logger instance for logging
            config: Configuration object with application settings
        """
        self.config = config
        self.logger = logger

        self.runtime_environment = self._get_runtime_environment()

        self.application = "UDM"
        self.environment = getattr(config, "env", "Environment not found")
        self.account_id = self._get_account_id()
        self.region = self._get_region()
        self.resource = self._get_resource_name()
        self.workstream = self._get_resource_workstream()

    def send_alert(
        self,
        header: str,
        body: Union[str, Dict[str, Any]],
        severity: int,
        additional_info: Dict[str, Any] = None,
    ):
        """
        Send an alert with the provided alert data.

        Args:
            header: Alert header/title
            body: Alert body/description (can be string or dictionary)
            severity: Alert severity level
            additional_info: Optional dictionary with additional information

        Raises:
            TypeError: If any input parameter has an incorrect type
        """
        if additional_info is None:
            additional_info = {}
        try:
            # Validate input types
            if not isinstance(header, str):
                raise TypeError(
                    "Header must be a string, got type " f"'{type(header).__name__}'"
                )

            if not isinstance(body, (str, dict)):
                raise TypeError(
                    "Body must be a string or dictionary, got type "
                    f"'{type(body).__name__}'"
                )

            if not isinstance(severity, int) or not 1 <= severity <= 5:
                raise ValueError(
                    "Severity must be an integer between 1 and 5, got " f"{severity}"
                )

            if not isinstance(additional_info, dict):
                raise TypeError(
                    "Additional info must be a dictionary, got type "
                    f"'{type(additional_info).__name__}'"
                )

            # Create alert data
            alert_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

            queue_name = self.config.foundation.alert_queue
            queue_url = (
                f"https://sqs.{self.region}.amazonaws.com/"
                f"{self.account_id}/{queue_name}"
            )

            # Convert body to string if it's a dictionary
            if isinstance(body, dict):
                body_str = json.dumps(body)
            else:
                body_str = body

            # Compile alert data
            alert = {
                "alert_id": alert_id,
                "severity": severity,
                "environment": self.environment,
                "region": self.region,
                "timestamp": timestamp,
                "header": header,
                "body": body_str,
                "application": self.application,
                "workstream": self.workstream,
                "resource": self.resource,
                "additional_info": additional_info,
            }

            # If local, only print alert
            self.logger.info({f"Sending alert to queue {queue_name}": alert})

            # If local, don't send alert to queue
            if self.runtime_environment == "LOCAL":
                self.logger.info(
                    f"Did not send alert '{alert.get('alert_id')}' to queue "
                    f"'{queue_name}' because this code is locally executed"
                )
                return

            try:
                sqs_client = boto3.client("sqs")
                sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(alert),
                )
            except Exception as e:
                self.logger.error(
                    f"Error sending alert to queue: {e}. Traceback: "
                    f"{traceback.format_exc()}"
                )
                raise e

        except Exception as e:
            self.logger.error(
                f"Error sending alert: {e}. Traceback: {traceback.format_exc()}"
            )
            raise e

    def _get_runtime_environment(self):
        """
        Get the runtime environment from the environment.
        """
        # Lambda
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
            return "LAMBDA"
        if "USE_PROXY" in os.environ:
            return "GLUE"
        return "LOCAL"

    def _get_account_id(self):
        """
        Get the account ID from the environment.
        """
        sts_client = boto3.client("sts")
        account_id = sts_client.get_caller_identity().get(
            "Account", "ACCOUNT_ID_NOT_FOUND"
        )
        return account_id

    def _get_region(self):
        if self.runtime_environment == "LAMBDA":
            return os.environ.get("AWS_REGION", "REGION_NOT_FOUND")
        if self.runtime_environment == "GLUE":
            return os.environ.get("AWS_DEFAULT_REGION", "REGION_NOT_FOUND")
        return "Local"

    def _get_resource_name(self):
        """
        Get resource information from the environment.
        """

        if self.runtime_environment == "LAMBDA":
            lambda_function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
            return lambda_function_name

        if self.runtime_environment == "GLUE":
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

            self.logger.error("No resource name found for glue job")
            return "GLUE_JOB_NAME_NOT_FOUND"

        return "Local"

    def _get_resource_workstream(self):
        """
        Get resource workstream from the resource's tags.
        """
        if self.runtime_environment == "LAMBDA":
            # Get Workstream tag from lambda function
            # TODO: Implement actual gathering of workstream
            lambda_client = boto3.client("lambda")
            lambda_function_arn = (
                f"arn:aws:lambda:{self.region}:"
                f"{self.account_id}:function:{self.resource}"
            )
            tags = lambda_client.list_tags(Resource=lambda_function_arn)
            workstream = tags.get("Tags", {}).get("Workstream", "WORKSTREAM_NOT_FOUND")
            if workstream == "WORKSTREAM_NOT_FOUND":
                self.logger.warning(
                    f"Workstream tag not found for lambda function {self.resource}"
                )
            return workstream

        if self.runtime_environment == "GLUE":
            # TODO: Implement actual gathering of workstream
            try:
                glue_client = boto3.client("glue")
                glue_job_arn = (
                    f"arn:aws:glue:{self.region}:{self.account_id}:job/{self.resource}"
                )
                glue_job_tags = glue_client.get_tags(ResourceArn=glue_job_arn)
                workstream_tag = glue_job_tags.get("Tags", {}).get(
                    "Workstream", "WORKSTREAM_NOT_FOUND"
                )
                if workstream_tag == "WORKSTREAM_NOT_FOUND":
                    self.logger.warning(
                        f"Workstream tag not found for glue job {self.resource}"
                    )
                return workstream_tag
            except Exception as e:
                self.logger.error(
                    f"Error getting workstream tag for glue job {self.resource}: "
                    f"{e}. Traceback: {traceback.format_exc()}"
                )
                return "WORKSTREAM_NOT_FOUND"

        return "Local"
