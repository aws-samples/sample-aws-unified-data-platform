# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Lambda function to trigger downstream services based on completed table names.
"""

import json
import logging
import os
import re

import boto3
import yaml

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_config_from_s3(s3_client, bucket, key):
    """Download and parse config file from S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    if key.endswith(".yaml") or key.endswith(".yml"):
        return yaml.safe_load(content)
    else:
        return json.loads(content)


def _trigger_downstream_service(clients, entry, completed_tables, udm_data=None):
    """Trigger downstream service if all required tables are completed."""
    tables_list = set(entry["tables_list"])

    # Check if all required tables are completed
    # Example:
    #   completed_tables = {"table1", "table2", "table3"} (3 tables completed)
    #   tables_list = {"table1", "table2"} (downstream config requires 2 tables)
    #   Result: {"table1", "table2"}.issubset({"table1", "table2", "table3"}) = True
    #   Action: Trigger downstream service (all required tables available)
    if not tables_list.issubset(completed_tables):
        return None

    service_type = entry.get("service_type", "step_function")
    service_arn = entry.get("service_arn", entry.get("step_function_arn"))
    execution_input = {
        "completed_tables": list(completed_tables),
        **entry.get("additional_parameters", {}),
    }

    # Add UDM data if available
    if udm_data:
        execution_input.update(udm_data)

    if service_type == "step_function":
        response = clients["stepfunctions"].start_execution(
            stateMachineArn=service_arn, input=json.dumps(execution_input)
        )
        logger.info("Triggered Step Function: %s", service_arn)
        return {
            "service_type": "step_function",
            "service_arn": service_arn,
            "execution_arn": response["executionArn"],
            "matched_tables": list(tables_list),
        }

    elif service_type == "lambda":
        response = clients["lambda"].invoke(
            FunctionName=service_arn,
            InvocationType="Event",
            Payload=json.dumps(execution_input),
        )
        logger.info("Triggered Lambda: %s", service_arn)
        return {
            "service_type": "lambda",
            "service_arn": service_arn,
            "status_code": response["StatusCode"],
            "matched_tables": list(tables_list),
        }

    elif service_type == "sns":
        response = clients["sns"].publish(
            TopicArn=service_arn, Message=json.dumps(execution_input)
        )
        logger.info("Published to SNS: %s", service_arn)
        return {
            "service_type": "sns",
            "service_arn": service_arn,
            "message_id": response["MessageId"],
            "matched_tables": list(tables_list),
        }

    elif service_type == "sqs":
        response = clients["sqs"].send_message(
            QueueUrl=service_arn, MessageBody=json.dumps(execution_input)
        )
        logger.info("Sent message to SQS: %s", service_arn)
        return {
            "service_type": "sqs",
            "service_arn": service_arn,
            "message_id": response["MessageId"],
            "matched_tables": list(tables_list),
        }

    else:
        logger.error("Unsupported service type: %s", service_type)
        return None


def lambda_handler(event, _context):
    """
    Triggers downstream services based on completed table names and config file.
    Handles both SNS events and direct invocation.
    """
    logger.info("Event: %s", event)
    try:
        # Initialize AWS clients
        clients = {
            "s3": boto3.client("s3"),
            "stepfunctions": boto3.client("stepfunctions"),
            "lambda": boto3.client("lambda"),
            "sns": boto3.client("sns"),
            "sqs": boto3.client("sqs"),
        }

        # Extract completed tables and UDM data from SNS event or direct event
        udm_data = {}
        if "Records" in event and event["Records"][0].get("EventSource") == "aws:sns":
            sns_message = event["Records"][0]["Sns"]["Message"]
            logger.info("SNS Message: %s", sns_message)

            # Look for JSON array after "Processed Tables:"
            # Use a more specific regex to capture the JSON array properly
            match = re.search(r"Processed Tables:\s*(\[[^\]]+\])", sns_message)
            if match:
                json_str = match.group(1).strip()
                logger.info("Found JSON string: %s", json_str)
                try:
                    tables_list = json.loads(json_str)
                    completed_tables = set(tables_list)
                    logger.info("Parsed tables: %s", completed_tables)
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse JSON: %s", e)
                    raise ValueError(f"Failed to parse processed tables JSON: {e}") from e
            else:
                logger.error("No processed tables found in SNS message")
                raise ValueError("No processed tables found in SNS message")

            # Extract UDM incremental data
            udm_column_match = re.search(
                r"udm_incremental_column:\s*(\w+)", sns_message
            )
            udm_value_match = re.search(
                r"udm_incremental_value:\s*([^\s\n]+)", sns_message
            )

            if udm_column_match:
                udm_data["udm_incremental_column"] = udm_column_match.group(1)
            if udm_value_match:
                udm_data["udm_incremental_value"] = udm_value_match.group(1)

            logger.info("Extracted UDM data: %s", udm_data)
        else:
            completed_tables = set(event["completed_tables"])

        # Use environment variables as fallback
        s3_bucket = event.get("s3_bucket") or os.environ.get("CONFIG_S3_BUCKET")
        config_file = event.get("config_file") or os.environ.get("CONFIG_FILE_PATH")

        if not s3_bucket or not config_file:
            raise ValueError("S3 bucket and config file path must be provided")

        config = _get_config_from_s3(clients["s3"], s3_bucket, config_file)

        # Handle both old and new config formats
        config_entries = config.get("downstream_configs", config)

        triggered_services = []
        for entry in config_entries:
            result = _trigger_downstream_service(
                clients, entry, completed_tables, udm_data
            )
            if result:
                triggered_services.append(result)

        return {
            "statusCode": 200,
            "body": {
                "triggered_services": triggered_services,
                "total_triggered": len(triggered_services),
            },
        }

    except (
        KeyError,
        json.JSONDecodeError,
        yaml.YAMLError,
        boto3.exceptions.Boto3Error,
    ) as e:
        logger.error("Error processing request: %s", str(e))
        return {"statusCode": 500, "body": {"error": str(e)}}
