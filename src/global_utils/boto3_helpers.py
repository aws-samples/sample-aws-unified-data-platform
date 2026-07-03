# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config_loader import load_configs
from .logger import get_logger

logger = get_logger(__name__)


cfg = load_configs(os.environ.get("ENV"))

s3 = boto3.client("s3", region_name=cfg.region)
dynamodb = boto3.resource("dynamodb", region_name=cfg.region)


def list_objects_by_filetype(bucket_name, prefix, file_extension=None):
    """
    Lists S3 object keys in a bucket under a specific prefix
    that match a file extension.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): The prefix (like a folder path) to list objects under.
        file_extension (str): File extension to filter by (e.g., ".csv").

    Returns:
        List[str]: List of object keys matching the file extension and prefix.

    Example:
        >>> list_objects_by_filetype("my-bucket", "data/exports/", ".csv")
        ['data/exports/file1.csv', 'data/exports/file2.csv']
    """
    paginator = s3.get_paginator("list_objects_v2")

    if not prefix.endswith("/"):
        prefix += "/"

    matching_keys = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if not key.endswith("/"):
                if file_extension:
                    if key.endswith(file_extension):
                        matching_keys.append(key)
                else:
                    matching_keys.append(key)

    return matching_keys


def update_dynamodb_item(table_name, key, attribute_name, new_value):
    """
    Update a single attribute of an item in a DynamoDB table.

    :param table_name: Name of the DynamoDB table
    :param key: Dictionary specifying the primary key of the item to update
    :param attribute_name: The attribute to update
    :param new_value: The new value to set for the attribute
    """
    table = dynamodb.Table(table_name)

    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=f"SET {attribute_name} = :val",
            ExpressionAttributeValues={":val": new_value},
            ReturnValues="UPDATED_NEW",
        )
        logger.info("Update successful:", response["Attributes"])
    except (BotoCoreError, ClientError) as error:
        logger.info("Error updating item:", error)
        ## should it raise error and fail?


def upload_dataframe_to_dynamodb(df, table_name):
    """
    Uploads a Pandas DataFrame to an AWS DynamoDB table.

    :param df: Pandas DataFrame to upload
    :param table_name: Name of the DynamoDB table
    """
    # Initialize a session using Boto3
    table = dynamodb.Table(table_name)

    # Iterate over DataFrame rows and insert each row into the table
    for _, row in df.iterrows():
        item = {col: row[col] for col in df.columns}
        table.put_item(Item=item)

    logger.info(
        f"Successfully uploaded {len(df)} records to DynamoDB table {table_name}"
    )
