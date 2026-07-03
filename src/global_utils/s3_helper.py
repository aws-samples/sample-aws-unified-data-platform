# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import re
from datetime import datetime, time
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from .logger import get_logger

logger = get_logger(__name__)


def get_object_list(s3_path: str, pattern: str = None) -> list:
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    txt_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if re.search(rf"{pattern}", key):
                txt_files.append(f"s3://{bucket}/{key}")

    return txt_files


def get_file_context(s3_path: str) -> str:
    """
    Reads and returns the content of a file from S3.

    Args:
        s3_path (str): Full S3 URI (e.g. s3://bucket/key)

    Returns:
        str: File content as string
    """
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)

    content = response["Body"].read().decode("utf-8")
    return content


def save_context_to_s3(text: str, s3_path: str):
    """
    Saves the given text to the specified S3 path.

    Args:
        text (str): The content to upload.
        s3_path (str): Full S3 path (e.g., 's3://bucket-name/folder/file.txt').

    Raises:
        ValueError: If the S3 path is invalid.
    """
    # Parse S3 path
    match = re.match(r"s3://([^/]+)/(.+)", s3_path)
    if not match:
        raise ValueError(f"Invalid S3 path: {s3_path}")

    bucket, key = match.group(1), match.group(2)

    # Upload to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))

    logger.info(f"Uploaded to s3://{bucket}/{key}")


def list_objects_by_date(bucket_name, target_date):
    """
    Lists objects in an S3 bucket that were last modified on the target date.

    Parameters:
        bucket_name (str): The name of the S3 bucket
        target_date (datetime.date): The date to filter objects by

    Returns:
        list: List of objects that were last modified on the target date
    """
    # Initialize S3 client

    s3_client = boto3.client("s3")

    # Create datetime objects for the start and end of the target date
    start_datetime = datetime.combine(target_date, time.min)
    end_datetime = datetime.combine(target_date, time.max)

    matching_objects = []
    continuation_token = None

    try:
        # Use pagination to handle large buckets
        while True:
            # Set up list_objects_v2 kwargs
            list_kwargs = {"Bucket": bucket_name, "MaxKeys": 1000}

            # Add continuation token for pagination if we have one
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            # Request objects
            response = s3_client.list_objects_v2(**list_kwargs)

            # Check if there are any objects returned
            if "Contents" in response:
                for obj in response["Contents"]:
                    last_modified = obj["LastModified"].replace(tzinfo=None)

                    # Check if the object's last modified date matches our target date
                    if start_datetime <= last_modified <= end_datetime:
                        matching_objects.append(obj)

            # Check if there are more objects to fetch
            if response.get("IsTruncated", False):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        return matching_objects

    except ClientError as e:
        logger.info(f"Error accessing S3: {e}")
        return []


def s3_object_exists(s3_path):
    """
    Check if an object exists in S3 using a full S3 URI.

    Args:
        s3_path (str): Full S3 path in the format s3://bucket-name/key

    Returns:
        bool: True if the object exists, False otherwise.
    """
    match = re.match(r"^s3://([^/]+)/(.+)$", s3_path)
    if not match:
        return False

    bucket, key = match.groups()
    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        return False
