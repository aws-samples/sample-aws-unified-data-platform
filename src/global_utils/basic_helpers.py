# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import ast
import re
from difflib import SequenceMatcher
from pathlib import PurePosixPath
from typing import Callable

from .logger import get_logger

logger = get_logger(__name__)


def evaluate_lambda_string(lambda_str: str) -> Callable:
    """
    Safely evaluate a lambda function from a string.

    Args:
        lambda_str (str): String representation of a lambda function
                         e.g. "lambda x: x['trade_id'] == 'MGLPRTSL01'"

    Returns:
        Callable: The evaluated lambda function

    Raises:
        ValueError: If the string is not a valid lambda function
        SyntaxError: If there's a syntax error in the lambda string
    """
    if not lambda_str.strip().startswith("lambda"):
        raise ValueError("String must be a lambda function starting with 'lambda'")

    # Parse the lambda string to ensure it's safe
    try:
        parsed = ast.parse(lambda_str.strip())
        if not isinstance(parsed.body[0].value, ast.Lambda):
            raise ValueError("String must contain only a lambda expression")

        # Evaluate the lambda string
        return ast.literal_eval(lambda_str)
    except SyntaxError as e:
        raise SyntaxError(f"Invalid lambda syntax: {e}") from e


def s3_path_to_bucket_prefix(s3_prefix: str) -> tuple[str, str]:
    """
    Extracts the bucket name and key (prefix) from an S3 path.

    Args:
        s3_prefix (str): The full S3 path, in the format 's3://bucket-name/path/...'

    Returns:
        tuple[str, str]: A tuple containing the bucket name and the key (prefix).

    Raises:
        ValueError: If the input `s3_prefix` is not a valid S3 path.

    Examples:
        >>> s3_path_to_bucket_prefix('s3://my-bucket/data/files.csv')
        ('my-bucket', 'data/files.csv')
        >>> s3_path_to_bucket_prefix('s3://another-bucket/some/more/files')
        ('another-bucket', 'some/more/files')
        >>> s3_path_to_bucket_prefix('invalid-s3-path')
        Traceback (most recent call last):
        ...
        ValueError: Invalid S3 path: 'invalid-s3-path'
    """
    s3_prefix_pattern = r"^s3://([^/]+)/(.+)$"
    match = re.match(s3_prefix_pattern, s3_prefix)
    if not match:
        raise ValueError(f"Invalid S3 path: '{s3_prefix}'")
    bucket = match.group(1)
    key = match.group(2)
    return bucket, key


def build_s3_uri(base: str, tail: str) -> str:
    """
    Constructs a well-formed S3 URI by safely joining a base S3 path
    and a tail segment.

    This function ensures there is no duplication of the 's3://' prefix and correctly
    joins path segments using POSIX rules (useful for S3 URIs).

    Args:
        base (str): The base S3 URI or path (e.g., "s3://my-bucket/my-prefix").
        tail (str): The additional path segment to append (e.g., "my-folder/my-file").

    Returns:
        str: A complete, properly formatted S3 URI.

    Example:
        build_s3_uri("s3://my-bucket/data", "2025/05/file.parquet")
        # returns: "s3://my-bucket/data/2025/05/file.parquet"
    """
    s3_base = base.replace("s3://", "")
    s3_path = PurePosixPath(s3_base) / tail
    output_path = f"s3://{s3_path}"
    return output_path


def string_contains(string_a: str, string_b: str, case_sensitive: bool = False) -> bool:
    """
    Check if string_a contains substring string_b.

    Parameters:
        string_a (str): The main string to search in.
        string_b (str): The substring to search for.
        case_sensitive (bool): Whether the search should be case-sensitive.
                             Default is False.

    Returns:
        bool: True if string_a contains string_b, False otherwise.
    """
    if not isinstance(string_a, str) or not isinstance(string_b, str):
        return False

    if case_sensitive:
        return string_b in string_a
    else:
        return string_b.lower() in string_a.lower()


def string_similarity(string_a: str, string_b: str) -> float:
    """
    Calculate the similarity between two strings using a simple ratio.
    Higher values indicate more similarity (0.0 to 1.0).

    Parameters:
        string_a (str): First string to compare.
        string_b (str): Second string to compare.

    Returns:
        float: Similarity score between 0.0 (completely different) and 1.0 (identical).
    """
    if not isinstance(string_a, str) or not isinstance(string_b, str):
        return 0.0

    return SequenceMatcher(None, string_a.lower(), string_b.lower()).ratio()
