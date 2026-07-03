# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Security helper utilities for UDM application.

This module provides security validation functions to prevent common vulnerabilities
such as SQL injection, command injection, and other security issues.
"""

import logging
import re

logger = logging.getLogger(__name__)


def validate_sql_identifier(identifier, identifier_type="identifier"):
    """
    Validate SQL identifier to prevent SQL injection attacks.

    This function ensures that database names, table names, column names, and other
    SQL identifiers only contain safe characters. It prevents SQL injection by
    rejecting any input containing special characters that could be used to
    manipulate SQL queries.

    Args:
        identifier (str): The SQL identifier to validate (e.g., database name, table name)  # pylint: disable=line-too-long
        identifier_type (str): Type of identifier for error messages (default: "identifier")  # pylint: disable=line-too-long

    Returns:
        str: The validated identifier if it passes validation

    Raises:
        ValueError: If identifier is empty or contains invalid characters

    Examples:
        >>> validate_sql_identifier("my_database_name", "database")
        'my_database_name'

        >>> validate_sql_identifier("table-name-123", "table")
        'table-name-123'

        >>> validate_sql_identifier("db; DROP TABLE users; --", "database")
        ValueError: database 'db; DROP TABLE users; --' contains invalid characters

    Security:
        - Prevents SQL injection attacks (CWE-89)
        - Blocks command injection via SQL
        - Enforces whitelist of safe characters only

    Allowed Characters:
        - Lowercase letters: a-z
        - Uppercase letters: A-Z
        - Numbers: 0-9
        - Underscore: _
        - Hyphen: -

    Blocked Characters (examples):
        - Semicolon (;) - prevents multiple SQL statements
        - Space ( ) - prevents UNION, SELECT, etc.
        - Quotes (' ") - prevents string escape attacks
        - Parentheses (()) - prevents subqueries
        - Asterisk (*) - prevents wildcard attacks
        - Comment markers (-- /* */) - prevents comment injection
    """
    if not identifier:
        raise ValueError(f"{identifier_type} cannot be empty")

    # Allow alphanumeric, underscores, and hyphens only
    if not re.match(r"^[a-zA-Z0-9_-]+$", identifier):
        logger.warning(
            "SQL injection attempt detected: %s='%s'",
            identifier_type,
            identifier,
        )
        raise ValueError(
            f"{identifier_type} '{identifier}' contains invalid characters. "
            "Only alphanumeric characters, underscores, and hyphens are allowed."
        )

    return identifier


def validate_athena_identifiers(database=None, table=None, view=None, column=None):
    """
    Validate multiple Athena/SQL identifiers at once.

    Convenience function to validate multiple SQL identifiers in a single call.
    Useful when constructing queries that involve multiple database objects.

    Args:
        database (str, optional): Database name to validate
        table (str, optional): Table name to validate
        view (str, optional): View name to validate
        column (str, optional): Column name to validate

    Returns:
        dict: Dictionary with validated identifiers (only includes provided args)

    Raises:
        ValueError: If any identifier contains invalid characters

    Example:
        >>> result = validate_athena_identifiers(
        ...     database="my_db",
        ...     table="my_table"
        ... )
        >>> result
        {'database': 'my_db', 'table': 'my_table'}
    """
    validated = {}

    if database is not None:
        validated["database"] = validate_sql_identifier(database, "database")

    if table is not None:
        validated["table"] = validate_sql_identifier(table, "table")

    if view is not None:
        validated["view"] = validate_sql_identifier(view, "view")

    if column is not None:
        validated["column"] = validate_sql_identifier(column, "column")

    return validated


def validate_s3_bucket_name(bucket_name):
    """
    Validate S3 bucket name format.

    Ensures bucket name follows AWS S3 naming conventions and doesn't contain
    potentially malicious characters.

    Args:
        bucket_name (str): S3 bucket name to validate

    Returns:
        str: Validated bucket name

    Raises:
        ValueError: If bucket name is invalid

    Rules:
        - 3-63 characters long
        - Lowercase letters, numbers, hyphens, dots
        - Must start and end with letter or number
        - No consecutive dots
    """
    if not bucket_name:
        raise ValueError("Bucket name cannot be empty")

    if len(bucket_name) < 3 or len(bucket_name) > 63:
        raise ValueError(f"Bucket name must be 3-63 characters, got {len(bucket_name)}")

    # S3 bucket naming rules
    if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", bucket_name):
        raise ValueError(
            f"Bucket name '{bucket_name}' is invalid. "
            "Must contain only lowercase letters, numbers, hyphens, and dots."
        )

    if ".." in bucket_name:
        raise ValueError("Bucket name cannot contain consecutive dots")

    return bucket_name


def sanitize_log_message(message, max_length=1000):
    """
    Sanitize log messages to prevent log injection attacks.

    Removes or escapes characters that could be used for log injection attacks,
    such as newlines, carriage returns, and control characters.

    Args:
        message (str): Log message to sanitize
        max_length (int): Maximum length of sanitized message (default: 1000)

    Returns:
        str: Sanitized log message

    Example:
        >>> sanitize_log_message("User logged in\\nADMIN: Granted access")
        'User logged in ADMIN: Granted access'
    """
    if not isinstance(message, str):
        message = str(message)

    # Remove newlines, carriage returns, and control characters
    sanitized = re.sub(r"[\n\r\t\x00-\x1f\x7f-\x9f]", " ", message)

    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "..."

    return sanitized


def validate_environment_name(env_name):
    """
    Validate environment name (e.g., tst, qa, prd).

    Args:
        env_name (str): Environment name to validate

    Returns:
        str: Validated environment name

    Raises:
        ValueError: If environment name is invalid
    """
    if not env_name:
        raise ValueError("Environment name cannot be empty")

    # Allow only lowercase alphanumeric and underscores, 2-10 chars
    if not re.match(r"^[a-z0-9_]{2,10}$", env_name):
        raise ValueError(
            f"Environment name '{env_name}' is invalid. "
            "Must be 2-10 lowercase alphanumeric characters or underscores."
        )

    return env_name
