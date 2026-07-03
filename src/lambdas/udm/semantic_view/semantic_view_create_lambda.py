# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Lambda function to create semantic views based on metadata table configuration.
"""

import logging
import os

import boto3
from security_helper import validate_sql_identifier

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_s3_output_location():
    """Get S3 output location for Athena queries."""
    athena_bucket = os.environ["ATHENA_RESULTS_BUCKET"]
    return f"s3://{athena_bucket}/"


def _get_table_columns(athena_client, database_name, table_name):
    """Get column names from a table using Athena."""
    query = f"DESCRIBE {database_name}.{table_name}"

    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": _get_s3_output_location()},
    )

    query_execution_id = response["QueryExecutionId"]

    # Wait for query completion
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if status != "SUCCEEDED":
        raise Exception(f"Query failed with status: {status}")

    # Get query results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    columns = []
    for row in results["ResultSet"]["Rows"][1:]:  # Skip header
        column_name = row["Data"][0]["VarCharValue"]
        # Clean column name by removing tabs and extra spaces, stop at partition info
        if column_name and not column_name.startswith("#") and column_name.strip():
            clean_name = column_name.split("\t")[0].strip()
            if clean_name:
                columns.append(clean_name)

    return columns


def _get_metadata_config(athena_client, metadata_db, metadata_table):
    """
    Get metadata configuration from metadata table.

    Example Usage:
        If transformation column contains: UPPER({field_name})

        Generated SQL becomes: UPPER(field_name) AS synonym_alias

        If no transformation: field_name AS synonym_alias
    """
    logger.info("Getting metadata config from %s.%s", metadata_db, metadata_table)

    try:
        # First check if transformation column exists
        describe_query = f"DESCRIBE {metadata_db}.{metadata_table}"
        logger.info("Executing describe query: %s", describe_query)

        response = athena_client.start_query_execution(
            QueryString=describe_query,
            ResultConfiguration={"OutputLocation": _get_s3_output_location()},
        )

        query_execution_id = response["QueryExecutionId"]
        logger.info("Describe query execution ID: %s", query_execution_id)

        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

        if status != "SUCCEEDED":
            error_reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            logger.error(
                "Describe query failed with status: %s, reason: %s",
                status,
                error_reason,
            )
            raise Exception(f"Describe query failed with status: {status}")

        # Get table schema
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        logger.info("Successfully retrieved table schema")

        # Check if transformation column exists
        has_transformation = any(
            row["Data"][0].get("VarCharValue", "").lower() == "transformation"
            for row in results["ResultSet"]["Rows"][1:]
        )
        logger.info("Transformation column exists: %s", has_transformation)

        # Validate identifiers to prevent SQL injection
        safe_metadata_db = validate_sql_identifier(metadata_db, "metadata_db")
        safe_metadata_table = validate_sql_identifier(metadata_table, "metadata_table")

        # Build query based on column availability
        # Identifiers validated above to prevent SQL injection
        if has_transformation:
            query = f"""
                SELECT
                    field_name,
                    synonyms,
                    data_type,
                    transformation
                FROM {safe_metadata_db}.{safe_metadata_table}
                WHERE field_name IS NOT NULL
            """  # nosec B608 (False-positive due to f-string - identifiers are validated) # pylint: disable=line-too-long
        else:
            query = f"""
                SELECT
                    field_name,
                    synonyms,
                    data_type
                FROM {safe_metadata_db}.{safe_metadata_table}
                WHERE field_name IS NOT NULL
            """  # nosec B608 (False-positive due to f-string - identifiers are validated) # pylint: disable=line-too-long

        logger.info("Executing metadata query: %s", query)
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": _get_s3_output_location()},
        )

        query_execution_id = response["QueryExecutionId"]
        logger.info("Metadata query execution ID: %s", query_execution_id)

        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

        if status != "SUCCEEDED":
            error_reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            logger.error(
                "Metadata query failed with status: %s, reason: %s",
                status,
                error_reason,
            )
            raise Exception(f"Metadata query failed with status: {status}")

        # Get query results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        logger.info("Successfully retrieved metadata query results")

        metadata_config = {}
        for row in results["ResultSet"]["Rows"][1:]:  # Skip header
            field_name = row["Data"][0].get("VarCharValue", "")
            synonyms = row["Data"][1].get("VarCharValue", "")
            data_type = row["Data"][2].get("VarCharValue", "")
            transformation = (
                row["Data"][3].get("VarCharValue", "")
                if has_transformation and len(row["Data"]) > 3
                else ""
            )

            if field_name:
                metadata_config[field_name] = {
                    "synonyms": synonyms if synonyms else field_name,
                    "data_type": data_type,
                    "transformation": transformation,
                }

        logger.info("Loaded %s metadata configurations", len(metadata_config))
        return metadata_config

    except Exception as e:
        logger.error("Error getting metadata config: %s", str(e))
        raise


def _create_view_sql(
    source_db, source_table, target_view, metadata_config, source_columns
):
    """Generate CREATE VIEW SQL statement."""
    try:
        logger.info("Creating view SQL for %s", target_view)
        logger.info("Source columns: %s", source_columns)
        logger.info("Metadata config: %s", metadata_config)
        select_columns = []

        for field_name, config in metadata_config.items():
            # Normalize field name for matching
            # (lowercase, replace spaces with underscores)
            normalized_field = field_name.lower().replace(" ", "_")

            # Check if normalized field matches any source column
            matched_column = None
            if field_name in source_columns:
                matched_column = field_name
            elif normalized_field in source_columns:
                matched_column = normalized_field

            if matched_column:
                synonyms = config["synonyms"] if config["synonyms"] else field_name

                # Use synonyms as-is, taking only first value if comma-separated
                if "," in synonyms:
                    alias = synonyms.split(",")[0].strip()
                else:
                    alias = synonyms

                transformation = config.get("transformation", "")

                if transformation and transformation.strip():
                    # Apply transformation logic
                    column_expr = transformation.replace("{field_name}", matched_column)
                    select_columns.append(f'{column_expr} AS "{alias}"')
                    logger.info('Added transformed column: %s AS "%s"', column_expr, alias)
                else:
                    # No transformation, use field as-is
                    select_columns.append(f'{matched_column} AS "{alias}"')
                    logger.info('Added column: %s AS "%s"', matched_column, alias)
            else:
                logger.warning(
                    "No match found for field: %s (normalized: %s)",
                    field_name,
                    normalized_field,
                )

        if not select_columns:
            logger.error("No matching columns found between source table and metadata")
            raise Exception(
                "No matching columns found between source table and metadata"
            )

        columns_sql = ",\n    ".join(select_columns)

        # Validate identifiers to prevent SQL injection
        safe_source_db = validate_sql_identifier(source_db, "source_db")
        safe_source_table = validate_sql_identifier(source_table, "source_table")
        # target_view already contains db.table, validate both parts
        if "." in target_view:
            db_part, view_part = target_view.split(".", 1)
            safe_target_view = f"{validate_sql_identifier(db_part, 'target_db')}.{validate_sql_identifier(view_part, 'target_view')}"  # pylint: disable=line-too-long
        else:
            safe_target_view = validate_sql_identifier(target_view, "target_view")

        # Identifiers validated above to prevent SQL injection
        view_sql = f"""
            CREATE OR REPLACE VIEW {safe_target_view} AS
            SELECT
                {columns_sql}
            FROM {safe_source_db}.{safe_source_table}
        """  # nosec B608 (False-positive due to f-string - identifiers are validated)  # pylint: disable=line-too-long

        logger.info("Generated view SQL with %s columns", len(select_columns))
        return view_sql

    except Exception as e:
        logger.error("Error creating view SQL: %s", str(e))
        raise


def _execute_query(athena_client, query):
    """Execute a query using Athena."""
    try:
        logger.info("Executing query: %s...", query[:100])
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": _get_s3_output_location()},
        )

        query_execution_id = response["QueryExecutionId"]
        logger.info("Query execution ID: %s", query_execution_id)

        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

        if status != "SUCCEEDED":
            error_reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            logger.error("Query execution failed: %s", error_reason)
            raise Exception(f"Query execution failed: {error_reason}")

        logger.info("Query executed successfully")
        return query_execution_id

    except Exception as e:
        logger.error("Error executing query: %s", str(e))
        raise


def lambda_handler(event, _context):
    """
    Create semantic view based on metadata table configuration.

    Expected event structure:
    {
        "env_name": "tst",
        "metadata_db_name": "aws_samples_udm_curated_tst",
        "metadata_table_name": "contract_fields_metadata_curated",
        "semantic_db_name": "aws_samples_udm_semantic_tst",
        "target_view_name": "contract_fields_semantic_view",
        "source_db_name": "aws_samples_udm_curated_tst",
        "source_table_name": "contract_data_curated"
    }
    """
    logger.info("Event: %s", event)
    try:
        athena_client = boto3.client("athena")

        # Extract parameters with defaults
        env_name = event["env_name"]
        metadata_db_name = event.get(
            "metadata_db_name", f"aws_samples_udm_curated_{env_name}"
        )
        metadata_table_name = event["metadata_table_name"]
        semantic_db_name = event.get(
            "semantic_db_name", f"aws_samples_udm_semantic_{env_name}"
        )
        target_view_name = event["target_view_name"]
        source_db_name = event.get(
            "source_db_name", f"aws_samples_udm_curated_{env_name}"
        )
        source_table_name = event["source_table_name"]

        # Full target view name with database
        full_target_view = f"{semantic_db_name}.{target_view_name}"

        logger.info("Creating semantic view: %s", full_target_view)
        logger.info("Source: %s.%s", source_db_name, source_table_name)
        logger.info("Metadata: %s.%s", metadata_db_name, metadata_table_name)

        # Get source table columns
        source_columns = _get_table_columns(
            athena_client, source_db_name, source_table_name
        )
        logger.info("Source table columns: %s", source_columns)

        # Get metadata configuration
        metadata_config = _get_metadata_config(
            athena_client, metadata_db_name, metadata_table_name
        )
        logger.info("Metadata config loaded: %d fields", len(metadata_config))

        # Generate view SQL
        view_sql = _create_view_sql(
            source_db_name,
            source_table_name,
            full_target_view,
            metadata_config,
            source_columns,
        )

        logger.info("Generated view SQL: %s", view_sql)

        # Execute view creation
        query_execution_id = _execute_query(athena_client, view_sql)

        return {
            "statusCode": 200,
            "body": {
                "message": f"Successfully created view {full_target_view}",
                "query_execution_id": query_execution_id,
                "source_table": f"{source_db_name}.{source_table_name}",
                "metadata_table": f"{metadata_db_name}.{metadata_table_name}",
                "columns_mapped": len(
                    [f for f in metadata_config if f in source_columns]
                ),
            },
        }

    except Exception as e:
        logger.error("Error creating semantic view: %s", str(e))
        return {"statusCode": 500, "body": {"error": str(e)}}
