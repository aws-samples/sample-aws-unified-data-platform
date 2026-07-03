# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Data reader module for reading from various data sources."""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    regexp_replace,
    to_timestamp,
)
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


class DataReader:
    """Class for reading data from DynamoDB and S3."""

    def __init__(self, glue_context=None):
        """Initialize DataReader with GlueContext.

        Args:
            glue_context: AWS Glue context object
        """
        if not glue_context:
            spark_context = SparkContext.getOrCreate()
            glue_context = GlueContext(spark_context)
        self.glue_context = glue_context

    def read_from_dynamodb(
        self, table_name: str, gsi_column: str = "updated_at", gsi_value=None
    ) -> DataFrame:
        """Read data from DynamoDB table with optional filtering.

        Args:
            table_name: Name of the DynamoDB table
            gsi_column: Column name for filtering
            gsi_value: Value to filter by (greater than comparison)

        Returns:
            Spark DataFrame with filtered data
        """
        connection_options = {
            "dynamodb.input.tableName": table_name,
            "dynamodb.throughput.read.percent": "0.5",
        }

        dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
            connection_type="dynamodb", connection_options=connection_options
        )

        dataframe = dynamic_frame.toDF()

        # Clean HTML entities from all string columns before any processing
        dataframe = self._clean_html_entities(dataframe)

        # Apply filter only if gsi_value is provided and column exists
        if gsi_value is not None:
            if gsi_column not in dataframe.columns:
                logger.info(
                    "[DATA_READER] Warning: Incremental column '%s' "
                    "not found in DynamoDB table '%s'. "
                    "Performing full extract instead.",
                    gsi_column,
                    table_name,
                )
                return dataframe
            return dataframe.filter(dataframe[gsi_column] > gsi_value)
        return dataframe

    def read_from_s3(
        self,
        s3_path,
        file_format: str = "parquet",
        transformation_ctx: str = None,
    ) -> DataFrame:
        """Read data from S3 path(s).

        Args:
            s3_path: S3 path or list of paths to read from
            file_format: File format (default: parquet)
            transformation_ctx: Transformation context for job bookmarks

        Returns:
            Spark DataFrame with data from S3
        """
        # Handle single path or list of paths
        paths = [s3_path] if isinstance(s3_path, str) else s3_path

        # Use helper method for format-specific reading
        return self._read_by_format(paths, file_format, transformation_ctx)

    def read_from_s3_incremental(
        self,
        s3_path: str,
        file_format: str,
        last_execution_time: str = None,
    ) -> DataFrame:
        """Read data from S3 with incremental logic.

        Args:
            s3_path: S3 path - if ends with file extension, read directly;
                    otherwise append datetime prefix for incremental read
            file_format: File format (json, csv, parquet, xlsx)
            last_execution_time: Last successful execution time for incremental read

        Returns:
            Spark DataFrame with data from S3
        """
        # Check if s3_path ends with file extension
        if s3_path.endswith(f".{file_format}"):
            logger.info("Reading single file: %s", s3_path)
            return self.read_from_s3(s3_path, file_format)

        # Get datetime prefixes based on last execution time
        if last_execution_time:
            try:
                parsed_time = datetime.fromisoformat(
                    last_execution_time.replace("Z", "+00:00")
                )
                date_prefixes = self._get_prefixes_after_datetime(s3_path, parsed_time)
                logger.info(
                    "Reading from %s date prefixes since "
                    "%s",
                    len(date_prefixes),
                    last_execution_time,
                )
            except Exception as e:
                logger.warning(
                    "Failed to filter datetime prefixes: %s. " "Reading all data.",
                    e,
                )
                old_datetime = datetime(1900, 1, 1, tzinfo=timezone.utc)
                date_prefixes = self._get_prefixes_after_datetime(s3_path, old_datetime)
        else:
            # First run - get all prefixes
            logger.info("First run - reading all data from: %s", s3_path)
            old_datetime = datetime(1900, 1, 1, tzinfo=timezone.utc)
            date_prefixes = self._get_prefixes_after_datetime(s3_path, old_datetime)
            logger.info("First run found %s prefixes", len(date_prefixes))

        if not date_prefixes:
            logger.info("No datetime prefixes found - 0 files to process")
            return None

        # Read from prefixes and add metadata
        df = self._read_by_format(date_prefixes, file_format)
        return self._add_metadata_columns(df, date_prefixes)

    def read_from_iceberg_table(
        self, database_name: str, table_name: str, partition_filter: str = None
    ) -> DataFrame:
        """Read data from Iceberg table using Spark SQL.

        Args:
            database_name: Name of the database
            table_name: Name of the Iceberg table
            partition_filter: Optional WHERE clause filter

        Returns:
            Spark DataFrame with data from Iceberg table
        """
        # Validate identifiers to prevent SQL injection
        if not re.match(r"^[a-zA-Z0-9_-]+$", database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        if not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        spark = self.glue_context.spark_session
        full_table_name = f"glue_catalog.{database_name}.{table_name}"

        if partition_filter:
            return spark.sql(
                f"SELECT * FROM {full_table_name} WHERE {partition_filter}"  # nosec B608 (False-positive due to f-string - identifiers are validated) # pylint: disable=line-too-long
            )
        else:
            return spark.table(full_table_name)

    def read_xlsx_from_s3(self, s3_path: str) -> DataFrame:
        """Read Excel (.xlsx) files from S3 using pandas.

        Args:
            s3_path: S3 path to Excel file

        Returns:
            Spark DataFrame with Excel data
        """
        import pandas as pd  # pylint: disable=import-outside-toplevel

        # Read Excel file using pandas
        pandas_df = pd.read_excel(s3_path)

        # Sanitize column names - replace special chars with underscores
        sanitized_columns = []
        for col in pandas_df.columns:
            # Replace special characters with underscores
            sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(col))
            # Remove consecutive underscores
            sanitized = re.sub(r"_+", "_", sanitized)
            # Remove leading and trailing underscores
            sanitized = sanitized.strip("_")
            sanitized_columns.append(sanitized)

        pandas_df.columns = sanitized_columns

        # Convert pandas DataFrame to Spark DataFrame
        spark = self.glue_context.spark_session
        return spark.createDataFrame(pandas_df)

    def read_from_catalog_table(
        self, database_name: str, table_name: str, partition_filter: str = None
    ) -> DataFrame:
        """Read data from Glue catalog table.

        Args:
            database_name: Name of the Glue database
            table_name: Name of the table
            partition_filter: Optional partition filter
                (e.g., "udm_processed_date >= '2024-01-01'")

        Returns:
            Spark DataFrame with data from catalog table
        """
        if partition_filter:
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name,
                push_down_predicate=partition_filter,
            )
        else:
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name, table_name=table_name
            )
        return dynamic_frame.toDF()

    def _read_by_format(
        self, paths: list, file_format: str, transformation_ctx: str = None
    ) -> DataFrame:
        """Read data from S3 paths based on file format.

        Args:
            paths: List of S3 paths to read from
            file_format: File format (json, csv, parquet, xlsx)
            transformation_ctx: Optional transformation context

        Returns:
            Spark DataFrame with data from S3
        """
        spark = self.glue_context.spark_session

        if file_format == "xlsx":
            # XLSX needs individual processing due to pandas dependency
            dataframes = []
            for path in paths:
                try:
                    temp_df = self.read_xlsx_from_s3(path)
                    dataframes.append(temp_df)
                except Exception as e:
                    logger.info("No data found in path %s: %s", path, e)
                    continue

            if dataframes:
                df = dataframes[0]
                for temp_df in dataframes[1:]:
                    df = df.union(temp_df)
                return df
            else:
                return spark.createDataFrame([])

        # Use native Spark readers for better directory handling
        if file_format == "json":
            df = (
                spark.read.option("multiline", "true")
                .option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*.json")
                .json(paths)
            )
            # Normalize column names to lowercase
            return self._normalize_column_names(df)
        elif file_format == "csv":
            df = (
                spark.read.option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*.csv")
                .csv(paths, header=True, inferSchema=True)
            )
            # Normalize column names to lowercase
            return self._normalize_column_names(df)
        elif file_format == "parquet":
            df = (
                spark.read.option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*.parquet")
                .parquet(*paths)
            )
            # Normalize column names to lowercase
            return self._normalize_column_names(df)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def _add_metadata_columns(self, df: DataFrame, date_prefixes: list) -> DataFrame:
        """Add updated_timestamp and source_file_name columns from S3 paths."""

        # Add source file name
        df = df.withColumn("source_file_name", input_file_name())

        # Extract datetime pattern from S3 path: 2025-10-13_14-38-45
        datetime_pattern = regexp_extract(
            col("source_file_name"), r"(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})", 0
        )

        # Convert to YYYY-MM-DD HH:MM:SS format
        formatted_datetime = regexp_replace(
            regexp_replace(datetime_pattern, "_", " "),
            "-([0-9]{2})-([0-9]{2})$",
            ":$1:$2",
        )

        df = df.withColumn(
            "updated_timestamp", to_timestamp(formatted_datetime, "yyyy-MM-dd HH:mm:ss")
        )

        return df

    def _get_prefixes_after_datetime(self, s3_path: str, target_datetime) -> list:
        """Get all S3 prefixes after target datetime."""

        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        s3_client = boto3.client("s3")

        try:
            logger.info("[S3_FILTER] Listing S3 bucket: %s, prefix: %s", bucket, prefix)
            response = s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/"
            )

            logger.info(
                "[S3_FILTER] Found %s "
                "prefixes",
                len(response.get("CommonPrefixes", [])),
            )

            matching_prefixes = []

            for obj in response.get("CommonPrefixes", []):
                folder_path = obj["Prefix"]
                folder_name = folder_path.rstrip("/").split("/")[-1]
                logger.info("[S3_FILTER] Processing folder: %s", folder_name)

                # Parse datetime from folder name (YYYY-MM-DD_HH-MM-SS)
                if "_" in folder_name and len(folder_name.split("_")) == 2:
                    try:
                        folder_datetime = datetime.strptime(
                            folder_name, "%Y-%m-%d_%H-%M-%S"
                        )
                        # Make folder datetime timezone-aware (UTC) to match target datetime  # pylint: disable=line-too-long
                        folder_datetime = folder_datetime.replace(tzinfo=timezone.utc)
                        logger.info(
                            "[S3_FILTER] Folder datetime: %s, "
                            "Target: %s",
                            folder_datetime,
                            target_datetime,
                        )
                        if folder_datetime > target_datetime:
                            matching_prefixes.append(f"s3://{bucket}/{folder_path}")
                            logger.info(
                                "[S3_FILTER] Added prefix: "
                                "s3://%s/%s",
                                bucket,
                                folder_path,
                            )
                        else:
                            logger.info(
                                "[S3_FILTER] Skipped prefix (older): "
                                "s3://%s/%s",
                                bucket,
                                folder_path,
                            )
                    except ValueError as ve:
                        logger.warning(
                            "[S3_FILTER] Failed to parse datetime from "
                            "%s: %s",
                            folder_name,
                            ve,
                        )
                        continue
                else:
                    logger.info(
                        "[S3_FILTER] Skipped folder (invalid format): "
                        "%s",
                        folder_name,
                    )

            logger.info("[S3_FILTER] Final matching prefixes: %s", matching_prefixes)
            return matching_prefixes

        except Exception as e:
            logger.warning("Failed to list prefixes after datetime: %s", e)
            return []

    def _clean_html_entities(self, dataframe: DataFrame) -> DataFrame:
        """Clean HTML entities from string columns to eliminate downstream decoding."""
        for field in dataframe.schema.fields:
            if isinstance(field.dataType, StringType):
                dataframe = dataframe.withColumn(
                    field.name,
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(col(field.name), "&quot;", '"'),
                                "&amp;",
                                "&",
                            ),
                            "&lt;",
                            "<",
                        ),
                        "&gt;",
                        ">",
                    ),
                )
        return dataframe

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        """Normalize column names to lowercase to avoid case sensitivity issues."""
        for old_name in df.columns:
            new_name = old_name.lower()
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        return df
