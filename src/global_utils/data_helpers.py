# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# pylint: disable=import-outside-toplevel,invalid-name
import ast
import os
import re
from datetime import datetime
from io import StringIO
from typing import List, Literal, Optional

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

# from . import spark_global_vars
from .basic_helpers import build_s3_uri, s3_path_to_bucket_prefix
from .boto3_helpers import list_objects_by_filetype
from .config_loader import load_configs
from .logger import get_logger

logger = get_logger(__name__)


# import findspark


s3 = boto3.client("s3")
logger = get_logger(__name__)

cfg = load_configs(os.environ.get("ENV"))


def format_object_columns(df: pd.DataFrame, dtype="string") -> pd.DataFrame:
    """
    Convert only object (string-like) columns in the DataFrame to the specified dtype,
    excluding datetime and numeric columns.

    Parameters:
    df (pandas.DataFrame): The DataFrame to format.
    dtype (str): The target dtype to convert object columns to (default: "string")

    Returns:
    pandas.DataFrame: The formatted DataFrame with string columns
    converted to specified dtype.
    """
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()

    # Get columns of type 'object'
    object_columns = df.select_dtypes(include=["object"]).columns

    for col in object_columns:
        # Skip datetime columns
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        # Skip numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            continue

        try:
            df[col] = df[col].astype(pd.StringDtype())
            logger.info(f"Converting column {col} to pd.StringDtype format.")
        except Exception as e:
            logger.info(f"Could not convert column {col}: {e}")

    return df


def read_all_s3_csv_to_df(s3_path: str) -> pd.DataFrame:
    """
    Reads all CSV files in an S3 path and returns a Pandas DataFrame.

    The function assumes that the CSV file in the specified S3 path has the same schema
    and is comma-delimited.
    It also ignores any leading spaces in the header names.

    Parameters:
    s3_path (str): The S3 prefix path to where CSV files are located.

    Returns:
    pandas.DataFrame: A Pandas DataFrame containing the data from the CSV file.
    """
    # Set the bucket name and prefix
    bucket_name, prefix = s3_path_to_bucket_prefix(s3_path)
    file_keys = list_objects_by_filetype(bucket_name, prefix, ".csv")

    # Create an empty list to store the DataFrames
    dfs = []

    # Loop through the file keys and read each CSV file into a DataFrame
    for file_key in file_keys:
        # Download the file from S3 to a temporary string buffer
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        # Read the CSV file into a DataFrame, ignoring leading spaces in the header
        df = pd.read_csv(StringIO(content), skipinitialspace=True)
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all the DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


# Add more mapping types in future as needed
def record_mapper_metadata(
    partition_key: str,
    group_id: Optional[str],
    job_arn_identifier: str,
    mapping_type: Literal["model", "rule"],
    job_type: Literal["Glue", "SageMakerBatchTransform"],
):
    """Record the writer metadata.

    Args:
        log_table: The DynamoDB table to write the log to.
        partition_key: The partition key the writer used to write.
        group_id: The group ID to use; this should be provided by the customer.
        job_arn_identifier: The job ARN, which acts as the primary key in the table.
        mapping_type: The writer type, e.g., 'model', 'rule', 'Glue', or
        'SageMakerBatchTransform'.
        job_type: A string indicating the job type, e.g., 'batch', 'realtime', etc.
    """

    table_name = cfg.mapping.dynamodb.pipeline_metadata
    region = cfg.region

    dynamodb = boto3.resource("dynamodb", region_name=region)

    try:
        table = dynamodb.Table(table_name)
        # Check if table exists by calling describe
        table.load()
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info(f"Table {table_name} not found. Creating it now...")

            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "job_arn", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "job_arn", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait for the table to become active
            table.wait_until_exists()
            logger.info(f"Table {table_name} created successfully.")
        else:
            raise

    item = {
        "job_arn": job_arn_identifier,
        "partition_key": partition_key,
        "group_id": group_id if group_id else "null",
        "mapping_type": mapping_type,
        "job_type": job_type,
    }

    try:
        table.put_item(Item=item)
        logger.info(f"Metadata recorded successfully for job ARN: {job_arn_identifier}")
    except ClientError as e:
        raise RuntimeError(f"Failed to write metadata: {e}") from e


def write_partitioned_parquet_to_s3(
    df: pd.DataFrame,
    output_dir: str,
    partition_cols: Optional[List[str]] = None,
    mode: str = "overwrite_partitions",
    database: Optional[str] = None,
    table: Optional[str] = None,
    create_db_if_not_exists: bool = True,
):
    """
    Write a DataFrame to S3 in partitioned Parquet format and optionally register with
    AWS Glue.

    Args:
        df (pd.DataFrame): Data to write.
        output_dir (str): S3 URI where data should be written (starts with 's3://').
        partition_cols (List[str], optional): Columns to partition by.
        mode (str): Write mode, e.g., 'overwrite_partitions', 'append'.
        database (str, optional): Glue database name for catalog registration.
        table (str, optional): Glue table name for catalog registration.
        create_db_if_not_exists (bool): Whether to create the Glue DB if missing.

    Raises:
        ValueError: If `output_dir` is not an S3 path.
    """
    if not output_dir.startswith("s3://"):
        raise ValueError("Output directory must be an S3 path starting with 's3://'")

    partition_cols = partition_cols or []

    if create_db_if_not_exists:
        try:
            wr.catalog.create_database(name=database)
        except wr.exceptions.AlreadyExists:
            # Database already exists, ignore the error
            pass

    df = final_datatype_cleansing(df)
    wr.s3.to_parquet(
        df=df,
        path=output_dir,
        dataset=True,
        mode=mode,
        partition_cols=partition_cols,
        database=database,
        table=table,
        catalog_versioning=True,
    )


def read_df_from_dynamodb(table, filter_dict=None):
    """
    Reads a DataFrame from a DynamoDB table with optional filtering.

    Args:
        table (str): The name of the DynamoDB table to read from.
        filter_dict (dict, optional): Dictionary of key-value pairs to filter the
            results. Example: {'status': 'active', 'type': 'user'}

    Returns:
        pandas.DataFrame: A DataFrame containing the filtered data from the
            DynamoDB table.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table)

    # Build the FilterExpression and ExpressionAttributeValues
    # if filter_dict is provided
    if filter_dict:
        filter_expression = " AND ".join([f"#{k} = :{k}" for k in filter_dict.keys()])
        expression_attr_names = {f"#{k}": k for k in filter_dict.keys()}
        expression_attr_values = {f":{k}": v for k, v in filter_dict.items()}

        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeNames=expression_attr_names,
            ExpressionAttributeValues=expression_attr_values,
        )
    else:
        response = table.scan()

    data = response["Items"]

    # Handle pagination
    while "LastEvaluatedKey" in response:
        if filter_dict:
            response = table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attr_names,
                ExpressionAttributeValues=expression_attr_values,
            )
        else:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response["Items"])

    return pd.DataFrame(data)


def get_table_location(database, table, region):
    """
    Gets the S3 location of an Athena table.

    Args:
        database (str): The Athena database name
        table (str): The table name
        region (str): AWS region

    Returns:
        str: S3 location of the table
    """
    try:
        # Create Glue client
        glue_client = boto3.client("glue", region_name=region)

        # Get table information from Glue Data Catalog
        response = glue_client.get_table(DatabaseName=database, Name=table)

        # Extract the table location
        table_location = response["Table"]["StorageDescriptor"]["Location"]
        return table_location

    except Exception as e:
        logger.error(f"Error getting table location: {str(e)}")
        raise


def convert_ndarrays(obj):
    if isinstance(obj, np.ndarray):
        return [convert_ndarrays(i) for i in obj.tolist()]
    elif isinstance(obj, dict):
        return {k: convert_ndarrays(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_ndarrays(i) for i in obj]
    else:
        return obj


def convert_ndarray_to_list(val):
    if isinstance(val, np.ndarray):
        return val.tolist()
    elif pd.isna(val) or val is pd.NaT:
        return []
    else:
        return [val]


def sanitize_ndarray(row):
    row_dict = row.asDict(recursive=True)
    if "curated_terms" in row_dict:
        row_dict["curated_terms"] = convert_ndarrays(row_dict["curated_terms"])
    return Row(**row_dict)


def read_parquet_from_table_spark(
    database,
    table,
    region=cfg.region,
    filter_func=None,
    partition_col=None,
    partition_values=None,
    selected_columns=None,
):
    """
    Reads parquet data from S3 using either partition filtering or UDF filtering.

    Usage examples:
        1. Using partition filtering:
            df_partitioned = read_parquet_from_table_spark(
                database="my_database",
                table="my_table",
                partition_col="year",
                partition_values=["2023", "2024"],
            )

        2. Using boolean function:
            df_filtered = read_parquet_from_table_spark(
                database="my_database",
                table="my_table",
                filter_func=lambda row: row.value > 100,
            )

        3. No filtering:
            df_all = read_parquet_from_table_spark(
                database="my_database",
                table="my_table",
            )

    Args:
        database (str): The Athena database name.
        table (str): The table name.
        region (str): AWS region.
        filter_func (callable, optional): Function that takes a row and returns a
            boolean.
            Example: lambda row: row.year == 2023 and row.value > 100
        partition_col (str, optional): Name of the partition column (e.g., 'year').
        partition_values (list, optional): List of values to filter on (e.g.,
            ['2023', '2024']).

    Returns:
        pandas.DataFrame: Filtered DataFrame.

    Examples:
        # Using partition filtering
        df = read_parquet_from_table_spark(
            database="my_db",
            table="my_table",
            partition_col="year",
            partition_values=["2023", "2024"],
        )

        # Using boolean function
        df = read_parquet_from_table_spark(
            database="my_db",
            table="my_table",
            filter_func=lambda row: row.value > 100,
        )
    """
    spark = create_spark_session()
    try:
        # Input validation
        if filter_func and (partition_col or partition_values):
            raise ValueError("Cannot use both filter_func and partition filtering")

        if bool(partition_col) != bool(partition_values):
            raise ValueError(
                "Must provide both partition_col and partition_values or neither"
            )

        # Get the S3 location of the table
        table_location = get_table_location(database, table, region)
        if not table_location:
            ## we are dealing with a view without backedup s3 file
            logger.info("Trying view table")
            table_location = (
                build_s3_uri(
                    cfg.mapping.s3.workstream_bucket,
                    cfg.data.glue.invoices.tables.rows_and_metadata,
                )
                + "/"
            )
            logger.info(f"Reading table from {table_location}")

            session = boto3.Session(
                region_name=cfg.region,
            )
            cols = wr.athena.describe_table(
                database=database, table=table, boto3_session=session
            )
            column_exprs = []
            # print(cols["Column Name"].to_list())
            if selected_columns is None:
                cols_to_pick = cols["Column Name"].to_list()
            else:
                cols_to_pick = selected_columns
            logger.info(f" columns to pick: {cols_to_pick}")
            for col in cols_to_pick:
                if col == "invoice_type":
                    # cast invoice_type as array of strings
                    column_exprs.append(
                        "CAST(invoice_type AS ARRAY<VARCHAR>) AS invoice_type"
                    )
                elif (
                    col == "other"
                ):  # we won't parse other here. Is done in the view table.
                    cols_to_pick.remove("other")
                    continue
                else:
                    column_exprs.append(col)

            columns_sql = ", ".join(column_exprs)
            # Validate table name to prevent SQL injection - strict identifier check
            if not re.match(r"^[a-zA-Z0-9_]+$", table):
                raise ValueError(f"Invalid table name: {table}")
            # Validate column names against actual table schema (allowlist approach)
            valid_columns = set(cols["Column Name"].to_list())
            for col_expr in cols_to_pick:
                if col_expr not in valid_columns:
                    raise ValueError(
                        f"Column '{col_expr}' not found in table schema"
                    )

            query = f"SELECT {columns_sql} FROM {table}"  # nosec B608
            logger.info(query)
            df_p = wr.athena.read_sql_query(
                sql=query,
                database=database,
                boto3_session=session,
            )
            try:
                df_p["curated_terms"] = df_p["curated_terms"].apply(
                    convert_ndarray_to_list
                )
                logger.info("converted curated_terms")
            except Exception as e:
                logger.info("failing curated_terms")
                raise e

            try:
                df_p["invoice_type"] = df_p["invoice_type"].apply(
                    lambda x: [str(y) for y in x] if x is not None else x
                )
                logger.info("converted invoice_type")
            except Exception as e:
                raise e

            for col in cfg.mapping.vars.rules.bad_typed_columns_in_view_table:
                try:
                    df_p[col] = df_p[col].apply(
                        lambda x: str(x) if pd.notnull(x) else ""
                    )
                    logger.info(f"converted bad types {col}")
                except Exception as e:
                    logger.info(f"failed to convert {col}")
                    raise e
            df = spark.createDataFrame(df_p)
            if filter_func:
                if "curated_terms" in df.columns:
                    cols_to_filter = [
                        col for col in df.columns if col != "curated_terms"
                    ]
                else:
                    cols_to_filter = df.columns
                filter_udf = F.udf(filter_func, BooleanType())
                df = df.filter(filter_udf(F.struct([df[x] for x in cols_to_filter])))
            return df.toPandas()

        logger.info(f"s3 location of table '{table}' is {table_location}")
        base_path = table_location.replace("s3://", "s3a://")

        # Partition-based filtering
        if partition_col and partition_values:
            # Convert partition values to strings
            partition_values = [str(v) for v in partition_values]

            # Create partition paths
            partition_paths = [
                f"{base_path}/{partition_col}={value}" for value in partition_values
            ]

            # Read only specified partitions
            df = spark.read.parquet(*partition_paths)
            logger.info(f"Reading partitions: {partition_values}")

        # Regular read without partitioning
        else:
            df = spark.read.parquet(base_path)
            # Apply UDF filter if provided
            if filter_func:
                if "curated_terms" in df.columns:
                    cols_to_filter = [
                        col for col in df.columns if col != "curated_terms"
                    ]
                else:
                    cols_to_filter = df.columns
                logger.info("Applying UDF filter")
                filter_udf = F.udf(filter_func, BooleanType())
                df = df.filter(filter_udf(F.struct([df[x] for x in cols_to_filter])))

        return df.toPandas()

    except Exception as e:
        logger.error(f"Error reading table as s3 parquet file directly: {str(e)}")
        raise e


def create_spark_session(is_glue=False):
    """
    Creates a Spark session with proper S3 configurations
    """
    is_not_local = bool(
        os.getenv("AWS_LAMBDA_FUNCTION_NAME") or "USE_PROXY" in os.environ
    )
    logger.info(f"Running in Glue: {is_not_local}")
    if is_not_local:
        # In Glue, spark context is already created
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext

        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

    else:
        # Local development setup
        from pyspark.sql import SparkSession

        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        credentials = session.get_credentials()
        if credentials is None:
            raise Exception("No AWS credentials found")

        frozen_creds = credentials.get_frozen_credentials()

        # Define package dependencies
        packages = [
            "org.apache.hadoop:hadoop-aws:3.3.1",
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        ]

        # Create Spark session with configurations
        spark = (
            SparkSession.builder.appName("ParquetReader")
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", frozen_creds.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", frozen_creds.secret_key)
            .config("spark.hadoop.fs.s3a.session.token", frozen_creds.token)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,"
                + "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .getOrCreate()
        )

    return spark


def normalize_number(item):
    """
    Normalize explicitly numeric inputs to float.

    Handles:
    - dicts or Rows with only numeric keys like 'double', 'long', 'string' (if numeric)
    - tuples like (None, 0, None)
    - strings in Row(...) format if fields are explicitly numeric
    - leaves other values untouched

    Returns:
    - float, pd.NaT, or original input
    """
    numeric_keys = {"double", "long"}

    if isinstance(item, (dict, Row)):
        item_dict = item.asDict() if isinstance(item, Row) else item
        for key in numeric_keys:
            if key in item_dict and item_dict[key] is not None:
                return float(item_dict[key])
        if "string" in item_dict:
            try:
                return float(item_dict["string"])
            except (ValueError, TypeError):
                return item  # preserve original if not numeric
        return pd.NaT

    elif isinstance(item, tuple):
        # Keep existing behavior for tuples
        return next((float(x) for x in item if x is not None and x != 0), pd.NaT)

    elif isinstance(item, (int, float)):
        return float(item)

    elif isinstance(item, str):
        # Only attempt conversion if string is Row(...) and has numeric content
        if item.startswith("Row(") and item.endswith(")"):
            try:
                row_dict = ast.literal_eval(item.replace("Row", "dict"))
                for key in numeric_keys:
                    if key in row_dict and row_dict[key] is not None:
                        return float(row_dict[key])
                if "string" in row_dict:
                    try:
                        return float(row_dict["string"])
                    except (ValueError, TypeError):
                        return item
                return pd.NaT
            except Exception:
                return pd.NaT

    return item


def normalize_numeric_types(df):
    """
    Normalizes the numeric types in a pandas DataFrame by converting columns of type
    spark.sql.Row, tuples, and other numeric representations to floats.

    Parameters:
        df (pandas.DataFrame): The input DataFrame

    Returns:
        pandas.DataFrame: The input DataFrame with numeric columns normalized

    Notes:
    ------
    This function is useful when processing data from sources like
    DynamoDB where numbers are stored with type information and need to be
    converted to standard Python numeric types.
    """

    for col in df.columns:
        # logger.info(f"Converting column {col} to float format.")
        df[col] = df[col].apply(normalize_number)
        # Try to convert the column to numeric, coercing errors to NaN
        # numeric_series = pd.to_numeric(df[col], errors="coerce")
        # If the conversion was successful (i.e., there are non-NaN values),
        # update the column
        # if not numeric_series.isna().all():
        #     df[col] = numeric_series

    return df


def convert_to_datetime(dates):
    """
    Convert date strings to pandas datetime.
    Now handles DataFrames, Series, lists, and single values.
    """

    def parse_date(date_str):
        if pd.isna(date_str):
            return None
        if isinstance(date_str, str):
            date_str = date_str.strip().lower()

            if date_str == "current":
                return pd.Timestamp(datetime.now().date())

            # Common date formats to try explicitly
            formats = [
                "%Y-%m-%d",
                "%d-%m-%Y",
                "%m-%d-%Y",
                "%Y/%m/%d",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%b %d, %Y",  # Jan 01, 2024
                "%B %d, %Y",  # January 01, 2024
                "%d %b %Y",  # 01 Jan 2024
                "%d %B %Y",  # 01 January 2024
                "%Y.%m.%d",
                "%d.%m.%Y",
                "%m.%d.%Y",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue

            # Only try pandas inference if the string contains date-like patterns
            if (
                any(sep in date_str for sep in ["-", "/", ".", ","])
                and not date_str.replace(" ", "").isnumeric()
            ):
                try:
                    return pd.to_datetime(date_str)
                except Exception:
                    return date_str

        return date_str

    if isinstance(dates, pd.DataFrame):
        return dates.apply(lambda x: x.apply(parse_date))
    elif isinstance(dates, pd.Series):
        return dates.apply(parse_date)
    elif isinstance(dates, list):
        return pd.Series([parse_date(d) for d in dates])
    else:
        return parse_date(dates)


def normalize_date_types(df):
    """
    Normalizes date types in a pandas DataFrame by converting columns of type
    """
    for col in df.columns:
        if df[col].notna().sum() == 0:
            # Force to string if no non-null values
            df[col] = df[col].astype("string")
            continue
        if "date" in col.lower():
            logger.info(f"Converting column {col} to datetime format.")
            parsed_col = convert_to_datetime(df[col])
            if parsed_col.apply(
                lambda x: isinstance(x, (pd.Timestamp, datetime))
            ).any():
                df[col] = parsed_col
            else:
                # Otherwise, fall back to string
                df[col] = parsed_col.astype("string")
    return df


def normalize_data_types(df):
    """
    Normalizes data types in a pandas DataFrame in the following order:
    1. Dates (to prevent them from being converted to strings)
    2. Numeric values (to prevent them from being converted to strings)
    3. Objects/strings (converting remaining object types to string dtype)

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame to normalize

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with normalized data types
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # First normalize dates to prevent them from being converted to strings
    df = normalize_date_types(df)

    # Then normalize numeric values
    df = normalize_numeric_types(df)

    # Finally convert remaining objects to string type
    df = format_object_columns(df)

    # Verify no object types remain (optional debugging)
    object_cols = df.select_dtypes(include=["object"]).columns
    if len(object_cols) > 0:
        logger.info("Their current values are:")
        for col in object_cols:
            logger.info(f"\n{col}:")
            logger.info(df[col].head())
            logger.info(f"Sample value type: {type(df[col].iloc[0])}")

    return df


def final_datatype_cleansing(df: pd.DataFrame):
    """
    Cleans data types in a pandas DataFrame before writing to a database.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame to clean

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with cleaned data types
    """
    for col in df.columns:
        if df[col].notna().sum() == 0:
            df[col] = df[col].astype("string")
    df = df.reset_index(drop=True)
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")

    return df
