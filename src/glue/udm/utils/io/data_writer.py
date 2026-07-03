# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Data writer module for writing data to S3, Glue Catalog, and S3 Tables."""

import logging
import re
import time

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, struct, when
from pyspark.sql.types import ArrayType, NullType, StringType, StructField, StructType

logger = logging.getLogger(__name__)


class DataWriter:
    """Class for writing data to S3, Glue Catalog, and S3 Tables."""

    def __init__(self, glue_context=None, spark_session=None):
        """Initialize DataWriter with GlueContext and SparkSession.

        Args:
            glue_context: AWS Glue context object
            spark_session: Spark session for S3 Tables operations
        """
        if not glue_context:
            spark_context = SparkContext.getOrCreate()
            glue_context = GlueContext(spark_context)
        self.glue_context = glue_context
        self.spark = spark_session or glue_context.spark_session

    def convert_void_columns_to_string(
        self, dataframe: DataFrame, _table_name: str
    ) -> DataFrame:
        """Convert void/null columns to string type for compatibility.

        Args:
            dataframe: Input Spark DataFrame
            table_name: Name of the table (for logging)

        Returns:
            DataFrame with converted schema
        """

        def replace_void_in_schema(schema: StructType) -> StructType:
            """Recursively replace void types in schema."""
            new_fields = []
            for field in schema.fields:
                if isinstance(field.dataType, NullType):
                    new_fields.append(StructField(field.name, StringType(), True))
                elif isinstance(field.dataType, StructType):
                    # Handle empty struct by adding dummy field
                    if len(field.dataType.fields) == 0:
                        dummy_struct = StructType(
                            [StructField("_placeholder", StringType(), True)]
                        )
                        new_fields.append(
                            StructField(field.name, dummy_struct, field.nullable)
                        )
                    else:
                        new_struct = StructType(
                            replace_void_in_schema(field.dataType).fields
                        )
                        new_fields.append(
                            StructField(field.name, new_struct, field.nullable)
                        )
                elif isinstance(field.dataType, ArrayType):
                    if isinstance(field.dataType.elementType, StructType):
                        new_element_type = StructType(
                            replace_void_in_schema(field.dataType.elementType).fields
                        )
                        new_array = ArrayType(
                            new_element_type, field.dataType.containsNull
                        )
                    elif isinstance(field.dataType.elementType, NullType):
                        # Convert array<void> to array<string>
                        new_array = ArrayType(StringType(), field.dataType.containsNull)
                    else:
                        new_array = field.dataType
                    new_fields.append(
                        StructField(field.name, new_array, field.nullable)
                    )
                else:
                    new_fields.append(field)
            return StructType(new_fields)

        # Add placeholder field to empty structs
        for field in dataframe.schema.fields:
            if (
                isinstance(field.dataType, StructType)
                and len(field.dataType.fields) == 0
            ):
                dataframe = dataframe.withColumn(
                    field.name,
                    when(col(field.name).isNull(), None).otherwise(
                        struct(lit(None).alias("_placeholder"))
                    ),
                )

        new_schema = replace_void_in_schema(dataframe.schema)
        return self.spark.createDataFrame(dataframe.rdd, new_schema)

    def write_to_data_catalog(self, dataframe: DataFrame, **kwargs) -> str:
        """Write DataFrame to S3 with Glue Data Catalog registration and partitioning.

        Args:
            dataframe: Input Spark DataFrame
            **kwargs: Keyword arguments including s3_output_path, database_name,
                     table_name, partition_column, partition_date, load_method

        Returns:
            Full S3 path where data was written
        """
        s3_output_path = kwargs["s3_output_path"]
        database_name = kwargs["database_name"]
        table_name = kwargs["table_name"]
        partition_column = kwargs["partition_column"]
        partition_date = kwargs["partition_date"]
        load_method = kwargs.get("load_method", "overwrite")

        processed_df = self.convert_void_columns_to_string(dataframe, table_name)

        # Use current timestamp if partition_date is None
        if partition_date is None:
            df_with_partition = processed_df.withColumn(
                partition_column, current_timestamp()
            )
        else:
            df_with_partition = processed_df.withColumn(
                partition_column, lit(partition_date)
            )

        full_s3_path = f"{s3_output_path}/{table_name}"

        writer = df_with_partition.write.mode(load_method.lower())

        if load_method.lower() == "overwrite":
            writer = writer.option(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

        (
            writer.option("mergeSchema", "true")
            .option("spark.sql.hive.convertMetastoreParquet", "false")
            .option("spark.sql.adaptive.enabled", "true")
            .option("spark.sql.adaptive.schemaInference.enabled", "true")
            .partitionBy([partition_column])
            .option("path", full_s3_path)
            .saveAsTable(f"{database_name}.{table_name}")
        )

        return f"{full_s3_path}/{partition_column}={partition_date}/"

    def write_to_s3_raw(
        self, dataframe: DataFrame, s3_output_path: str, file_format: str = "parquet"
    ) -> str:
        """Write DataFrame directly to S3 without catalog registration or partitioning.

        Args:
            dataframe: Input Spark DataFrame
            s3_output_path: S3 output path
            file_format: Output file format

        Returns:
            S3 output path
        """
        dynamic_frame = DynamicFrame.fromDF(
            dataframe, self.glue_context, "dynamic_frame"
        )

        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format=file_format,
            connection_options={"path": s3_output_path},
        )

        return s3_output_path

    def create_table_if_not_exists(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
        dataframe: DataFrame,
        partition_column: str = None,
    ) -> None:
        """Create S3 table with dynamic schema if it doesn't exist."""
        columns_def = [
            f"{field.name} {field.dataType.simpleString()}"
            for field in dataframe.schema.fields
        ]
        columns_sql = ",\n        ".join(columns_def)

        partition_clause = (
            f"PARTITIONED BY ({partition_column})" if partition_column else ""
        )

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} (
                {columns_sql}
            ) USING ICEBERG
            {partition_clause}
        """

        logger.info("Creating table with SQL: %s", create_sql)
        self.spark.sql(create_sql)
        time.sleep(5)  # Allow table creation to complete
        logger.info("Table creation completed")

    def add_new_columns(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
        dataframe: DataFrame,
    ) -> None:
        """Add new columns to existing table for schema evolution."""

        # Validate identifiers to prevent SQL injection
        if not re.match(r"^[a-zA-Z0-9_-]+$", catalog_name):
            raise ValueError(f"Invalid catalog name: {catalog_name}")
        if not re.match(r"^[a-zA-Z0-9_-]+$", database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        if not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        try:
            existing_table = self.spark.table(
                f"{catalog_name}.{database_name}.{table_name}"
            )
            existing_columns = set(existing_table.columns)
            source_columns = set(dataframe.columns)

            new_columns = source_columns - existing_columns
            if new_columns:
                for col_name in new_columns:
                    # Validate column name
                    if not re.match(r"^[a-zA-Z0-9_-]+$", col_name):
                        logger.warning("Skipping invalid column name: %s", col_name)
                        continue
                    col_type = dict(dataframe.dtypes)[col_name]
                    alter_sql = (
                        f"ALTER TABLE {catalog_name}.{database_name}."
                        f"{table_name} ADD COLUMN {col_name} {col_type}"
                    )
                    self.spark.sql(alter_sql)
                    logger.info("Added new column: %s %s", col_name, col_type)
        except Exception as e:
            logger.info("Could not add new columns: %s", e)

    def perform_upsert(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
        dataframe: DataFrame,
        primary_keys: list,
    ) -> None:
        """Perform MERGE (upsert) operation with Iceberg."""
        # Deduplicate source data
        dataframe = dataframe.dropDuplicates(primary_keys)

        # Create temporary view
        temp_view = f"source_data_{int(time.time())}"
        dataframe.createOrReplaceTempView(temp_view)

        # Get all columns except primary keys for UPDATE SET clause
        columns = [
            field.name
            for field in dataframe.schema.fields
            if field.name not in primary_keys
        ]

        # Build UPDATE SET clause dynamically
        update_set = ",\n            ".join(
            [f"{col} = source.{col}" for col in columns]
        )

        # Build INSERT VALUES clause dynamically
        all_columns = [field.name for field in dataframe.schema.fields]
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])

        # Build ON clause for multiple primary keys
        on_conditions = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in primary_keys]
        )

        # Use Iceberg MERGE with auto schema evolution
        merge_sql = f"""
            MERGE INTO {catalog_name}.{database_name}.{table_name} AS target
            USING {temp_view} AS source
            ON {on_conditions}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """  # nosec B608 (False-positive due to f-string - identifiers are validated) # pylint: disable=line-too-long

        logger.info("Executing MERGE SQL: %s", merge_sql)
        self.spark.sql(merge_sql)

    def write_to_s3_table(
        self,
        dataframe: DataFrame,
        catalog_name: str,
        database_name: str,
        table_name: str,
        write_mode: str = "append",
        primary_keys: list = None,
        partition_column: str = None,
    ) -> str:
        """Write DataFrame to S3 Tables with different write modes.

        Args:
            dataframe: Input Spark DataFrame
            catalog_name: S3 Tables catalog name
            database_name: Database name
            table_name: Table name
            write_mode: Write mode (append/overwrite/upsert)
            primary_keys: List of primary keys for upsert operations
            partition_column: Column to partition by

        Returns:
            S3 Tables path where data was written
        """
        # Convert void columns using existing method
        dataframe = self.convert_void_columns_to_string(dataframe, table_name)

        # Create table if not exists
        self.create_table_if_not_exists(
            catalog_name, database_name, table_name, dataframe, partition_column
        )

        # Add new columns for schema evolution
        self.add_new_columns(catalog_name, database_name, table_name, dataframe)

        # Write data using different modes
        full_table_name = f"{catalog_name}.{database_name}.{table_name}"

        if write_mode.lower() == "append":
            dataframe.writeTo(full_table_name).option(
                "check-ordering", "false"
            ).tableProperty("write.merge.mode", "merge-on-read").tableProperty(
                "format-version", "2"
            ).using(
                "iceberg"
            ).append()

        elif write_mode.lower() == "overwrite":
            dataframe.writeTo(full_table_name).using("iceberg").overwritePartitions()

        elif write_mode.lower() == "upsert":
            if not primary_keys:
                raise ValueError("Primary keys are required for upsert operations")
            self.perform_upsert(
                catalog_name, database_name, table_name, dataframe, primary_keys
            )

        else:
            raise ValueError(f"Unsupported write mode: {write_mode}")

        return f"s3tables://{database_name}.{table_name}"

    def create_iceberg_table_if_not_exists(
        self,
        database_name: str,
        table_name: str,
        dataframe: DataFrame,
        s3_location: str,
        partition_column: str = None,
    ) -> None:
        """Create Iceberg table on S3 if it doesn't exist."""
        # Validate identifiers to prevent SQL injection
        if not re.match(r"^[a-zA-Z0-9_-]+$", database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        if not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        if partition_column and not re.match(r"^[a-zA-Z0-9_-]+$", partition_column):
            raise ValueError(f"Invalid partition column: {partition_column}")

        columns_def = [
            f"{field.name} {field.dataType.simpleString()}"
            for field in dataframe.schema.fields
        ]
        columns_sql = ",\n        ".join(columns_def)

        partition_clause = (
            f"PARTITIONED BY ({partition_column})" if partition_column else ""
        )

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
                {columns_sql}
            ) USING ICEBERG
            LOCATION '{s3_location}'
            {partition_clause}
        """

        logger.info("Creating Iceberg table with SQL: %s", create_sql)
        self.spark.sql(create_sql)
        time.sleep(5)
        logger.info("Iceberg table creation completed")

    def perform_iceberg_upsert(
        self,
        database_name: str,
        table_name: str,
        dataframe: DataFrame,
        primary_keys: list,
    ) -> None:
        """Perform MERGE (upsert) operation on Iceberg table."""
        dataframe = dataframe.dropDuplicates(primary_keys)

        temp_view = f"source_data_{int(time.time())}"
        dataframe.createOrReplaceTempView(temp_view)

        columns = [
            field.name
            for field in dataframe.schema.fields
            if field.name not in primary_keys
        ]
        update_set = ",\n            ".join(
            [f"{col} = source.{col}" for col in columns]
        )

        all_columns = [field.name for field in dataframe.schema.fields]
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])

        # Build ON clause for multiple primary keys
        on_conditions = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in primary_keys]
        )

        merge_sql = f"""
            MERGE INTO glue_catalog.{database_name}.{table_name} AS target
            USING {temp_view} AS source
            ON {on_conditions}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """  # nosec B608 (False-positive due to f-string - identifiers are validated) # pylint: disable=line-too-long

        logger.info("Executing Iceberg MERGE SQL: %s", merge_sql)
        self.spark.sql(merge_sql)

    def write_to_iceberg_table(
        self,
        dataframe: DataFrame,
        database_name: str,
        table_name: str,
        s3_location: str,
        write_mode: str = "append",
        primary_keys: list = None,
        partition_column: str = None,
    ) -> str:
        """Write DataFrame to Iceberg table on S3.

        Args:
            dataframe: Input Spark DataFrame
            database_name: Database name
            table_name: Table name
            s3_location: S3 location for the table
            write_mode: Write mode (append/overwrite/upsert)
            primary_keys: List of primary keys for upsert operations
            partition_column: Column to partition by

        Returns:
            S3 path where data was written
        """
        # Convert void columns
        dataframe = self.convert_void_columns_to_string(dataframe, table_name)

        # Create table if not exists
        self.create_iceberg_table_if_not_exists(
            database_name, table_name, dataframe, s3_location, partition_column
        )

        # Add new columns for schema evolution
        self.add_new_columns("glue_catalog", database_name, table_name, dataframe)

        full_table_name = f"glue_catalog.{database_name}.{table_name}"

        if write_mode.lower() == "append":
            dataframe.write.format("iceberg").mode("append").option(
                "write.merge.mode", "merge-on-read"
            ).option("format-version", "2").save(full_table_name)

        elif write_mode.lower() == "overwrite":
            dataframe.write.format("iceberg").mode("overwrite").save(full_table_name)

        elif write_mode.lower() == "upsert":
            if not primary_keys:
                raise ValueError("Primary keys are required for upsert operations")
            self.perform_iceberg_upsert(
                database_name, table_name, dataframe, primary_keys
            )

        else:
            raise ValueError(f"Unsupported write mode: {write_mode}")

        return s3_location
