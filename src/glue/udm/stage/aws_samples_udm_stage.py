# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Stage ETL job for Iceberg tables using standardized modules."""

import logging
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, to_timestamp

# Import custom utilities
from utils.audit.audit_module import AuditManager
from utils.config.config_reader import load_configs
from utils.io.data_reader import DataReader
from utils.io.data_writer import DataWriter
from utils.io.incremental_reader import IncrementalReader
from utils.job_utils import finalize_job_failure, finalize_job_success, get_source_name

# Import schema evolution utilities
from utils.transformations.schema_evolution import SchemaEvolutionHandler


def extract_error_message(exception):
    """Extract meaningful error message from exception."""
    error_msg = str(exception)
    if "\n" in error_msg:
        return error_msg.split("\n", 1)[0].strip()
    return error_msg.strip()


def main():
    """Main ETL job function."""
    # Get job parameters
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "config_file_path",
            "audit_table_name",
            "stepfunction_execution_id",
            "job_type",
            "table_location",
        ],
    )

    # Initialize Glue context first (Glue manages Spark session)
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    logger = glue_context.get_logger()

    # Load configuration and initialize utilities
    cfg = load_configs(config_file_path=args["config_file_path"])
    job_section = getattr(cfg, args["job_type"])

    audit_manager = AuditManager(args["audit_table_name"])
    data_reader = DataReader(glue_context)
    data_writer = DataWriter(glue_context, spark)
    incremental_reader = IncrementalReader(args["audit_table_name"])

    logger.info(f"Loaded config for {args['job_type']} job")
    # Determine table type based on config
    table_type = "Glue Catalog" if hasattr(job_section, "s3") else "Iceberg"
    logger.info(
        f"Target {table_type} Table: "
        f"{job_section.database_name}.{job_section.target_table_name}"
    )

    try:
        source_name = get_source_name(cfg)

        audit_manager.start_stage_job(
            args["stepfunction_execution_id"],
            job_name=args["JOB_NAME"],
            job_type=args["job_type"],
            source_table_name=source_name,
            glue_table_name=job_section.target_table_name,
        )

        # Capture job execution timestamp after audit start entry
        job_execution_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Job execution time: {job_execution_time}")

        # Read data based on source type
        if cfg.source_type.lower() == "dynamodb":
            dataframe = data_reader.read_from_dynamodb(
                table_name=cfg.source_table_name,
                gsi_column=cfg.source_incremental_col,
                gsi_value=incremental_reader.get_last_successful_execution_start_time(
                    cfg.source_table_name, args["JOB_NAME"], args["job_type"]
                ),
            )
        elif cfg.source_type.lower() in ["parquet", "csv", "json", "xlsx"]:
            # Use incremental S3 reading for all supported formats
            last_execution_time = (
                incremental_reader.get_last_successful_execution_start_time(
                    source_name, args["JOB_NAME"], args["job_type"]
                )
            )
            dataframe = data_reader.read_from_s3_incremental(
                cfg.source_s3_path,
                cfg.source_type.lower(),
                last_execution_time,
            )
        else:
            raise ValueError("Source type does not match with any data reader class")

        # Check if no data was found
        if dataframe is None:
            logger.info(
                f"No new data found for source {source_name}. "
                "Completing job with 0 files processed."
            )
            finalize_job_success(
                audit_manager,
                args,
                source_name,
                "No new data to process",
                0,
                job_execution_time,
            )
            return

        # Apply consistent struct handling - convert all structs to strings
        logger.info("Normalizing all struct fields to strings for consistent schema")
        dataframe = SchemaEvolutionHandler.normalize_struct_to_string(dataframe, logger)

        # Add udm_processed_date column using job execution timestamp
        dataframe = dataframe.withColumn(
            "udm_processed_date",
            to_timestamp(lit(job_execution_time), "yyyy-MM-dd HH:mm:ss"),
        )

        # Write data to Iceberg table
        row_count = dataframe.count()
        logger.info(f"Row count: {row_count}")

        if row_count == 0:
            logger.info(f"No data to process for source {source_name}. Skipping write.")
            output_path = "No data to write for this run"
        else:
            # Check if s3 config exists to determine table type
            s3_config = getattr(job_section, "s3", None)

            if s3_config:
                # Write to regular Glue catalog table
                logger.info("Writing data to Glue catalog table...")
                output_path = data_writer.write_to_data_catalog(
                    dataframe=dataframe,
                    s3_output_path=s3_config.target_bucket.rstrip("/"),
                    database_name=job_section.database_name,
                    table_name=job_section.target_table_name,
                    partition_column=s3_config.partition_col,
                    partition_date=job_execution_time,
                    load_method=s3_config.load_method,
                )
            else:
                # Write to Iceberg table
                logger.info("Writing data to Iceberg table...")
                output_path = data_writer.write_to_iceberg_table(
                    dataframe=dataframe,
                    database_name=job_section.database_name,
                    table_name=job_section.target_table_name,
                    s3_location=(
                        f"{args['table_location'].rstrip('/')}/"
                        f"{job_section.target_table_name}/"
                    ),
                    write_mode=job_section.load_method,
                    primary_keys=getattr(cfg, "primary_keys", [dataframe.columns[0]]),
                    partition_column="udm_processed_date",
                )
            logger.info(f"Data successfully written to {output_path}")

        finalize_job_success(
            audit_manager, args, source_name, output_path, row_count, job_execution_time
        )

    except Exception as exception:
        finalize_job_failure(audit_manager, args, source_name, exception)
        raise exception

    finally:
        job.commit()


if __name__ == "__main__":
    main()
