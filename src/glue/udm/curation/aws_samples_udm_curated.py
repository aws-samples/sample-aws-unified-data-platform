# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Curated job for processing staged data and creating curated datasets."""

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
from utils.column_selector import ColumnSelector
from utils.config.config_reader import load_configs
from utils.data_quality import DataQualityManager
from utils.io.data_reader import DataReader
from utils.io.data_writer import DataWriter
from utils.io.incremental_reader import IncrementalReader
from utils.job_utils import (
    get_curation_targets,
    get_source_name,
)
from utils.transformation_engine import apply_rule_based_transformations
from utils.transformations.dedupe_module import DedupeHandler

# Import schema evolution utilities
from utils.transformations.schema_evolution import SchemaEvolutionHandler


def main():
    """Main curated job function."""
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
            "environment",
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

    # Load configuration and initialize all utilities
    cfg = load_configs(config_file_path=args["config_file_path"])
    curation_targets = get_curation_targets(cfg)
    audit_manager, data_reader, data_writer = (
        AuditManager(args["audit_table_name"]),
        DataReader(glue_context),
        DataWriter(glue_context, spark),
    )
    incremental_reader = IncrementalReader(args["audit_table_name"])
    dq_manager = DataQualityManager(glue_context, data_writer, logger)

    logger.info(f"Loaded config for {args['job_type']} job")

    try:
        source_name = get_source_name(cfg)

        # Create initial curation audit entry
        audit_manager.start_curation_job(
            args["stepfunction_execution_id"], source_name, args["JOB_NAME"]
        )

        # Capture job execution timestamp after audit start entry
        job_execution_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Job execution time: {job_execution_time}")

        # Get incremental timestamp from current step function's stage job
        stage_job_name = f"aws-samples-udm-stage-{args['environment']}"
        logger.info(f"stage_job_name: {stage_job_name}")
        last_stage_execution_time = (
            incremental_reader.get_last_successful_execution_start_time(
                source_name,
                stage_job_name,
                args["job_type"],
                args["stepfunction_execution_id"],
            )
        )

        # Read staged data with incremental filter
        stage_table_type = "Glue Catalog" if hasattr(cfg.stage, "s3") else "Iceberg"
        logger.info(
            f"Reading staged data from {stage_table_type} table: "
            f"{cfg.stage.database_name}.{cfg.stage.target_table_name}"
        )
        logger.info(f"Using incremental filter from: {last_stage_execution_time}")

        # Read from table using appropriate method based on table type
        if stage_table_type == "Glue Catalog":
            # Use Glue catalog reader for regular tables
            partition_filter = None
            if last_stage_execution_time:
                # Convert ISO format from audit to simple format for comparison
                try:
                    parsed_time = datetime.fromisoformat(
                        last_stage_execution_time.replace("Z", "+00:00")
                    )
                    simple_format_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
                    partition_filter = (
                        f"udm_processed_date >= " f"timestamp('{simple_format_time}')"
                    )
                except Exception:
                    # Fallback to original format if parsing fails
                    partition_filter = (
                        f"udm_processed_date >= "
                        f"timestamp('{last_stage_execution_time}')"
                    )
                logger.info(f"Using partition filter: {partition_filter}")
            else:
                logger.info(
                    "No last execution time found - reading all data "
                    "(first run or no previous successful stage execution)"
                )

            dataframe = data_reader.read_from_catalog_table(
                database_name=cfg.stage.database_name,
                table_name=cfg.stage.target_table_name,
                partition_filter=partition_filter,
            )
        else:
            # Use Iceberg reader for Iceberg tables
            partition_filter = None
            if last_stage_execution_time:
                # Convert ISO format from audit to simple format for comparison
                try:
                    parsed_time = datetime.fromisoformat(
                        last_stage_execution_time.replace("Z", "+00:00")
                    )
                    simple_format_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
                    partition_filter = (
                        f"udm_processed_date >= " f"timestamp('{simple_format_time}')"
                    )
                except Exception:
                    # Fallback to original format if parsing fails
                    partition_filter = (
                        f"udm_processed_date >= "
                        f"timestamp('{last_stage_execution_time}')"
                    )

            dataframe = data_reader.read_from_iceberg_table(
                database_name=cfg.stage.database_name,
                table_name=cfg.stage.target_table_name,
                partition_filter=partition_filter,
            )

        row_count = dataframe.count()
        logger.info(f"Initial row count: {row_count}")

        # Phase 1: Process all targets and prepare outputs (no writes yet)
        prepared_outputs = []

        for target in curation_targets:
            # Capture start time for this target
            target_start_time = datetime.utcnow().isoformat()

            # Start audit for this target
            audit_manager.start_stage_job(
                args["stepfunction_execution_id"],
                job_name=args["JOB_NAME"],
                job_type=args["job_type"],
                source_table_name=source_name,
                glue_table_name=target["target_table_name"],
                curation_name=target["name"],
                execution_start_time=target_start_time,
            )

            # Apply target-specific filter if specified
            target_dataframe = dataframe
            filter_condition = target.get("filter_source_data")
            if filter_condition:
                logger.info(f"Applying filter for {target['name']}: {filter_condition}")
                try:
                    target_dataframe = dataframe.filter(filter_condition)
                    filtered_count = target_dataframe.count()
                    logger.info(
                        f"Row count after filter for {target['name']}: {filtered_count}"
                    )
                except Exception as e:
                    logger.warning(f"Filter failed for {target['name']}: {e}")
                    target_dataframe = dataframe
            target_name = target["name"]
            logger.info(f"Preparing curation target: {target_name}")

            try:
                target_row_count = target_dataframe.count()
                if target_row_count > 0:
                    # Apply data quality checks if configured and enabled
                    dq_config = dq_manager.get_dq_config_for_target(cfg, target_name)
                    dq_metrics = {}
                    if dq_config and dq_config.get("enabled", False):
                        logger.info(f"Applying data quality checks for {target_name}")
                        (
                            target_dataframe,
                            dq_metrics,
                        ) = dq_manager.apply_data_quality_checks(
                            dataframe=target_dataframe,
                            dq_config=dq_config,
                            source_name=source_name,
                            target_name=target_name,
                            execution_id=args["stepfunction_execution_id"],
                            table_location=args["table_location"],
                        )
                        logger.info(f"DQ metrics: {dq_metrics}")

                        # Check if any records passed DQ validation
                        if target_dataframe is None or target_dataframe.count() == 0:
                            logger.info(
                                f"No records passed data quality validation for {target_name}. Skipping transformations."  # pylint: disable=line-too-long
                            )
                            # Complete audit entry for this target with DQ metrics
                            no_of_rows_failed_dq_check = dq_metrics.get(
                                "failed_rows", 0
                            )
                            audit_manager.end_stage_job(
                                args["stepfunction_execution_id"],
                                source_table_name=source_name,
                                job_type=args["job_type"],
                                status="Completed",
                                data_path="No data passed DQ validation",
                                no_of_rows_processed=0,
                                udm_processed_date=job_execution_time,
                                curation_name=target_name,
                                no_of_rows_failed_dq_check=no_of_rows_failed_dq_check,
                            )
                            prepared_outputs.append(
                                {
                                    "target": target,
                                    "dataframe": None,
                                    "row_count": 0,
                                    "dq_metrics": dq_metrics,
                                }
                            )
                            continue
                    else:
                        logger.info(
                            f"Data quality checks disabled or not configured for {target_name}"  # pylint: disable=line-too-long
                        )

                    # Handle schema evolution before transformations
                    target_schema = None
                    try:
                        target_schema = SchemaEvolutionHandler.get_table_schema(
                            spark, target["database_name"], target["target_table_name"]
                        )
                    except Exception as e:
                        logger.info(
                            f"Could not get target schema for {target_name}: {e}"
                        )

                    # Skip JSON-to-struct parsing to preserve complete JSON data
                    evolved_df = target_dataframe

                    # Skip schema evolution to preserve JSON data integrity
                    logger.info(
                        f"Skipping schema evolution to preserve JSON data "
                        f"for {target_name}"
                    )

                    # Apply transformations with global config
                    config_dict = {
                        "defaults": getattr(cfg, "defaults", {}),
                        "columns": getattr(cfg, "columns", {}),
                    }

                    # Get target table schema for type alignment
                    target_table_schema = None
                    if target_schema:
                        target_table_schema = {
                            field.name: field.dataType for field in target_schema.fields
                        }
                        logger.info(
                            f"Using target table schema for {target_name}: {len(target_table_schema)} columns"  # pylint: disable=line-too-long
                        )

                    logger.info(f"Applying transformations for {target_name}...")
                    transformed_df = apply_rule_based_transformations(
                        evolved_df, config_dict, target_table_schema=target_table_schema
                    )

                    # Select columns specified for this target,
                    # including exploded array columns
                    target_columns = target.get("columns", [])
                    if target_columns:
                        transformed_df = ColumnSelector.select_target_columns(
                            transformed_df, target_columns, config_dict
                        )
                        logger.info(
                            f"Selected {len(target_columns)} specified columns "
                            f"for {target_name}"
                        )
                    else:
                        logger.info(
                            f"No target columns specified for {target_name} - "
                            "using all available columns"
                        )

                    # Add udm_processed_date column using job execution timestamp
                    transformed_df = transformed_df.withColumn(
                        "udm_processed_date",
                        to_timestamp(lit(job_execution_time), "yyyy-MM-dd HH:mm:ss"),
                    )

                    # Apply deduplication to remove duplicate records
                    logger.info(f"Applying deduplication for {target_name}...")
                    dedupe_df = DedupeHandler.dedupe_dataframe(transformed_df)

                    final_row_count = dedupe_df.count()
                    logger.info(
                        f"Final row count after deduplication for {target_name}: {final_row_count}"  # pylint: disable=line-too-long
                    )

                    # Store prepared output (no write yet)
                    prepared_outputs.append(
                        {
                            "target": target,
                            "dataframe": dedupe_df,
                            "row_count": final_row_count,
                            "dq_metrics": dq_metrics,
                        }
                    )
                else:
                    logger.info(f"No data to process for {target_name}. Skipping.")
                    prepared_outputs.append(
                        {
                            "target": target,
                            "dataframe": None,
                            "row_count": 0,
                            "dq_metrics": {},
                        }
                    )
            except Exception as exception:
                # Handle failure for this specific target
                audit_manager.end_stage_job(
                    args["stepfunction_execution_id"],
                    source_table_name=source_name,
                    job_type=args["job_type"],
                    status="Failed",
                    failure_reason=str(exception),
                    curation_name=target_name,
                )
                logger.error(f"Failed processing target {target_name}: {exception}")
                # Fail the entire job instead of continuing
                audit_manager.end_curation_job(
                    args["stepfunction_execution_id"], source_name, "Failed"
                )
                raise exception

        logger.info(f"All {len(prepared_outputs)} targets prepared successfully")

        # Phase 2: Atomic write - only if ALL targets processed successfully
        for output in prepared_outputs:
            target = output["target"]
            target_name = target["name"]
            transformed_df = output["dataframe"]
            final_row_count = output["row_count"]
            dq_metrics = output.get("dq_metrics", {})

            logger.info(f"Writing curation target: {target_name}")

            try:
                if transformed_df is not None:
                    # Check if s3 config exists to determine table type
                    s3_config = target.get("s3")

                    if s3_config:
                        # Convert SimpleNamespace to dict if needed
                        if hasattr(s3_config, "__dict__"):
                            s3_config = vars(s3_config)

                        # Write to regular Glue catalog table
                        logger.info(
                            f"Writing data to Glue catalog table for {target_name}..."
                        )
                        output_path = data_writer.write_to_data_catalog(
                            dataframe=transformed_df,
                            s3_output_path=s3_config["target_bucket"].rstrip("/"),
                            database_name=target["database_name"],
                            table_name=target["target_table_name"],
                            partition_column=s3_config["partition_col"],
                            partition_date=job_execution_time,
                            load_method=s3_config["load_method"],
                        )
                    else:
                        # Write to Iceberg table
                        logger.info(
                            f"Writing data to Iceberg table for {target_name}..."
                        )
                        output_path = data_writer.write_to_iceberg_table(
                            dataframe=transformed_df,
                            database_name=target["database_name"],
                            table_name=target["target_table_name"],
                            s3_location=(
                                f"{args['table_location'].rstrip('/')}/"
                                f"{target['target_table_name']}/"
                            ),
                            write_mode=target["load_method"],
                            primary_keys=target.get(
                                "primary_keys", [transformed_df.columns[0]]
                            ),
                            partition_column="udm_processed_date",
                        )
                    logger.info(
                        f"Data successfully written to {output_path} for {target_name}"
                    )
                else:
                    output_path = "No data to write for this run"

                # Finalize audit for this target
                no_of_rows_failed_dq_check = dq_metrics.get("failed_rows", 0)
                audit_manager.end_stage_job(
                    args["stepfunction_execution_id"],
                    source_table_name=source_name,
                    job_type=args["job_type"],
                    status="Completed",
                    data_path=output_path,
                    no_of_rows_processed=final_row_count,
                    udm_processed_date=job_execution_time,
                    curation_name=target_name,
                    no_of_rows_failed_dq_check=no_of_rows_failed_dq_check,
                )
            except Exception as exception:
                # Handle failure for this specific target
                audit_manager.end_stage_job(
                    args["stepfunction_execution_id"],
                    source_table_name=source_name,
                    job_type=args["job_type"],
                    status="Failed",
                    failure_reason=str(exception),
                    curation_name=target_name,
                )
                logger.error(f"Failed writing target {target_name}: {exception}")
                continue

        # Check if any tables failed
        failed_tables = []
        for output in prepared_outputs:
            target_name = output["target"]["name"]
            # Check audit table for this target's status
            try:
                response = audit_manager.table.get_item(
                    Key={
                        "stepfunction_execution_id": args["stepfunction_execution_id"],
                        "table_name": source_name,
                    }
                )
                curation_data = (
                    response.get("Item", {}).get("stages", {}).get("curation", {})
                )
                for table_entry in curation_data.get("tables", []):
                    if (
                        table_entry.get("curation_name") == target_name
                        and table_entry.get("status") == "Failed"
                    ):
                        failed_tables.append(target_name)
            except Exception as e:
                logger.warning(f"Could not check audit status for {target_name}: {e}")

        if failed_tables:
            logger.error(f"Job failed due to table failures: {failed_tables}")
            audit_manager.end_curation_job(
                args["stepfunction_execution_id"], source_name, "Failed"
            )
            raise Exception(f"Curation job failed for tables: {failed_tables}")
        else:
            logger.info("All curation targets written successfully")
            audit_manager.end_curation_job(
                args["stepfunction_execution_id"], source_name, "Completed"
            )

    except Exception as exception:
        # Update overall curation status to Failed
        audit_manager.end_curation_job(
            args["stepfunction_execution_id"], source_name, "Failed"
        )
        logger.error(f"Job failed with exception: {exception}")
        raise exception

    finally:
        job.commit()


if __name__ == "__main__":
    main()
