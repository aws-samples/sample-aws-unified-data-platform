# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for aws_samples_udm_curated.py module."""

# pylint: disable=C0415

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from curation.aws_samples_udm_curated import main

# Mock the Glue modules before importing the main module
sys.modules["awsglue"] = Mock()
sys.modules["awsglue.context"] = Mock()
sys.modules["awsglue.job"] = Mock()
sys.modules["awsglue.utils"] = Mock()
sys.modules["awsgluedq"] = Mock()
sys.modules["awsgluedq.transforms"] = Mock()
sys.modules["pyspark"] = Mock()
sys.modules["pyspark.context"] = Mock()
sys.modules["pyspark.sql"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()


class TestAwsSamplesUdmCurated:
    """Test cases for the UDM curation job."""

    @pytest.fixture
    def mock_args(self):
        """Create mock job arguments."""
        return {
            "JOB_NAME": "test-curation-job",
            "config_file_path": "s3://bucket/config.yaml",
            "audit_table_name": "test-audit-table",
            "stepfunction_execution_id": "exec-123",
            "job_type": "curation",
            "table_location": "s3://bucket/tables/",
            "environment": "test",
        }

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        # Stage configuration
        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.s3 = SimpleNamespace()  # Indicates Glue Catalog table

        # Curation configuration
        config.curation = [
            {
                "name": "test_curation_1",
                "database_name": "test_db",
                "target_table_name": "test_curated_table_1",
                "load_method": "overwrite",
                "columns": ["id", "name", "processed_date"],
                "filter_source_data": "id > 0",
            },
            {
                "name": "test_curation_2",
                "database_name": "test_db",
                "target_table_name": "test_curated_table_2",
                "load_method": "append",
                "columns": ["id", "age"],
                "s3": {
                    "target_bucket": "s3://test-bucket/curated/",
                    "partition_col": "date",
                    "load_method": "append",
                },
            },
        ]

        # Defaults and columns configuration
        config.defaults = {
            "string": {"missing_value_default": "Unknown"},
            "integer": {"missing_value_default": 0},
        }

        config.columns = {"name": {"type": "string"}, "age": {"type": "integer"}}

        return config

    @pytest.fixture
    def mock_dependencies(self):
        """Create all necessary mocks for dependencies."""
        mocks = {}

        # Mock Glue context and job
        mocks["glue_context"] = Mock()
        mocks["glue_context"].spark_session = Mock()
        mocks["glue_context"].get_logger.return_value = Mock()

        mocks["job"] = Mock()
        mocks["spark_context"] = Mock()

        # Mock utility classes
        mocks["audit_manager"] = Mock()
        mocks["data_reader"] = Mock()
        mocks["data_writer"] = Mock()
        mocks["incremental_reader"] = Mock()

        return mocks

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.DataQualityManager")
    @patch("curation.aws_samples_udm_curated.DedupeHandler")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    @patch("curation.aws_samples_udm_curated.apply_rule_based_transformations")
    @patch("curation.aws_samples_udm_curated.ColumnSelector")
    def test_main_successful_curation(
        self,
        mock_column_selector,
        mock_apply_transformations,
        mock_get_source_name,
        mock_dedupe_handler,
        mock_dq_manager_class,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test successful curation execution."""
        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_curation_targets.return_value = mock_config.curation
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup DQ manager mock
        mock_dq_manager = Mock()
        mock_dq_manager.get_dq_config_for_target.return_value = None
        mock_dq_manager_class.return_value = mock_dq_manager

        # Setup dedupe handler mock
        mock_dedupe_handler.dedupe_dataframe.return_value = sample_dataframe

        # Setup data flow
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = "2024-01-01T00:00:00Z"
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )
        mock_apply_transformations.return_value = sample_dataframe
        mock_column_selector.select_target_columns.return_value = sample_dataframe
        mock_dependencies["data_writer"].write_to_iceberg_table.return_value = (
            "s3://bucket/output/"
        )
        mock_dependencies["data_writer"].write_to_data_catalog.return_value = (
            "s3://bucket/output/"
        )

        # Mock audit table response for checking failed tables
        mock_dependencies["audit_manager"].table.get_item.return_value = {
            "Item": {
                "stages": {
                    "curation": {
                        "tables": [
                            {"curation_name": "test_curation_1", "status": "Completed"},
                            {"curation_name": "test_curation_2", "status": "Completed"},
                        ]
                    }
                }
            }
        }

        # Mock schema evolution
        with patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema:
            mock_schema.get_table_schema.return_value = None

            # Execute main function
            main()

            # Verify key interactions
            mock_dependencies["audit_manager"].start_curation_job.assert_called_once()
            mock_dependencies["audit_manager"].end_curation_job.assert_called_once()
            mock_dependencies[
                "data_reader"
            ].read_from_catalog_table.assert_called_once()

            # Verify that both curation targets were processed
            assert mock_dependencies["audit_manager"].start_stage_job.call_count == 2
            assert mock_dependencies["audit_manager"].end_stage_job.call_count == 2

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_iceberg_table_read(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test curation with Iceberg table reading."""
        # Setup config without s3 attribute (indicates Iceberg table)
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        # No s3 attribute - indicates Iceberg table

        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                "columns": ["id", "name"],
            }
        ]

        config.defaults = {}
        config.columns = {}

        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data flow for Iceberg table
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_iceberg_table.return_value = (
            sample_dataframe
        )
        mock_dependencies["data_writer"].write_to_iceberg_table.return_value = (
            "s3://bucket/output/"
        )

        # Mock audit table response
        mock_dependencies["audit_manager"].table.get_item.return_value = {
            "Item": {
                "stages": {
                    "curation": {
                        "tables": [
                            {"curation_name": "test_curation", "status": "Completed"}
                        ]
                    }
                }
            }
        }

        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.ColumnSelector"
        ) as mock_selector, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema_handler:
            mock_transform.return_value = sample_dataframe
            mock_selector.select_target_columns.return_value = sample_dataframe

            # Mock schema handler to return None for target schema
            mock_schema_handler.get_table_schema.return_value = None

            # Execute main function
            main()

            # Verify Iceberg table read was called
            mock_dependencies[
                "data_reader"
            ].read_from_iceberg_table.assert_called_once()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_target_processing_failure(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test curation with target processing failure."""
        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_curation_targets.return_value = mock_config.curation
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data flow
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        # Mock transformation failure
        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema_handler:
            mock_transform.side_effect = Exception("Transformation failed")
            mock_schema_handler.get_table_schema.return_value = None

            # Execute main function - should raise exception
            with pytest.raises(Exception, match="Transformation failed"):
                main()

            # Verify failure was logged
            mock_dependencies["audit_manager"].end_stage_job.assert_called()
            mock_dependencies["audit_manager"].end_curation_job.assert_called_with(
                mock_args["stepfunction_execution_id"], "test_source", "Failed"
            )

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_no_data_to_process(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        spark,
    ):
        """Test curation when no data is available to process."""
        # Setup config with single target
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.s3 = SimpleNamespace()

        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                "columns": ["id", "name"],
            }
        ]

        config.defaults = {}
        config.columns = {}

        # Create empty DataFrame mock
        empty_df = Mock()
        empty_df.count.return_value = 0
        empty_df.columns = ["id", "name"]

        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data flow with empty DataFrame
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = empty_df

        # Mock audit table response
        mock_dependencies["audit_manager"].table.get_item.return_value = {
            "Item": {
                "stages": {
                    "curation": {
                        "tables": [
                            {"curation_name": "test_curation", "status": "Completed"}
                        ]
                    }
                }
            }
        }

        # Execute main function
        main()

        # Verify that processing completed even with no data
        mock_dependencies["audit_manager"].start_curation_job.assert_called_once()
        mock_dependencies["audit_manager"].end_curation_job.assert_called_once()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_write_failure_recovery(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test curation with write failure and recovery."""
        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_curation_targets.return_value = mock_config.curation
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data flow
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        # Mock write failure for first target, success for second
        mock_dependencies["data_writer"].write_to_iceberg_table.side_effect = [
            Exception("Write failed"),  # First target fails
            "s3://bucket/output/",  # Second target succeeds
        ]
        mock_dependencies["data_writer"].write_to_data_catalog.return_value = (
            "s3://bucket/output/"
        )

        # Mock audit table response showing one failed table
        mock_dependencies["audit_manager"].table.get_item.return_value = {
            "Item": {
                "stages": {
                    "curation": {
                        "tables": [
                            {"curation_name": "test_curation_1", "status": "Failed"},
                            {"curation_name": "test_curation_2", "status": "Completed"},
                        ]
                    }
                }
            }
        }

        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.ColumnSelector"
        ) as mock_selector, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema_handler:
            mock_transform.return_value = sample_dataframe
            mock_selector.select_target_columns.return_value = sample_dataframe
            mock_schema_handler.get_table_schema.return_value = None

            # Execute main function - should raise exception due to failed table
            with pytest.raises(Exception, match="Curation job failed for tables"):
                main()

            # Verify failure was logged
            mock_dependencies["audit_manager"].end_curation_job.assert_called_with(
                mock_args["stepfunction_execution_id"], "test_source", "Failed"
            )

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_datetime_parsing_fallback(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test datetime parsing fallback for invalid formats."""
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_curation_targets.return_value = mock_config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Return invalid datetime format to trigger fallback
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = "invalid-datetime"
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        # Mock datetime.fromisoformat to raise exception
        with patch("curation.aws_samples_udm_curated.datetime") as mock_datetime:
            mock_datetime.fromisoformat.side_effect = ValueError("Invalid format")
            mock_datetime.now.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )

            with patch(
                "curation.aws_samples_udm_curated.apply_rule_based_transformations"
            ) as mock_transform, patch(
                "curation.aws_samples_udm_curated.ColumnSelector"
            ) as mock_selector, patch(
                "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
            ) as mock_schema:
                mock_transform.return_value = sample_dataframe
                mock_selector.select_target_columns.return_value = sample_dataframe
                mock_schema.get_table_schema.return_value = None
                main()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_iceberg_datetime_parsing_fallback(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test Iceberg datetime parsing fallback."""
        # Configure for Iceberg table
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.table_type = "iceberg"

        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                "columns": ["id", "name"],
            }
        ]
        config.defaults = {}
        config.columns = {}

        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = "invalid-datetime"
        mock_dependencies["data_reader"].read_from_iceberg_table.return_value = (
            sample_dataframe
        )

        with patch("curation.aws_samples_udm_curated.datetime") as mock_datetime:
            mock_datetime.fromisoformat.side_effect = ValueError("Invalid format")
            mock_datetime.now.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )

            with patch(
                "curation.aws_samples_udm_curated.apply_rule_based_transformations"
            ) as mock_transform, patch(
                "curation.aws_samples_udm_curated.ColumnSelector"
            ) as mock_selector, patch(
                "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
            ) as mock_schema:
                mock_transform.return_value = sample_dataframe
                mock_selector.select_target_columns.return_value = sample_dataframe
                mock_schema.get_table_schema.return_value = None
                main()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_filter_exception_handling(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test filter exception handling."""
        # Add filter to target
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.s3 = SimpleNamespace()

        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                "columns": ["id", "name"],
                "filter": "invalid_column = 'test'",
            }
        ]
        config.defaults = {}
        config.columns = {}

        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None

        # Make filter raise exception
        filtered_df = Mock()
        filtered_df.count.return_value = 10
        filtered_df.withColumn.return_value = filtered_df
        sample_dataframe.filter.side_effect = Exception("Filter failed")
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.ColumnSelector"
        ) as mock_selector, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "curation.aws_samples_udm_curated.datetime"
        ) as mock_datetime:
            mock_transform.return_value = sample_dataframe
            mock_selector.select_target_columns.return_value = sample_dataframe
            mock_schema.get_table_schema.return_value = None
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            main()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_schema_get_exception(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test schema retrieval exception handling."""
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_curation_targets.return_value = mock_config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        with patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "curation.aws_samples_udm_curated.datetime"
        ) as mock_datetime:
            mock_schema.get_table_schema.side_effect = Exception("Schema error")
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            with patch(
                "curation.aws_samples_udm_curated.apply_rule_based_transformations"
            ) as mock_transform, patch(
                "curation.aws_samples_udm_curated.ColumnSelector"
            ) as mock_selector:
                mock_transform.return_value = sample_dataframe
                mock_selector.select_target_columns.return_value = sample_dataframe
                main()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_no_target_columns(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test handling when no target columns are specified."""
        # Remove columns from config
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.s3 = SimpleNamespace()

        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                # No columns specified
            }
        ]
        config.defaults = {}
        config.columns = {}

        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )

        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "curation.aws_samples_udm_curated.datetime"
        ) as mock_datetime:
            mock_transform.return_value = sample_dataframe
            mock_schema.get_table_schema.return_value = None
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            main()

    @patch("curation.aws_samples_udm_curated.getResolvedOptions")
    @patch("curation.aws_samples_udm_curated.GlueContext")
    @patch("curation.aws_samples_udm_curated.SparkContext")
    @patch("curation.aws_samples_udm_curated.Job")
    @patch("curation.aws_samples_udm_curated.load_configs")
    @patch("curation.aws_samples_udm_curated.get_curation_targets")
    @patch("curation.aws_samples_udm_curated.AuditManager")
    @patch("curation.aws_samples_udm_curated.DataReader")
    @patch("curation.aws_samples_udm_curated.DataWriter")
    @patch("curation.aws_samples_udm_curated.IncrementalReader")
    @patch("curation.aws_samples_udm_curated.get_source_name")
    def test_main_s3_config_namespace(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_get_curation_targets,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test S3 config with SimpleNamespace conversion."""
        # Add S3 config as SimpleNamespace
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source = "test_source"

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.s3 = SimpleNamespace()

        s3_config = SimpleNamespace()
        s3_config.target_bucket = "s3://test-bucket/"
        config.curation = [
            {
                "name": "test_curation",
                "database_name": "test_db",
                "target_table_name": "test_curated_table",
                "load_method": "overwrite",
                "columns": ["id", "name"],
                "s3": s3_config,
            }
        ]
        config.defaults = {}
        config.columns = {}

        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_curation_targets.return_value = config.curation
        mock_get_source_name.return_value = "test_source"

        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = None
        mock_dependencies["data_reader"].read_from_catalog_table.return_value = (
            sample_dataframe
        )
        mock_dependencies["data_writer"].write_to_data_catalog.return_value = (
            "s3://output/path"
        )

        with patch(
            "curation.aws_samples_udm_curated.apply_rule_based_transformations"
        ) as mock_transform, patch(
            "curation.aws_samples_udm_curated.ColumnSelector"
        ) as mock_selector, patch(
            "curation.aws_samples_udm_curated.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "curation.aws_samples_udm_curated.datetime"
        ) as mock_datetime:
            mock_transform.return_value = sample_dataframe
            mock_selector.select_target_columns.return_value = sample_dataframe
            mock_schema.get_table_schema.return_value = None
            mock_datetime.utcnow.return_value.strftime.return_value = (
                "2023-01-01 00:00:00"
            )
            mock_datetime.utcnow.return_value.isoformat.return_value = (
                "2023-01-01T00:00:00"
            )
            main()

    def test_main_entry_point(self):
        """Test main entry point when called directly."""
        with patch(
            "curation.aws_samples_udm_curated.getResolvedOptions"
        ) as mock_get_options:
            mock_get_options.return_value = {
                "JOB_NAME": "test-job",
                "config_file_path": "s3://test/config.yaml",
            }
            with patch("curation.aws_samples_udm_curated.main") as mock_main:
                # Test the __main__ block by directly calling main()
                import curation.aws_samples_udm_curated as module

                # Simulate running as main by calling main() directly
                if hasattr(module, "__name__"):
                    original_name = module.__name__
                    module.__name__ = "__main__"
                    try:
                        module.main()
                    finally:
                        module.__name__ = original_name
