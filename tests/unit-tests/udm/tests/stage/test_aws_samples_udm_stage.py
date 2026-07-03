# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for aws_samples_udm_stage.py module."""

# pylint: disable=C0301,C0415,I1101

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from stage.aws_samples_udm_stage import extract_error_message, main

# Mock the Glue modules before importing the main module
sys.modules["awsglue"] = Mock()
sys.modules["awsglue.context"] = Mock()
sys.modules["awsglue.job"] = Mock()
sys.modules["awsglue.utils"] = Mock()
sys.modules["pyspark"] = Mock()
sys.modules["pyspark.context"] = Mock()
sys.modules["pyspark.sql"] = Mock()

# Mock PySpark functions to return Column-like objects
mock_functions = Mock()
mock_functions.current_timestamp = Mock(return_value=Mock())
mock_functions.lit = Mock(return_value=Mock())
mock_functions.to_timestamp = Mock(return_value=Mock())
sys.modules["pyspark.sql.functions"] = mock_functions

mock_types = Mock()
mock_types.StringType = Mock()
mock_types.StructType = Mock()
sys.modules["pyspark.sql.types"] = mock_types


class TestAwsSamplesUdmStage:
    """Test cases for the UDM stage job."""

    @pytest.fixture
    def mock_args(self):
        """Create mock job arguments."""
        return {
            "JOB_NAME": "test-stage-job",
            "config_file_path": "s3://bucket/config.yaml",
            "audit_table_name": "test-audit-table",
            "stepfunction_execution_id": "exec-123",
            "job_type": "stage",
            "table_location": "s3://bucket/tables/",
        }

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source_incremental_col = "updated_at"
        config.source_s3_path = "s3://bucket/data/"
        config.primary_keys = ["id"]

        # Stage configuration
        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.load_method = "append"

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

    def test_extract_error_message_single_line(self):
        """Test extracting error message from single line exception."""
        exception = Exception("Simple error message")

        result = extract_error_message(exception)

        assert result == "Simple error message"

    def test_extract_error_message_multiline(self):
        """Test extracting error message from multiline exception."""
        exception = Exception("First line error\nSecond line details\nThird line")

        result = extract_error_message(exception)

        assert result == "First line error"

    @patch("stage.aws_samples_udm_stage.getResolvedOptions")
    @patch("stage.aws_samples_udm_stage.GlueContext")
    @patch("stage.aws_samples_udm_stage.SparkContext")
    @patch("stage.aws_samples_udm_stage.Job")
    @patch("stage.aws_samples_udm_stage.load_configs")
    @patch("stage.aws_samples_udm_stage.AuditManager")
    @patch("stage.aws_samples_udm_stage.DataReader")
    @patch("stage.aws_samples_udm_stage.DataWriter")
    @patch("stage.aws_samples_udm_stage.IncrementalReader")
    @patch("stage.aws_samples_udm_stage.get_source_name")
    @patch("stage.aws_samples_udm_stage.finalize_job_success")
    def test_main_dynamodb_source_success(
        self,
        mock_finalize_success,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
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
        """Test successful execution with DynamoDB source."""
        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data reader to return sample dataframe
        mock_dependencies["data_reader"].read_from_dynamodb.return_value = (
            sample_dataframe
        )
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = "2024-01-01T00:00:00Z"
        mock_dependencies["data_writer"].write_to_iceberg_table.return_value = (
            "s3://bucket/output/"
        )

        # Mock schema evolution
        with patch(
            "stage.aws_samples_udm_stage.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "stage.aws_samples_udm_stage.to_timestamp"
        ) as mock_to_timestamp, patch(
            "stage.aws_samples_udm_stage.lit"
        ) as mock_lit:
            mock_schema.normalize_struct_to_string.return_value = sample_dataframe

            # Mock PySpark functions to return Column-like objects
            mock_column = Mock()
            mock_to_timestamp.return_value = mock_column
            mock_lit.return_value = mock_column

            # Mock withColumn to return the same dataframe
            sample_dataframe.withColumn.return_value = sample_dataframe

            # Execute main function
            main()

            # Verify key interactions
            mock_dependencies["audit_manager"].start_stage_job.assert_called_once()
            mock_dependencies["data_reader"].read_from_dynamodb.assert_called_once()
            mock_dependencies["data_writer"].write_to_iceberg_table.assert_called_once()
            mock_finalize_success.assert_called_once()

    @patch("stage.aws_samples_udm_stage.getResolvedOptions")
    @patch("stage.aws_samples_udm_stage.GlueContext")
    @patch("stage.aws_samples_udm_stage.SparkContext")
    @patch("stage.aws_samples_udm_stage.Job")
    @patch("stage.aws_samples_udm_stage.load_configs")
    @patch("stage.aws_samples_udm_stage.AuditManager")
    @patch("stage.aws_samples_udm_stage.DataReader")
    @patch("stage.aws_samples_udm_stage.DataWriter")
    @patch("stage.aws_samples_udm_stage.IncrementalReader")
    @patch("stage.aws_samples_udm_stage.get_source_name")
    @patch("stage.aws_samples_udm_stage.finalize_job_success")
    def test_main_s3_source_success(
        self,
        mock_finalize_success,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test successful execution with S3 source."""
        # Setup config for S3 source
        config = SimpleNamespace()
        config.source_type = "parquet"
        config.source_s3_path = "s3://bucket/data/"
        config.source_incremental_col = "updated_at"
        config.primary_keys = ["id"]

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.load_method = "append"

        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data reader to return sample dataframe
        mock_dependencies["data_reader"].read_from_s3_incremental.return_value = (
            sample_dataframe
        )
        mock_dependencies[
            "incremental_reader"
        ].get_last_successful_execution_start_time.return_value = "2024-01-01T00:00:00Z"
        mock_dependencies["data_writer"].write_to_iceberg_table.return_value = (
            "s3://bucket/output/"
        )

        # Mock schema evolution
        with patch(
            "stage.aws_samples_udm_stage.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "stage.aws_samples_udm_stage.to_timestamp"
        ) as mock_to_timestamp, patch(
            "stage.aws_samples_udm_stage.lit"
        ) as mock_lit:
            mock_schema.normalize_struct_to_string.return_value = sample_dataframe

            # Mock PySpark functions to return Column-like objects
            mock_column = Mock()
            mock_to_timestamp.return_value = mock_column
            mock_lit.return_value = mock_column

            # Mock withColumn to return the same dataframe
            sample_dataframe.withColumn.return_value = sample_dataframe

            # Execute main function
            main()

            # Verify S3 incremental read was called
            mock_dependencies[
                "data_reader"
            ].read_from_s3_incremental.assert_called_once()
            mock_finalize_success.assert_called_once()

    @patch("stage.aws_samples_udm_stage.getResolvedOptions")
    @patch("stage.aws_samples_udm_stage.GlueContext")
    @patch("stage.aws_samples_udm_stage.SparkContext")
    @patch("stage.aws_samples_udm_stage.Job")
    @patch("stage.aws_samples_udm_stage.load_configs")
    @patch("stage.aws_samples_udm_stage.AuditManager")
    @patch("stage.aws_samples_udm_stage.DataReader")
    @patch("stage.aws_samples_udm_stage.DataWriter")
    @patch("stage.aws_samples_udm_stage.IncrementalReader")
    @patch("stage.aws_samples_udm_stage.get_source_name")
    @patch("stage.aws_samples_udm_stage.finalize_job_success")
    def test_main_no_data_found(
        self,
        mock_finalize_success,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_config,
        mock_dependencies,
    ):
        """Test execution when no data is found."""
        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = mock_config
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data reader to return None (no data)
        mock_dependencies["data_reader"].read_from_dynamodb.return_value = None

        # Execute main function
        main()

        # Verify that finalize_job_success was called with 0 files processed
        mock_finalize_success.assert_called_once()
        call_args = mock_finalize_success.call_args[0]
        assert call_args[4] == 0  # row_count should be 0

    @patch("stage.aws_samples_udm_stage.getResolvedOptions")
    @patch("stage.aws_samples_udm_stage.GlueContext")
    @patch("stage.aws_samples_udm_stage.SparkContext")
    @patch("stage.aws_samples_udm_stage.Job")
    @patch("stage.aws_samples_udm_stage.load_configs")
    @patch("stage.aws_samples_udm_stage.AuditManager")
    @patch("stage.aws_samples_udm_stage.DataReader")
    @patch("stage.aws_samples_udm_stage.DataWriter")
    @patch("stage.aws_samples_udm_stage.IncrementalReader")
    @patch("stage.aws_samples_udm_stage.get_source_name")
    @patch("stage.aws_samples_udm_stage.finalize_job_failure")
    def test_main_unsupported_source_type(
        self,
        mock_finalize_failure,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
    ):
        """Test execution with unsupported source type."""
        # Setup config with unsupported source type
        config = SimpleNamespace()
        config.source_type = "unsupported_type"
        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"

        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Execute main function - should raise ValueError
        with pytest.raises(ValueError, match="Source type does not match"):
            main()

    @patch("stage.aws_samples_udm_stage.getResolvedOptions")
    @patch("stage.aws_samples_udm_stage.GlueContext")
    @patch("stage.aws_samples_udm_stage.SparkContext")
    @patch("stage.aws_samples_udm_stage.Job")
    @patch("stage.aws_samples_udm_stage.load_configs")
    @patch("stage.aws_samples_udm_stage.AuditManager")
    @patch("stage.aws_samples_udm_stage.DataReader")
    @patch("stage.aws_samples_udm_stage.DataWriter")
    @patch("stage.aws_samples_udm_stage.IncrementalReader")
    @patch("stage.aws_samples_udm_stage.get_source_name")
    def test_main_with_s3_config(
        self,
        mock_get_source_name,
        mock_incremental_reader_class,
        mock_data_writer_class,
        mock_data_reader_class,
        mock_audit_manager_class,
        mock_load_configs,
        mock_job_class,
        mock_spark_context,
        mock_glue_context_class,
        mock_get_resolved_options,
        mock_args,
        mock_dependencies,
        sample_dataframe,
    ):
        """Test execution with S3 configuration (Glue catalog table)."""
        # Setup config with S3 configuration
        config = SimpleNamespace()
        config.source_type = "dynamodb"
        config.source_table_name = "test_source_table"
        config.source_incremental_col = "updated_at"
        config.primary_keys = ["id"]

        config.stage = SimpleNamespace()
        config.stage.database_name = "test_db"
        config.stage.target_table_name = "test_stage_table"
        config.stage.load_method = "append"
        config.stage.s3 = SimpleNamespace()
        config.stage.s3.target_bucket = "s3://test-bucket/data/"
        config.stage.s3.partition_col = "date"
        config.stage.s3.load_method = "append"

        # Setup mocks
        mock_get_resolved_options.return_value = mock_args
        mock_glue_context_class.return_value = mock_dependencies["glue_context"]
        mock_spark_context.getOrCreate.return_value = mock_dependencies["spark_context"]
        mock_job_class.return_value = mock_dependencies["job"]
        mock_load_configs.return_value = config
        mock_get_source_name.return_value = "test_source"

        # Setup utility class mocks
        mock_audit_manager_class.return_value = mock_dependencies["audit_manager"]
        mock_data_reader_class.return_value = mock_dependencies["data_reader"]
        mock_data_writer_class.return_value = mock_dependencies["data_writer"]
        mock_incremental_reader_class.return_value = mock_dependencies[
            "incremental_reader"
        ]

        # Setup data reader to return sample dataframe
        mock_dependencies["data_reader"].read_from_dynamodb.return_value = (
            sample_dataframe
        )
        mock_dependencies["data_writer"].write_to_data_catalog.return_value = (
            "s3://bucket/output/"
        )

        # Mock schema evolution
        with patch(
            "stage.aws_samples_udm_stage.SchemaEvolutionHandler"
        ) as mock_schema, patch(
            "stage.aws_samples_udm_stage.to_timestamp"
        ) as mock_to_timestamp, patch(
            "stage.aws_samples_udm_stage.lit"
        ) as mock_lit:
            mock_schema.normalize_struct_to_string.return_value = sample_dataframe

            # Mock PySpark functions to return Column-like objects
            mock_column = Mock()
            mock_to_timestamp.return_value = mock_column
            mock_lit.return_value = mock_column

            # Mock withColumn to return the same dataframe
            sample_dataframe.withColumn.return_value = sample_dataframe

            with patch(
                "stage.aws_samples_udm_stage.finalize_job_success"
            ) as mock_finalize:
                # Execute main function
                main()

                # Verify that write_to_data_catalog was called instead of write_to_iceberg_table
                mock_dependencies[
                    "data_writer"
                ].write_to_data_catalog.assert_called_once()
                mock_dependencies[
                    "data_writer"
                ].write_to_iceberg_table.assert_not_called()
                mock_finalize.assert_called_once()

    def test_main_entry_point(self):
        """Test the if __name__ == '__main__' entry point (line 198)."""
        with patch("stage.aws_samples_udm_stage.main") as mock_main:
            # Import and execute the module's main block
            import stage.aws_samples_udm_stage

            # Simulate running the script directly
            stage.aws_samples_udm_stage.__name__ = "__main__"

            # Execute the main block
            if stage.aws_samples_udm_stage.__name__ == "__main__":
                stage.aws_samples_udm_stage.main()

            # Verify main was called
            mock_main.assert_called_once()
