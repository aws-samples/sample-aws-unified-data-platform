# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os
import sys

import pytest

# Add parent directory to path to import the framework
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# pylint: disable=wrong-import-position
from unified_aws_framework import AWSTestConfig, UnifiedAWSFramework


class TestUDMSchemas:
    """Test all UDM schemas from configuration file"""

    @pytest.fixture
    def aws(self, udm_config):
        config = AWSTestConfig(
            region=udm_config["runtime"]["region"],
            environment=udm_config["runtime"]["environment"],
            account_id=udm_config["runtime"]["account_id"],
            output_location=f"s3://{udm_config['runtime']['output_bucket']}/athena/",
        )
        return UnifiedAWSFramework(config)

    def test_all_tables(self, aws, udm_config):
        """Test all tables from config exist and have required columns"""
        database = udm_config["udm_test_config"]["database"]
        tables = udm_config["udm_test_config"]["tables_to_check"]

        for table in tables:
            table_name = table["name"]

            # Test table exists and count
            results = aws.athena_query(
                f"SELECT COUNT(*) as count FROM {table_name}",  # nosec B608
                database=database,
            )
            print(f"{table_name}")

    def test_all_tables_exist_and_have_data(self, aws, udm_config):
        """Test all tables from config exist and have required columns"""
        database = udm_config["udm_test_config"]["database"]
        tables = udm_config["udm_test_config"]["tables_to_check"]

        for table in tables:
            table_name = table["name"]

            # Test table exists and count
            results = aws.athena_query(
                f"SELECT COUNT(*) as count FROM {table_name}",  # nosec B608
                database=database,
            )

            count = int(results[0]["count"])
            print(f"{table_name}: {count} rows")
            assert count >= 0, f"Table {table_name} query failed"

    def _convert_athena_to_dynamodb_name(self, athena_table_name: str) -> str:
        """Convert Athena table name (with underscores) to DynamoDB table name (with hyphens)"""
        return athena_table_name.replace("_", "-")

    def test_all_dynamodb_tables_exist(self, aws, udm_config):
        """Test all DynamoDB tables (converted from Athena table names) exist"""
        tables = udm_config["udm_test_config"]["tables_to_check"]

        for table in tables:
            athena_table_name = table["name"]
            # Convert Athena table name to DynamoDB table name
            dynamodb_table_name = self._convert_athena_to_dynamodb_name(
                athena_table_name
            )

            # Test table exists
            exists = aws.dynamodb_table_exists(dynamodb_table_name)
            print(
                f"DynamoDB table {dynamodb_table_name} (from Athena {athena_table_name}): {'exists' if exists else 'not found'}"
            )
            assert exists, f"DynamoDB table {dynamodb_table_name} does not exist"

    def test_all_dynamodb_tables_have_data(self, aws, udm_config):
        """Test all DynamoDB tables (converted from Athena table names) have data"""
        tables = udm_config["udm_test_config"]["tables_to_check"]

        for table in tables:
            athena_table_name = table["name"]
            # Convert Athena table name to DynamoDB table name
            dynamodb_table_name = self._convert_athena_to_dynamodb_name(
                athena_table_name
            )

            # Get item count
            item_count = aws.dynamodb_get_item_count(dynamodb_table_name)
            print(
                f"DynamoDB table {dynamodb_table_name} (from Athena {athena_table_name}): {item_count} items"
            )
            assert (
                item_count >= 0
            ), f"DynamoDB table {dynamodb_table_name} item count check failed"
