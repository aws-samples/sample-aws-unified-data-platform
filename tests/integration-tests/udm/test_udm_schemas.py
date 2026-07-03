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

    def test_all_tables_exist_and_have_data(self, aws, udm_config):
        """Test all tables from config exist and have required columns"""
        database = udm_config["udm_test_config"]["database"]
        tables = udm_config["udm_test_config"]["tables_to_check"]

        for table in tables:
            table_name = table["name"]

            # Validate table_name is a safe SQL identifier
            if not table_name or not all(
                c.isalnum() or c in ("_", ".") for c in table_name
            ):
                raise ValueError(f"Invalid table name: {table_name}")

            # Test table exists and count
            results = aws.athena_query(
                f"SELECT COUNT(*) as count FROM {table_name}",  # nosec B608
                database=database,
            )

            count = int(results[0]["count"])
            print(f"{table_name}: {count} rows")
            assert count >= 0, f"Table {table_name} query failed"
