# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#!/usr/bin/env python3
"""Discover actual AWS resources in the environment"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
import boto3
from unified_aws_framework import AWSTestConfig, UnifiedAWSFramework


def discover_resources():
    config = AWSTestConfig(
        region="us-east-1",
        environment="qa",
        account_id="111122223333",
        output_location="s3://udm-curated-output-bucket-qa/athena/",
    )

    # List Glue databases
    glue = boto3.client("glue", region_name="us-east-1")
    try:
        databases = glue.get_databases()
        print("Available Glue Databases:")
        for db in databases["DatabaseList"]:
            print(f"  - {db['Name']}")
    except Exception as e:
        print(f"Error listing databases: {e}")

    # List QuickSight dashboards
    qs = boto3.client("quicksight", region_name="us-east-1")
    try:
        dashboards = qs.list_dashboards(AwsAccountId="111122223333")
        print("\nAvailable QuickSight Dashboards:")
        for dash in dashboards["DashboardSummaryList"]:
            print(f"  - ID: {dash['DashboardId']}, Name: {dash['Name']}")
    except Exception as e:
        print(f"Error listing dashboards: {e}")

    # List QuickSight datasets
    try:
        datasets = qs.list_data_sets(AwsAccountId="111122223333")
        print("\nAvailable QuickSight Datasets:")
        for ds in datasets["DataSetSummaries"]:
            print(f"  - ID: {ds['DataSetId']}, Name: {ds['Name']}")
    except Exception as e:
        print(f"Error listing datasets: {e}")


if __name__ == "__main__":
    discover_resources()
