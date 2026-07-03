# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3

glue = boto3.client("glue", region_name="us-east-1")

try:
    databases = glue.get_databases()
    print("Available Athena/Glue Databases:")
    for db in databases["DatabaseList"]:
        name = db["Name"]
        print(f"  - {name}")

        # Get tables for each database
        try:
            tables = glue.get_tables(DatabaseName=name)
            if tables["TableList"]:
                print(f"    Tables:")
                for table in tables["TableList"]:
                    print(f"      - {table['Name']}")
            else:
                print(f"    No tables")
        except Exception as e:
            print(f"    Error getting tables: {e}")
        print()

except Exception as e:
    print(f"Error: {e}")
