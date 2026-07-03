# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3

sts = boto3.client("sts")
identity = sts.get_caller_identity()
print(f"Current AWS Account: {identity['Account']}")
print(f"User ARN: {identity['Arn']}")

glue = boto3.client("glue", region_name="us-east-1")
try:
    databases = glue.get_databases()
    print(f"\nDatabases found: {len(databases['DatabaseList'])}")
    for db in databases["DatabaseList"]:
        print(f"  - {db['Name']}")
except Exception as e:
    print(f"Error: {e}")
