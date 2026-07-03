# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Pytest configuration and fixtures for UDM tests."""

# pylint: disable=C0103,C0413

# Mock awsglue modules before any imports
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import mock_awsglue
import pytest


# Mock PySpark modules completely before any imports
def create_mock_pyspark_function():
    """Create a mock PySpark function that returns a mock object."""

    def mock_func(*args, **kwargs):
        mock_result = Mock()
        # Add common DataFrame methods to the mock
        mock_result.cast = Mock(return_value=Mock())
        mock_result.isNull = Mock(return_value=Mock())
        mock_result.otherwise = Mock(return_value=Mock())
        return mock_result

    return mock_func


# Create comprehensive PySpark mocks
mock_pyspark = Mock()
mock_pyspark_sql = Mock()
mock_pyspark_context = Mock()

# Mock all PySpark functions
mock_functions = Mock()
mock_functions.col = create_mock_pyspark_function()
mock_functions.lit = create_mock_pyspark_function()
mock_functions.when = create_mock_pyspark_function()
mock_functions.to_timestamp = create_mock_pyspark_function()
mock_functions.to_date = create_mock_pyspark_function()
mock_functions.current_timestamp = create_mock_pyspark_function()
mock_functions.explode = create_mock_pyspark_function()
mock_functions.trim = create_mock_pyspark_function()
mock_functions.regexp_replace = create_mock_pyspark_function()
mock_functions.input_file_name = create_mock_pyspark_function()

# Mock PySpark types
mock_types = Mock()
mock_types.StringType = Mock
mock_types.IntegerType = Mock
mock_types.FloatType = Mock
mock_types.DoubleType = Mock
mock_types.BooleanType = Mock
mock_types.StructType = Mock
mock_types.StructField = Mock
mock_types.NullType = Mock  # Add NullType mock

# Mock DataFrame and SparkSession
mock_dataframe = Mock()
mock_spark_session = Mock()

# Set up the module mocks BEFORE any imports
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_pyspark_sql
sys.modules["pyspark.sql.functions"] = mock_functions
sys.modules["pyspark.sql.types"] = mock_types
sys.modules["pyspark.context"] = mock_pyspark_context

# Set attributes on the mocked modules
mock_pyspark_sql.DataFrame = mock_dataframe
mock_pyspark_sql.SparkSession = mock_spark_session

# Add current directory to path for mock imports
current_dir = os.path.dirname(__file__)
sys.path.insert(0, current_dir)

sys.modules["awsglue"] = Mock()
sys.modules["awsglue.context"] = mock_awsglue.context
sys.modules["awsglue.dynamicframe"] = mock_awsglue.dynamicframe
sys.modules["awsglue.job"] = mock_awsglue.job
sys.modules["awsglue.transforms"] = mock_awsglue.transforms
sys.modules["awsglue.utils"] = mock_awsglue.utils

# Mock awsgluedq modules
sys.modules["awsgluedq"] = Mock()
sys.modules["awsgluedq.transforms"] = mock_awsglue.awsgluedq_transforms

# Mock boto3 only for specific usage to prevent region errors
# Don't mock the entire boto3 module to avoid conflicts with moto

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Add source paths to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../src"))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../../../../src/glue/udm")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../../../../src/global_utils")
)

# Store original types
_original_StructType = Mock
_original_StructField = Mock


# Create custom StructType mock that accepts field lists
class MockStructType:
    def __init__(self, fields=None):
        self.fields = fields or []

    def __str__(self):
        return f"MockStructType(fields={len(self.fields)})"


# Create custom StructField mock
class MockStructField:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable


# Patch isinstance to handle both original and mocked StructType
original_isinstance = isinstance


def patched_isinstance(obj, class_or_tuple):
    """Patched isinstance that handles mocked StructType."""
    # Handle the case where class_or_tuple is a Mock (like our mocked NullType)
    if hasattr(class_or_tuple, "_mock_name") or str(class_or_tuple).startswith("<Mock"):
        # For mocked types, check if obj is also a mock with similar characteristics
        return hasattr(obj, "_mock_name") or "Mock" in str(obj.__class__)

    # Use original isinstance to avoid recursion
    if hasattr(obj, "__class__") and "Mock" in str(obj.__class__):
        # For mocked objects, check the mock name or attributes
        if class_or_tuple == _original_StructType or class_or_tuple == MockStructType:
            return hasattr(obj, "fields") or "StructType" in str(obj)
        elif hasattr(class_or_tuple, "__name__"):
            return class_or_tuple.__name__.lower() in str(obj).lower()
    elif original_isinstance(obj, MockStructType) and (
        class_or_tuple == _original_StructType or class_or_tuple == MockStructType
    ):
        return True
    return original_isinstance(obj, class_or_tuple)


# Apply the patch globally
import builtins

builtins.isinstance = patched_isinstance

current_module = sys.modules[__name__]
setattr(current_module, "StructType", MockStructType)
setattr(current_module, "StructField", MockStructField)


@pytest.fixture(scope="session")
def spark():
    """Create mock Spark session for testing."""
    mock_spark = Mock()

    def mock_create_dataframe(data, schema):
        mock_df = Mock()
        mock_df.columns = (
            [field.name for field in schema.fields] if hasattr(schema, "fields") else []
        )
        mock_df.schema = schema
        mock_df.count.return_value = len(data)

        # Mock collect to return data as Row objects
        mock_rows = []
        for row_data in data:
            mock_row = Mock()
            for i, field in enumerate(
                schema.fields if hasattr(schema, "fields") else []
            ):
                setattr(
                    mock_row, field.name, row_data[i] if i < len(row_data) else None
                )
                mock_row.__getitem__ = lambda self, key: getattr(self, key)
            mock_rows.append(mock_row)

        mock_df.collect.return_value = mock_rows

        # Improve select to actually filter columns
        def mock_select(*columns):
            selected_df = Mock()
            if columns:
                selected_df.columns = [str(col) for col in columns]
                # Filter rows to only include selected columns
                selected_rows = []
                for row in mock_rows:
                    selected_row = Mock()
                    for col in columns:
                        col_name = str(col)
                        if hasattr(row, col_name):
                            setattr(selected_row, col_name, getattr(row, col_name))
                            selected_row.__getitem__ = lambda self, key: getattr(
                                self, key
                            )
                    selected_rows.append(selected_row)
                selected_df.collect.return_value = selected_rows
            else:
                selected_df.columns = mock_df.columns
                selected_df.collect.return_value = mock_rows
            selected_df.schema = mock_df.schema
            selected_df.count.return_value = len(mock_rows)
            return selected_df

        mock_df.select = mock_select
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.withColumnRenamed = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        mock_df.dropDuplicates = Mock(return_value=mock_df)
        mock_df.orderBy = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.join = Mock(return_value=mock_df)
        mock_df.union = Mock(return_value=mock_df)
        mock_df.distinct = Mock(return_value=mock_df)
        mock_df.limit = Mock(return_value=mock_df)
        mock_df.show = Mock()
        mock_df.printSchema = Mock()
        mock_df.write = Mock()
        mock_df.repartition = Mock(return_value=mock_df)
        mock_df.coalesce = Mock(return_value=mock_df)

        return mock_df

    mock_spark.createDataFrame = mock_create_dataframe
    mock_spark.sql = Mock(return_value=Mock())
    mock_spark.read = Mock()
    mock_spark.conf = Mock()
    mock_spark.sparkContext = Mock()

    return mock_spark


@pytest.fixture
def mock_glue_context():
    """Create mock Glue context."""
    mock_context = Mock()
    mock_context.spark_session = Mock()
    return mock_context


@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing."""
    schema = MockStructType(
        [
            MockStructField("id", Mock(), True),
            MockStructField("name", Mock(), True),
            MockStructField("age", Mock(), True),
        ]
    )

    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    return spark.createDataFrame(data, schema)
