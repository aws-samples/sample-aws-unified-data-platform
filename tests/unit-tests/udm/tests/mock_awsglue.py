# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Mock AWS Glue modules for unit testing."""

from unittest.mock import MagicMock, Mock

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class MockDynamicFrame:
    """Mock DynamicFrame class."""

    def __init__(
        self,
        jdf=None,
        glue_ctx=None,
        name="",
        transformation_ctx="",
        info=None,
        schema=None,
        additional_options=None,
    ):
        self.jdf = jdf
        self.glue_ctx = glue_ctx
        self.name = name
        self.transformation_ctx = transformation_ctx
        self.info = info or {}
        self.schema = schema
        self.additional_options = additional_options or {}
        self._df = None

    def to_df(self):
        """Convert to DataFrame."""
        if self._df is not None:
            return self._df
        # Return a mock DataFrame with proper methods
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.show = Mock()
        mock_df.print_schema = Mock()
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        mock_df._jdf = Mock()
        mock_df._jdf.count.return_value = 10
        return mock_df

    def toDF(self):  # pylint: disable=invalid-name
        """Convert to DataFrame (camelCase for compatibility)."""
        return self.to_df()

    def count(self):
        """Return count."""
        return 10

    def show(self, n=20):
        """Show data."""
        pass

    def print_schema(self):
        """Print schema."""

    def printSchema(self):  # pylint: disable=invalid-name
        """Print schema (camelCase for compatibility)."""
        self.print_schema()

    def select_fields(self, paths):
        """Select fields."""
        return MockDynamicFrame()

    def drop_fields(self, paths):
        """Drop fields."""
        return MockDynamicFrame()

    def rename_field(self, old_name, new_name):
        """Rename field."""
        return MockDynamicFrame()

    def apply_mapping(self, mappings):
        """Apply mapping."""
        return MockDynamicFrame()

    def resolve_choice(self, specs):
        """Resolve choice."""
        return MockDynamicFrame()

    def resolveChoice(self, specs):  # pylint: disable=invalid-name
        """Resolve choice (camelCase for compatibility)."""
        return self.resolve_choice(specs)

    def filter(self, f):
        """Filter data."""
        return MockDynamicFrame()

    def map(self, f):
        """Map function."""
        return MockDynamicFrame()

    def write(
        self,
        connection_type,
        connection_options,
        format=None,
        format_options=None,
        transformation_ctx="",
    ):
        """Write data."""
        pass


class MockGlueContext:
    """Mock GlueContext class."""

    def __init__(self, spark_context=None):
        self.spark_context = spark_context
        self.spark_session = Mock()

    def create_dynamic_frame_from_options(
        self,
        connection_type,
        connection_options,
        format=None,
        format_options=None,
        transformation_ctx="",
        push_down_predicate="",
    ):
        """Create DynamicFrame from options."""
        df = MockDynamicFrame()
        df._df = Mock(spec=DataFrame)
        df._df.count.return_value = 10
        df._df.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        return df

    def create_dynamic_frame_from_catalog(
        self,
        database,
        table_name,
        transformation_ctx="",
        push_down_predicate="",
        additional_options=None,
    ):
        """Create DynamicFrame from catalog."""
        df = MockDynamicFrame()
        df._df = Mock(spec=DataFrame)
        df._df.count.return_value = 10
        df._df.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        return df

    def create_dynamic_frame_from_rdd(
        self, data, name, schema=None, sample_ratio=None, transformation_ctx=""
    ):
        """Create DynamicFrame from RDD."""
        return MockDynamicFrame()

    def write_dynamic_frame_from_options(
        self,
        frame,
        connection_type,
        connection_options,
        format=None,
        format_options=None,
        transformation_ctx="",
    ):
        """Write DynamicFrame using options."""
        pass

    def write_dynamic_frame_from_catalog(
        self,
        frame,
        database,
        table_name,
        transformation_ctx="",
        additional_options=None,
    ):
        """Write DynamicFrame to catalog."""
        pass

    def get_logger(self):
        """Get logger."""
        return Mock()


class MockJob:
    """Mock Job class."""

    @staticmethod
    def init(args, job_name):
        """Initialize job."""
        pass

    @staticmethod
    def commit():
        """Commit job."""
        pass


class MockEvaluateDataQuality:
    """Mock EvaluateDataQuality transform."""

    def process_rows(
        self, frame, ruleset, publishing_options=None, additional_options=None
    ):
        """Mock process_rows method."""
        # Create mock result with rowLevelOutcomes
        mock_result = Mock()

        # Create mock row-level outcomes DynamicFrame
        mock_outcomes = MockDynamicFrame()
        mock_outcomes._df = Mock(spec=DataFrame)
        mock_outcomes._df.count.return_value = 10

        # Add DataQualityEvaluationResult column with "Passed" values
        mock_outcomes._df.filter.return_value = mock_outcomes._df
        mock_outcomes._df.drop.return_value = mock_outcomes._df
        mock_outcomes._df.columns = [
            "id",
            "name",
            "DataQualityEvaluationResult",
            "DataQualityRulesPass",
            "DataQualityRulesFail",
            "DataQualityRulesSkip",
        ]

        # Mock the collection result
        mock_result_collection = Mock()
        mock_result_collection.select.return_value = mock_outcomes

        return mock_result_collection


class MockSelectFromCollection:
    """Mock SelectFromCollection transform."""

    @staticmethod
    def apply(dfc, key):
        """Mock apply method."""
        mock_frame = MockDynamicFrame()
        mock_frame._df = Mock(spec=DataFrame)
        mock_frame._df.count.return_value = 10
        mock_frame._df.filter.return_value = mock_frame._df
        mock_frame._df.drop.return_value = mock_frame._df
        mock_frame._df.columns = [
            "id",
            "name",
            "DataQualityEvaluationResult",
            "DataQualityRulesPass",
            "DataQualityRulesFail",
            "DataQualityRulesSkip",
        ]
        return mock_frame


# Mock modules
dynamicframe = Mock()
dynamicframe.DynamicFrame = MockDynamicFrame

context = Mock()
context.GlueContext = MockGlueContext

job = Mock()
job.Job = MockJob

transforms = Mock()
transforms.SelectFromCollection = MockSelectFromCollection
utils = Mock()

# Mock awsgluedq module
awsgluedq_transforms = Mock()
awsgluedq_transforms.EvaluateDataQuality = MockEvaluateDataQuality
