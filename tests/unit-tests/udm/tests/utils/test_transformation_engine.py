# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for transformation_engine module - Fixed and Merged."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from utils.transformation_engine import (
    _align_with_target_schema,
    apply_rule_based_transformations,
)


class TestTransformationEngine:
    """Test cases for transformation engine functions."""

    @pytest.fixture
    def basic_config(self):
        """Create basic transformation configuration without Spark-triggering fields."""
        return {
            "defaults": {
                "string": {"missing_value_default": "Unknown"},
                "integer": {"missing_value_default": 0},
                "float": {"missing_value_default": 0.0},
            },
            "columns": {},
        }

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock()
        mock_df.columns = ["test_col"]
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.drop.return_value = mock_df
        return mock_df

    # Coverage improvement tests - these are the key ones that were missing
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_rename_from_column_not_exists(self, mock_dq, mock_core, basic_config):
        """Test rename_from when source column doesn't exist - line 59."""
        mock_df = Mock()
        mock_df.columns = ["existing_col"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["new_field"] = {
            "type": "string",
            "rename_from": "non_existent_col",
        }

        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should not call rename_column since source doesn't exist
        mock_core.rename_column.assert_not_called()
        assert result_df is not None

    @patch("utils.transformation_engine.NestedDataTransformations")
    @patch("utils.transformation_engine.lit")
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_source_path_single_exception(
        self, mock_dq, mock_core, mock_lit, mock_nested, basic_config
    ):
        """Test single source path with exception - line 84."""
        mock_df = Mock()
        mock_df.columns = ["data"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["extracted"] = {
            "type": "string",
            "source_path": "field.nested",
        }

        mock_nested.flatten_column.side_effect = Exception("Flatten failed")
        mock_lit.return_value = "null_value"
        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_lit.assert_called_with(None)
        assert result_df is not None

    @patch("utils.transformation_engine.NestedDataTransformations")
    @patch("utils.transformation_engine.lit")
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_source_path_multiple_no_temp_cols(
        self, mock_dq, mock_core, mock_lit, mock_nested, basic_config
    ):
        """Test multiple source paths with no successful extractions - line 94."""
        mock_df = Mock()
        mock_df.columns = ["data"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["extracted"] = {
            "type": "string",
            "source_path": ["field1", "field2"],
        }

        mock_nested.flatten_column.side_effect = Exception("All paths fail")
        mock_lit.return_value = "null_value"
        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should create column with null when all paths fail
        mock_df.withColumn.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.NestedDataTransformations")
    @patch("utils.transformation_engine.coalesce")
    @patch("utils.transformation_engine.col")
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_source_path_multiple_temp_cleanup(
        self, mock_dq, mock_core, mock_col, mock_coalesce, mock_nested, basic_config
    ):
        """Test multiple source paths temp column cleanup - lines 104-106."""
        mock_df = Mock()
        mock_df.columns = ["data", "extracted_temp_0", "extracted_temp_1"]
        mock_df.withColumn.return_value = mock_df
        mock_df.drop.return_value = mock_df

        basic_config["columns"]["extracted"] = {
            "type": "string",
            "source_path": ["field1", "field2"],
        }

        mock_nested.flatten_column.return_value = mock_df
        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df
        mock_col.return_value = "mock_col"
        mock_coalesce.return_value = "mock_coalesce"

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should drop temp columns
        mock_df.drop.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.DataQuality")
    @patch("utils.transformation_engine.CoreTransformations")
    def test_string_missing_default_from_type_defaults(
        self, mock_core, mock_dq, basic_config
    ):
        """Test string missing default from type defaults - line 130."""
        mock_df = Mock()
        mock_df.columns = ["text_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["text_field"] = {
            "type": "string"
            # No missing_value_default specified, should use type defaults
        }

        mock_dq.handle_missing_values.return_value = mock_df
        mock_core.string_cleaning.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_dq.handle_missing_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_string_corrections_not_provided(self, mock_dq, mock_core, basic_config):
        """Test string without corrections - line 132."""
        mock_df = Mock()
        mock_df.columns = ["text_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["text_field"] = {
            "type": "string"
            # No corrections specified
        }

        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should not call apply_corrections_mapping
        mock_core.apply_corrections_mapping.assert_not_called()
        assert result_df is not None

    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_string_skip_cleaning_true(self, mock_dq, mock_core, basic_config):
        """Test string with skip_cleaning=True - line 135."""
        mock_df = Mock()
        mock_df.columns = ["text_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["text_field"] = {
            "type": "string",
            "skip_cleaning": True,
        }

        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should not call string_cleaning
        mock_core.string_cleaning.assert_not_called()
        assert result_df is not None

    @patch("utils.transformation_engine.DataQuality")
    @patch("utils.transformation_engine.CoreTransformations")
    def test_numeric_missing_default_from_type_defaults(
        self, mock_core, mock_dq, basic_config
    ):
        """Test numeric missing default from type defaults - line 161."""
        mock_df = Mock()
        mock_df.columns = ["num_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["num_field"] = {
            "type": "integer"
            # No missing_value_default specified, should use type defaults
        }

        mock_dq.handle_missing_values.return_value = mock_df
        mock_core.convert_data_types.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_dq.handle_missing_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.CoreTransformations")
    def test_numeric_skip_cleaning_true(self, mock_core, basic_config):
        """Test numeric with skip_cleaning=True - line 181."""
        mock_df = Mock()
        mock_df.columns = ["num_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["num_field"] = {
            "type": "integer",
            "skip_cleaning": True,
        }

        mock_core.convert_data_types.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        # Should call convert_data_types but not handle_missing_values
        mock_core.convert_data_types.assert_called()
        assert result_df is not None

    # Additional comprehensive tests
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_basic_transformations(self, mock_dq, mock_core, basic_config):
        """Test basic rule-based transformations."""
        mock_df = Mock()
        mock_df.columns = ["name", "age"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["name"] = {"type": "string"}
        basic_config["columns"]["age"] = {"type": "integer"}

        mock_dq.handle_missing_values.return_value = mock_df
        mock_core.string_cleaning.return_value = mock_df
        mock_core.convert_data_types.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        assert result_df is not None
        mock_dq.handle_missing_values.assert_called()
        mock_core.string_cleaning.assert_called()

    @patch("utils.transformation_engine.DateTransformations")
    @patch("utils.transformation_engine.CoreTransformations")
    def test_date_overrides_from_type_defaults(
        self, mock_core, mock_date, basic_config
    ):
        """Test date overrides from type defaults - lines 187-188."""
        mock_df = Mock()
        mock_df.columns = ["date_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["defaults"]["date"] = {"date_overrides": {"invalid": "2000-01-01"}}
        basic_config["columns"]["date_field"] = {
            "type": "date"
            # No date_overrides specified, should use type defaults
        }

        mock_date.clean_date_strings.return_value = mock_df
        mock_date.convert_date_format.return_value = mock_df
        mock_core.convert_data_types.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_date.clean_date_strings.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.DateTransformations")
    def test_timestamp_format_conversion(self, mock_date, basic_config):
        """Test timestamp format conversion - lines 198-210."""
        mock_df = Mock()
        mock_df.columns = ["ts_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["ts_field"] = {
            "type": "timestamp",
            "input_format": "yyyy-MM-dd HH:mm:ss",
            "target_format": "MM/dd/yyyy HH:mm:ss",
        }

        mock_date.clean_date_strings.return_value = mock_df
        mock_date.convert_timestamp_format.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_date.convert_timestamp_format.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.logger")
    @patch("utils.transformation_engine.col")
    def test_align_with_target_schema_cast_exception(self, mock_col, mock_logger):
        """Test _align_with_target_schema with cast exception."""
        mock_df = Mock()
        mock_df.columns = ["test_col"]
        mock_df.dtypes = [("test_col", "string")]

        # Mock col to raise an exception during cast
        mock_col.return_value.cast.side_effect = Exception("Cast failed")

        target_schema = {"test_col": "int"}

        with pytest.raises(Exception):
            _align_with_target_schema(mock_df, target_schema)

        mock_logger.error.assert_called()

    @patch("utils.transformation_engine.col")
    @patch("utils.transformation_engine.lit")
    def test_align_with_target_schema_string_to_array(self, mock_lit, mock_col):
        """Test _align_with_target_schema with string to array cast."""
        mock_df = Mock()
        mock_df.columns = ["test_col"]
        mock_df.dtypes = [("test_col", "string")]
        mock_df.withColumn.return_value = mock_df

        mock_col.return_value = "mock_col"
        mock_lit.return_value.cast.return_value = "mock_cast"

        target_schema = {"test_col": "array<string>"}

        result_df = _align_with_target_schema(mock_df, target_schema)

        # Should use lit(None).cast for incompatible cast
        mock_lit.assert_called_with(None)
        assert result_df is not None

    def test_align_with_target_schema_no_matching_columns(self):
        """Test _align_with_target_schema with no matching columns."""
        mock_df = Mock()
        mock_df.columns = ["other_col"]

        target_schema = {"test_col": "int"}

        result_df = _align_with_target_schema(mock_df, target_schema)

        # Should return df unchanged when no matching columns
        assert result_df == mock_df

    def test_align_with_target_schema_same_types(self):
        """Test _align_with_target_schema when types already match."""
        mock_df = Mock()
        mock_df.columns = ["test_col"]
        mock_df.dtypes = [("test_col", "int")]

        target_schema = {"test_col": "int"}

        result_df = _align_with_target_schema(mock_df, target_schema)

        # Should return df unchanged when types match
        mock_df.withColumn.assert_not_called()
        assert result_df == mock_df

    # Test various transformation types
    @patch("utils.transformation_engine.DataQuality")
    @patch("utils.transformation_engine.CoreTransformations")
    def test_fee_rate_invalid_values_from_type_defaults(
        self, mock_core, mock_dq, basic_config
    ):
        """Test fee_rate invalid values from type defaults - lines 245-259."""
        mock_df = Mock()
        mock_df.columns = ["rate_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["defaults"]["fee_rate"] = {"invalid_values": ["%", "N/A"]}
        basic_config["columns"]["rate_field"] = {
            "type": "fee_rate"
            # No invalid_values specified, should use type defaults
        }

        mock_dq.clean_invalid_values.return_value = mock_df
        mock_core.convert_data_types.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_dq.clean_invalid_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.DataQuality")
    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.BusinessTransformations")
    def test_value_normalization_with_normalization_key(
        self, mock_business, mock_core, mock_dq, basic_config
    ):
        """Test value normalization with normalization key - lines 269-273."""
        mock_df = Mock()
        mock_df.columns = ["business_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["defaults"]["value_normalization"] = {
            "missing_value_default": "Unknown",
            "business_line": {"old_value": "new_value"},
        }
        basic_config["columns"]["business_field"] = {
            "type": "value_normalization",
            "normalization_key": "business_line",
        }

        mock_dq.handle_missing_values.return_value = mock_df
        mock_core.string_cleaning.return_value = mock_df
        mock_business.normalize_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_business.normalize_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.DataQuality")
    def test_struct_missing_default_from_type_defaults(self, mock_dq, basic_config):
        """Test struct missing default from type defaults - lines 295-299."""
        mock_df = Mock()
        mock_df.columns = ["struct_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["defaults"]["struct"] = {"missing_value_default": "{}"}
        basic_config["columns"]["struct_field"] = {
            "type": "struct"
            # No missing_value_default specified, should use type defaults
        }

        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_dq.handle_missing_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_boolean_with_missing_default(self, mock_dq, mock_core, basic_config):
        """Test boolean with missing_value_default - lines 311-315."""
        mock_df = Mock()
        mock_df.columns = ["bool_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["bool_field"] = {
            "type": "boolean",
            "true_values": ["yes", "true"],
            "false_values": ["no", "false"],
            "missing_value_default": False,
        }

        mock_core.convert_to_boolean.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(mock_df, basic_config)

        mock_dq.handle_missing_values.assert_called()
        assert result_df is not None

    @patch("utils.transformation_engine.ArrayTransformations")
    @patch("utils.transformation_engine.logger")
    def test_array_explode_exception(self, mock_logger, mock_array, basic_config):
        """Test array explode with exception - lines 329-330."""
        mock_df = Mock()
        mock_df.columns = ["array_field"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["array_field"] = {
            "type": "array_explode",
            "explode_columns": {"item": {"source_field": "value"}},
        }

        mock_array.explode_array_column.side_effect = Exception("Explode failed")

        with pytest.raises(Exception):
            apply_rule_based_transformations(mock_df, basic_config)

        mock_logger.error.assert_called()

    @patch("utils.transformation_engine.MetadataExtractor")
    @patch("utils.transformation_engine.logger")
    def test_metadata_extractor_exception(
        self, mock_logger, mock_extractor, basic_config
    ):
        """Test metadata extractor with exception - lines 351-373."""
        mock_df = Mock()
        mock_df.columns = ["_raw_data"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["extracted"] = {
            "type": "metadata_extractor",
            "source_column": "_raw_data",
            "explode_columns": {"field1": {"source_field": "field1"}},
        }

        mock_extractor.extract_metadata_fields.side_effect = Exception(
            "Extraction failed"
        )

        with pytest.raises(Exception):
            apply_rule_based_transformations(mock_df, basic_config)

        mock_logger.error.assert_called()

    @patch("utils.transformation_engine.S3Transformations")
    @patch("utils.transformation_engine.logger")
    def test_s3_prefix_extractor_exception(self, mock_logger, mock_s3, basic_config):
        """Test S3 prefix extractor with exception."""
        mock_df = Mock()
        mock_df.columns = ["source_file_name"]
        mock_df.withColumn.return_value = mock_df

        basic_config["columns"]["extracted_prefix"] = {
            "type": "s3_prefix_extractor",
            "source_column": "source_file_name",
            "position": 1,
        }

        mock_s3.extract_from_s3_prefix.side_effect = Exception("S3 extraction failed")

        with pytest.raises(Exception):
            apply_rule_based_transformations(mock_df, basic_config)

        mock_logger.error.assert_called()

    @patch("utils.transformation_engine.CoreTransformations")
    @patch("utils.transformation_engine.DataQuality")
    def test_column_selection_no_final_columns(self, mock_dq, mock_core, basic_config):
        """Test column selection when no final columns exist."""
        mock_df = Mock()
        mock_df.columns = ["other_field"]

        basic_config["columns"]["field1"] = {"type": "string"}

        mock_core.string_cleaning.return_value = mock_df
        mock_dq.handle_missing_values.return_value = mock_df

        result_df = apply_rule_based_transformations(
            mock_df, basic_config, select_columns=True
        )

        # Should not call select when no matching columns
        mock_df.select.assert_not_called()
        assert result_df is not None
