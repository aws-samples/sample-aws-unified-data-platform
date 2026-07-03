# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for config_reader module."""

# pylint: disable=C0301,C0415

import json
from types import SimpleNamespace
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml
from utils.config.config_reader import dict_to_namespace, load_configs, load_s3_config


class TestConfigReader:
    """Test cases for config reader functions."""

    @pytest.fixture
    def sample_yaml_config(self):
        """Create sample YAML configuration."""
        return """
source_type: "dynamodb"
source_table_name: "test_table"
source_incremental_col: "updated_at"

stage:
  database_name: "test_db"
  target_table_name: "test_stage_table"
  load_method: "append"

curation:
  - name: "curation_1"
    database_name: "test_db"
    target_table_name: "test_curated_table_1"
    load_method: "overwrite"
    columns: ["id", "name"]
  - name: "curation_2"
    database_name: "test_db"
    target_table_name: "test_curated_table_2"
    load_method: "append"
    columns: ["id", "age"]

defaults:
  string:
    missing_value_default: "Unknown"
  integer:
    missing_value_default: 0
  float:
    missing_value_default: 0.0

columns:
  name:
    type: "string"
    missing_value_default: "N/A"
  age:
    type: "integer"
    missing_value_default: 0
  salary:
    type: "float"
"""

    @pytest.fixture
    def sample_json_config(self):
        """Create sample JSON configuration."""
        return """{
  "source_type": "parquet",
  "source_s3_path": "s3://bucket/data/",
  "stage": {
    "database_name": "test_db",
    "target_table_name": "test_stage_table",
    "load_method": "append"
  },
  "defaults": {
    "string": {
      "missing_value_default": "Unknown"
    }
  },
  "columns": {
    "name": {
      "type": "string"
    }
  }
}"""

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_from_s3_yaml(self, mock_boto3_client, sample_yaml_config):
        """Test loading YAML configuration from S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=sample_yaml_config.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        config = load_configs("s3://bucket/config.yaml")

        # Verify configuration structure
        assert isinstance(config, SimpleNamespace)
        assert config.source_type == "dynamodb"
        assert config.source_table_name == "test_table"
        assert config.source_incremental_col == "updated_at"

        # Verify nested structures
        assert hasattr(config, "stage")
        assert config.stage.database_name == "test_db"
        assert config.stage.target_table_name == "test_stage_table"
        assert config.stage.load_method == "append"

        # Verify curation list
        assert hasattr(config, "curation")
        assert isinstance(config.curation, list)
        assert len(config.curation) == 2
        assert config.curation[0]["name"] == "curation_1"
        assert config.curation[1]["name"] == "curation_2"

        # Verify defaults and columns
        assert hasattr(config, "defaults")
        assert hasattr(config, "columns")
        assert config.defaults.string.missing_value_default == "Unknown"
        assert config.columns.name.type == "string"

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_from_s3_json(self, mock_boto3_client, sample_json_config):
        """Test loading JSON configuration from S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=sample_json_config.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        config = load_configs("s3://bucket/config.json")

        # Verify configuration structure
        assert isinstance(config, SimpleNamespace)
        assert config.source_type == "parquet"
        assert config.source_s3_path == "s3://bucket/data/"

        # Verify nested structures
        assert hasattr(config, "stage")
        assert config.stage.database_name == "test_db"

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_s3_file_not_found(self, mock_boto3_client):
        """Test handling S3 file not found error."""
        # Mock S3 client to raise exception
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")
        mock_boto3_client.return_value = mock_s3

        # Should raise exception
        with pytest.raises(Exception, match="NoSuchKey"):
            load_configs("s3://bucket/nonexistent.yaml")

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_invalid_yaml(self, mock_boto3_client):
        """Test handling invalid YAML content."""
        invalid_yaml = "invalid: yaml: content: ["

        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=invalid_yaml.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Should raise ValueError with proper message
        with pytest.raises(ValueError, match="Failed to load configuration"):
            load_configs("s3://bucket/invalid.yaml")

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_invalid_json(self, mock_boto3_client):
        """Test handling invalid JSON content."""
        invalid_json = '{"invalid": json content'

        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=invalid_json.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Should raise JSON parsing exception
        with pytest.raises(Exception):  # JSON parsing will raise ValueError or similar
            load_configs("s3://bucket/invalid.json")

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_complex_nested_structure(self, mock_boto3_client):
        """Test loading configuration with complex nested structures."""
        complex_yaml = """
source_type: "dynamodb"
nested:
  level1:
    level2:
      level3:
        value: "deep_value"
  array_of_objects:
    - name: "item1"
      value: 1
    - name: "item2"
      value: 2
transformations:
  field1:
    type: "string"
    rules:
      - action: "clean"
        params: ["trim", "lowercase"]
      - action: "validate"
        params: {"min_length": 1}
"""

        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=complex_yaml.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        config = load_configs("s3://bucket/complex.yaml")

        # Verify complex nested structure
        assert config.nested.level1.level2.level3.value == "deep_value"
        assert len(config.nested.array_of_objects) == 2
        assert config.nested.array_of_objects[0]["name"] == "item1"
        assert config.transformations.field1.type == "string"
        assert len(config.transformations.field1.rules) == 2

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_empty_file(self, mock_boto3_client):
        """Test handling empty configuration file."""
        # Mock S3 client with empty content
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {"Body": Mock(read=Mock(return_value=b""))}
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        config = load_configs("s3://bucket/empty.yaml")

        # Should return SimpleNamespace with no attributes for empty config
        assert isinstance(config, SimpleNamespace)

    @patch("utils.config.config_reader.boto3.client")
    def test_load_configs_yaml_with_null_values(self, mock_boto3_client):
        """Test handling YAML with null values."""
        yaml_with_nulls = """
source_type: "dynamodb"
optional_field: null
nested:
  required_field: "value"
  optional_field: null
"""

        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=yaml_with_nulls.encode()))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        config = load_configs("s3://bucket/nulls.yaml")

        # Verify null values are handled correctly
        assert config.source_type == "dynamodb"
        assert config.optional_field is None
        assert config.nested.required_field == "value"
        assert config.nested.optional_field is None


class TestDictToNamespace:
    """Test cases for dict_to_namespace function."""

    def test_dict_to_namespace_none_input(self):
        """Test dict_to_namespace with None input."""
        result = dict_to_namespace(None)
        assert result is None

    def test_dict_to_namespace_non_dict_input(self):
        """Test dict_to_namespace with non-dict input."""
        # Test with string
        result = dict_to_namespace("test_string")
        assert result == "test_string"

        # Test with number
        result = dict_to_namespace(42)
        assert result == 42

        # Test with list
        test_list = [1, 2, 3]
        result = dict_to_namespace(test_list)
        assert result == test_list

    def test_dict_to_namespace_simple_dict(self):
        """Test dict_to_namespace with simple dictionary."""
        test_dict = {"key1": "value1", "key2": "value2"}
        result = dict_to_namespace(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert result.key1 == "value1"
        assert result.key2 == "value2"

    def test_dict_to_namespace_nested_dict(self):
        """Test dict_to_namespace with nested dictionary."""
        test_dict = {
            "level1": {"level2": {"value": "nested_value"}},
            "simple": "simple_value",
        }
        result = dict_to_namespace(test_dict)

        assert isinstance(result, SimpleNamespace)
        assert isinstance(result.level1, SimpleNamespace)
        assert isinstance(result.level1.level2, SimpleNamespace)
        assert result.level1.level2.value == "nested_value"
        assert result.simple == "simple_value"


class TestLoadS3Config:
    """Test cases for load_s3_config function."""

    def test_load_s3_config_invalid_path_format(self):
        """Test load_s3_config with invalid S3 path format."""
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            load_s3_config("invalid://path/config.yaml")

    @patch("utils.config.config_reader.boto3.client")
    def test_load_s3_config_bucket_only_path(self, mock_boto3_client):
        """Test load_s3_config with bucket-only path (no key)."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b"test: value"))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration with bucket-only path
        result = load_s3_config("s3://bucket-name")

        # Verify S3 client was called with empty key
        mock_s3.get_object.assert_called_once_with(Bucket="bucket-name", Key="")
        assert result == {"test": "value"}

    @patch("utils.config.config_reader.boto3.client")
    def test_load_s3_config_with_key(self, mock_boto3_client):
        """Test load_s3_config with bucket and key."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b"test: value"))
        }
        mock_boto3_client.return_value = mock_s3

        # Load configuration
        result = load_s3_config("s3://bucket-name/path/to/config.yaml")

        # Verify S3 client was called correctly
        mock_s3.get_object.assert_called_once_with(
            Bucket="bucket-name", Key="path/to/config.yaml"
        )
        assert result == {"test": "value"}

    @patch("utils.config.config_reader.boto3.client")
    def test_load_s3_config_boto3_exception(self, mock_boto3_client):
        """Test load_s3_config with boto3 exception."""
        # Mock S3 client to raise exception
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = Exception("S3 error")
        mock_boto3_client.return_value = mock_s3

        # Should raise ValueError with proper message
        with pytest.raises(
            ValueError,
            match="Failed to load configuration from s3://bucket/config.yaml: S3 error",
        ):
            load_s3_config("s3://bucket/config.yaml")


class TestLoadConfigsIntegration:
    """Integration test cases for load_configs function."""

    @patch("utils.config.config_reader.load_s3_config")
    def test_load_configs_s3_path(self, mock_load_s3):
        """Test load_configs with S3 path."""
        mock_load_s3.return_value = {"test": "value"}

        result = load_configs("s3://bucket/config.yaml")

        mock_load_s3.assert_called_once_with("s3://bucket/config.yaml")
        assert isinstance(result, SimpleNamespace)
        assert result.test == "value"
