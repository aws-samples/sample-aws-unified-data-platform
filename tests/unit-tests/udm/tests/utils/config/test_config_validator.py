# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Tests for config_validator module."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock, patch

import pytest

from src.glue.udm.utils.config_validator import validate_config_structure


class TestValidateConfigStructure:
    """Test cases for validate_config_structure function."""

    def test_valid_config_structure(self):
        """Test validation with valid config structure."""
        # Create mock config with valid structure
        mock_config = Mock()

        # Mock curation as list
        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1", "col2"]
        mock_config.curation = [mock_target]

        # Mock defaults and columns at root level
        mock_config.defaults = {"string": "default_value"}
        mock_config.columns = ["col1", "col2", "col3"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("✓ Multi-target curation format detected")
            mock_logger.info.assert_any_call("  - Target: test_target")
            mock_logger.info.assert_any_call("    Table: test_table")
            mock_logger.info.assert_any_call("    Columns: 2 defined")
            mock_logger.info.assert_any_call("✓ Defaults found at root level")
            mock_logger.info.assert_any_call("  Types defined: ['string']")
            mock_logger.info.assert_any_call("✓ Columns found at root level")
            mock_logger.info.assert_any_call("  Columns defined: 3")

    def test_invalid_curation_not_list(self):
        """Test validation fails when curation is not a list."""
        mock_config = Mock()
        mock_config.curation = "not_a_list"  # Invalid - should be list

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is False
            mock_logger.info.assert_called_with(
                "✗ ERROR: 'curation' must be an array of target tables"
            )

    def test_curation_none(self):
        """Test validation fails when curation is None."""
        mock_config = Mock()
        mock_config.curation = None

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is False
            mock_logger.info.assert_called_with(
                "✗ ERROR: 'curation' must be an array of target tables"
            )

    def test_target_without_name(self):
        """Test target without name uses default naming."""
        mock_config = Mock()

        # Mock target without name attribute
        mock_target = Mock()
        del mock_target.name  # Remove name attribute
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1"]
        mock_config.curation = [mock_target]

        mock_config.defaults = {"string": "default"}
        mock_config.columns = ["col1"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("  - Target: target_0")

    def test_target_without_table_name(self):
        """Test target without table name shows N/A."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        del mock_target.target_table_name  # Remove table name
        mock_target.columns = ["col1"]
        mock_config.curation = [mock_target]

        mock_config.defaults = {"string": "default"}
        mock_config.columns = ["col1"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("    Table: N/A")

    def test_target_without_columns(self):
        """Test target without columns shows warning."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = []  # Empty columns
        mock_config.curation = [mock_target]

        mock_config.defaults = {"string": "default"}
        mock_config.columns = ["col1"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("    Columns: 0 defined")
            mock_logger.info.assert_any_call(
                "    ✗ WARNING: No columns defined for test_target"
            )

    def test_missing_defaults(self):
        """Test validation fails when defaults are missing."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1"]
        mock_config.curation = [mock_target]

        mock_config.defaults = None  # Missing defaults
        mock_config.columns = ["col1"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is False
            mock_logger.info.assert_any_call("✗ No defaults found at root level")

    def test_missing_columns(self):
        """Test validation fails when columns are missing."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1"]
        mock_config.curation = [mock_target]

        mock_config.defaults = {"string": "default"}
        mock_config.columns = None  # Missing columns

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is False
            mock_logger.info.assert_any_call("✗ No columns found at root level")

    def test_multiple_targets(self):
        """Test validation with multiple targets."""
        mock_config = Mock()

        # Create multiple targets
        mock_target1 = Mock()
        mock_target1.name = "target1"
        mock_target1.target_table_name = "table1"
        mock_target1.columns = ["col1", "col2"]

        mock_target2 = Mock()
        mock_target2.name = "target2"
        mock_target2.target_table_name = "table2"
        mock_target2.columns = ["col3"]

        mock_config.curation = [mock_target1, mock_target2]
        mock_config.defaults = {"string": "default", "int": 0}
        mock_config.columns = ["col1", "col2", "col3"]

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("  - Target: target1")
            mock_logger.info.assert_any_call("    Table: table1")
            mock_logger.info.assert_any_call("    Columns: 2 defined")
            mock_logger.info.assert_any_call("  - Target: target2")
            mock_logger.info.assert_any_call("    Table: table2")
            mock_logger.info.assert_any_call("    Columns: 1 defined")
            mock_logger.info.assert_any_call("  Types defined: ['string', 'int']")

    def test_config_with_getattr_fallbacks(self):
        """Test config validation using getattr with fallback values."""
        mock_config = Mock()

        # Test when attributes don't exist - getattr should return defaults
        mock_config.curation = []  # Empty list but still valid
        mock_config.defaults = {"string": "default"}
        mock_config.columns = {"col1": "type"}

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("✓ Multi-target curation format detected")

    def test_target_with_missing_attributes_using_getattr(self):
        """Test target validation when attributes are missing and getattr is used."""
        mock_config = Mock()

        # Create a target that will trigger getattr fallbacks
        mock_target = Mock(spec=[])  # Empty spec means no attributes
        mock_config.curation = [mock_target]
        mock_config.defaults = {"string": "default"}
        mock_config.columns = {"col1": "type"}

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            # Should use default name when name attribute doesn't exist
            mock_logger.info.assert_any_call("  - Target: target_0")
            # Should show N/A when table_name doesn't exist
            mock_logger.info.assert_any_call("    Table: N/A")
            # Should show 0 columns when columns attribute doesn't exist
            mock_logger.info.assert_any_call("    Columns: 0 defined")
            mock_logger.info.assert_any_call(
                "    ✗ WARNING: No columns defined for target_0"
            )

    def test_defaults_with_keys_method(self):
        """Test defaults validation when defaults has keys() method."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1"]
        mock_config.curation = [mock_target]

        # Mock defaults with keys() method
        mock_defaults = Mock()
        mock_defaults.keys.return_value = ["string", "integer", "float"]
        mock_config.defaults = mock_defaults
        mock_config.columns = {"col1": "type"}

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call(
                "  Types defined: ['string', 'integer', 'float']"
            )

    def test_columns_with_len_method(self):
        """Test columns validation when columns has len() method."""
        mock_config = Mock()

        mock_target = Mock()
        mock_target.name = "test_target"
        mock_target.target_table_name = "test_table"
        mock_target.columns = ["col1", "col2", "col3"]
        mock_config.curation = [mock_target]

        mock_config.defaults = {"string": "default"}

        # Mock columns with len() method
        mock_columns = Mock()
        mock_columns.__len__ = Mock(return_value=5)
        mock_config.columns = mock_columns

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            mock_logger.info.assert_any_call("  Columns defined: 5")

    def test_multiple_targets_with_different_column_counts(self):
        """Test multiple targets with varying column counts including empty."""
        mock_config = Mock()

        # Target with columns
        mock_target1 = Mock()
        mock_target1.name = "target_with_columns"
        mock_target1.target_table_name = "table1"
        mock_target1.columns = ["col1", "col2", "col3"]

        # Target without columns (empty list)
        mock_target2 = Mock()
        mock_target2.name = "target_no_columns"
        mock_target2.target_table_name = "table2"
        mock_target2.columns = []

        # Target with missing columns attribute
        mock_target3 = Mock(spec=[])

        mock_config.curation = [mock_target1, mock_target2, mock_target3]
        mock_config.defaults = {"string": "default"}
        mock_config.columns = {"col1": "type"}

        with patch("src.glue.udm.utils.config_validator.logger") as mock_logger:
            result = validate_config_structure(mock_config)

            assert result is True
            # First target should show 3 columns
            mock_logger.info.assert_any_call("    Columns: 3 defined")
            # Second target should show 0 columns and warning
            mock_logger.info.assert_any_call("    Columns: 0 defined")
            mock_logger.info.assert_any_call(
                "    ✗ WARNING: No columns defined for target_no_columns"
            )
            # Third target should use default name and show warning
            mock_logger.info.assert_any_call("  - Target: target_2")
            mock_logger.info.assert_any_call(
                "    ✗ WARNING: No columns defined for target_2"
            )
