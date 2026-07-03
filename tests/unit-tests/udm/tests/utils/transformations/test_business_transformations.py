# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import unittest
from unittest.mock import Mock, patch

from src.glue.udm.utils.transformations.business_transformations import (
    BusinessTransformations,
)


class TestBusinessTransformations(unittest.TestCase):
    """Test cases for BusinessTransformations class."""

    def test_normalize_values_basic(self):
        """Test basic value normalization."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.business_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.business_transformations.lit"
            ) as mock_lit:
                with patch(
                    "src.glue.udm.utils.transformations.business_transformations.when"
                ) as mock_when:
                    mock_col.return_value = Mock()
                    mock_lit.return_value = Mock()
                    mock_when.return_value.otherwise.return_value = Mock()
                    mock_df.withColumn.return_value = mock_result_df

                    normalization_mapping = {"old_value": "new_value", "test": "result"}

                    result = BusinessTransformations.normalize_values(
                        mock_df, "test_column", normalization_mapping
                    )

                    assert result == mock_result_df
                    mock_df.withColumn.assert_called_once()

    def test_normalize_values_empty_mapping(self):
        """Test normalization with empty mapping."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.business_transformations.col"
        ) as mock_col:
            mock_col.return_value = Mock()
            mock_df.withColumn.return_value = mock_result_df

            result = BusinessTransformations.normalize_values(
                mock_df, "test_column", {}
            )

            assert result == mock_result_df
            mock_df.withColumn.assert_called_once()

    def test_normalize_values_single_mapping(self):
        """Test normalization with single mapping."""
        mock_df = Mock()
        mock_result_df = Mock()

        with patch(
            "src.glue.udm.utils.transformations.business_transformations.col"
        ) as mock_col:
            with patch(
                "src.glue.udm.utils.transformations.business_transformations.lit"
            ) as mock_lit:
                with patch(
                    "src.glue.udm.utils.transformations.business_transformations.when"
                ) as mock_when:
                    mock_col.return_value = Mock()
                    mock_lit.return_value = Mock()
                    mock_when.return_value.otherwise.return_value = Mock()
                    mock_df.withColumn.return_value = mock_result_df

                    normalization_mapping = {"A": "B"}

                    result = BusinessTransformations.normalize_values(
                        mock_df, "column", normalization_mapping
                    )

                    assert result == mock_result_df
                    mock_when.assert_called_once()
                    mock_lit.assert_called_once_with("B")


if __name__ == "__main__":
    unittest.main()
