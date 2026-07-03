# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Unit tests for DedupeHandler class."""

# pylint: disable=C0301,C0415

from unittest.mock import Mock

import pytest
from utils.transformations.dedupe_module import DedupeHandler


class TestDedupeHandler:
    """Test cases for DedupeHandler class."""

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock()
        mock_df.distinct.return_value = mock_df
        return mock_df

    def test_dedupe_dataframe_with_valid_dataframe(self, mock_df):
        """Test deduplication with valid DataFrame."""
        result = DedupeHandler.dedupe_dataframe(mock_df)

        mock_df.distinct.assert_called_once()
        assert result == mock_df

    def test_dedupe_dataframe_with_none_input(self):
        """Test deduplication with None input."""
        result = DedupeHandler.dedupe_dataframe(None)

        assert result is None
