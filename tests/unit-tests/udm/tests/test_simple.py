# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Simple test to verify test framework is working."""

import pytest


class TestSimple:
    """Simple test class to verify pytest is working."""

    def test_basic_functionality(self):
        """Test basic Python functionality."""
        assert 1 + 1 == 2
        assert "hello" == "hello"
        assert [1, 2, 3] == [1, 2, 3]

    def test_string_operations(self):
        """Test string operations."""
        text = "Hello World"
        assert text.lower() == "hello world"
        assert text.upper() == "HELLO WORLD"
        assert len(text) == 11

    def test_list_operations(self):
        """Test list operations."""
        numbers = [1, 2, 3, 4, 5]
        assert len(numbers) == 5
        assert sum(numbers) == 15
        assert max(numbers) == 5
        assert min(numbers) == 1
