# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from setuptools import find_packages, setup

setup(
    name="udm",
    version="0.1.0",
    author="Amazon Web Services",
    author_email="",
    packages=find_packages(),
    install_requires=[
        # Core dependencies
        "boto3>=1.28.0",
        "botocore>=1.31.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "pydantic>=2.0.0",
        "typing-extensions>=4.7.0",
        # AWS specific
        "awswrangler>=3.4.0",
        "pyspark>=3.3.0",
        # Data processing
        "python-dateutil>=2.8.2",
        "pytz>=2023.3",
        "regex>=2023.6.3",
        "pyyaml>=6.0.0",
        # JSON processing
        "json5>=0.9.14",
        # Enum support
        "enum-compat>=0.0.3",
        # Path handling
        "pathlib>=1.0.1",
        # Security
        "cryptography>=43.0.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "mypy>=1.3.0",
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ],
    },
    python_requires=">=3.12",
)
