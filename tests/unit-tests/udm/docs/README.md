# UDM Unit Tests

This directory contains unit tests for the UDM (Unified Data Model) Glue modules.

## Overview

The tests are designed to run in the AWS Glue Docker container (`public.ecr.aws/glue/aws-glue-libs:5`) to ensure compatibility with the actual Glue runtime environment.

## Test Structure

- `test_core_transformations.py` - Tests for CoreTransformations class
- `test_data_reader.py` - Tests for DataReader class
- `pytest.ini` - Pytest configuration with coverage settings
- `requirements.txt` - Test dependencies
- `Dockerfile` - Docker configuration for test environment
- `run_tests.sh` - Script to build and run tests

## Running Tests

### Prerequisites

**Option 1: Local Testing (Recommended)**
- Python 3.9+
- Virtual environment support

**Option 2: Docker Testing**
- Docker installed and running
- AWS Glue Docker image: `docker pull public.ecr.aws/glue/aws-glue-libs:5`
- Java Runtime (required for PySpark)

### Execute Tests

From the repository root directory:

**Local Testing:**
```bash
# Run tests locally (no Java/Spark required)
bash tests/unit-tests/python/udm/run_tests_local.sh
```

**Docker Testing:**
```bash
# Run tests in AWS Glue container (requires Java)
bash tests/unit-tests/python/udm/run_tests.sh
```

### Coverage Reports

After running tests, coverage reports are generated in:
- HTML: `tests/unit-tests/python/udm/htmlcov/index.html`
- XML: `tests/unit-tests/python/udm/coverage.xml`

## Test Coverage

Current test coverage includes:
- String cleaning and data type conversions
- Boolean conversions with custom mappings
- Column renaming and corrections mapping
- HTML entity cleaning
- S3 prefix filtering by datetime
- DynamoDB and catalog table reading

## Adding New Tests

1. Create new test files following the `test_*.py` naming convention
2. Use pytest fixtures for common setup
3. Mock external dependencies (AWS services, Spark contexts)
4. Ensure tests are isolated and can run independently

## Configuration

- Minimum coverage threshold: 80%
- Test discovery pattern: `test_*.py`
- Coverage includes all modules under `src/glue/udm/`