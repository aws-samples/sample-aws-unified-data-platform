# UDM Unit Tests

This directory contains comprehensive unit tests for the UDM (Unified Data Management) module with 90%+ code coverage.

## Test Structure

The test structure mirrors the source code structure for easy navigation:

```
tests/unit-tests/python/udm/tests/
├── curation/                    # Tests for curation jobs
│   └── test_aws_samples_udm_curated.py
├── dynamodb_to_parquet/         # Tests for DynamoDB to Parquet ETL
│   └── test_dynamodb_to_parquet.py
├── json_to_parquet_etl/         # Tests for JSON to Parquet ETL
│   └── test_json_to_parquet_etl.py
├── stage/                       # Tests for stage jobs
│   └── test_aws_samples_udm_stage.py
├── synthesize_data/             # Tests for data synthesis
│   └── test_synthesize.py
├── utils/                       # Tests for utility modules
│   ├── audit/
│   │   └── test_audit_module.py
│   ├── config/
│   │   └── test_config_reader.py
│   ├── io/
│   │   ├── test_data_reader.py
│   │   ├── test_data_writer.py
│   │   └── test_incremental_reader.py
│   ├── transformations/
│   │   ├── test_array_transformations.py
│   │   ├── test_business_transformations.py
│   │   ├── test_core_transformations.py
│   │   ├── test_data_quality.py
│   │   ├── test_date_transformations.py
│   │   ├── test_extraction_utils.py
│   │   ├── test_metadata_extractor.py
│   │   ├── test_nested_data.py
│   │   ├── test_s3_transformations.py
│   │   └── test_schema_evolution.py
│   ├── test_column_selector.py
│   ├── test_config_validator.py
│   ├── test_job_utils.py
│   └── test_transformation_engine.py
├── conftest.py                  # Shared fixtures and configuration
└── README.md                    # This file
```

## Running Tests

### Prerequisites

1. Install required dependencies:
   ```bash
   pip install -r ../config/requirements.txt
   ```

2. Ensure PySpark is available in your environment

### Running All Tests

Use the existing test runner script:
```bash
cd /path/to/repo
./tests/unit-tests/python/udm/scripts/run_tests.sh
```

### Running Specific Test Files

```bash
# Run tests for a specific module
pytest tests/unit-tests/python/udm/tests/utils/test_job_utils.py -v

# Run tests for a specific class
pytest tests/unit-tests/python/udm/tests/utils/test_job_utils.py::TestJobUtils -v

# Run a specific test method
pytest tests/unit-tests/python/udm/tests/utils/test_job_utils.py::TestJobUtils::test_convert_namespace_to_dict_simple -v
```

### Running with Coverage

```bash
# Run with coverage report
pytest --cov=src/glue/udm --cov-report=html --cov-report=term-missing

# Generate HTML coverage report
coverage html
```

## Test Configuration

- **pytest.ini**: Located in `../config/pytest.ini` with 90% coverage requirement
- **.coveragerc**: Located in `../config/.coveragerc` for coverage configuration
- **requirements.txt**: Located in `../config/requirements.txt` for test dependencies

## Test Coverage Goals

The test suite aims for 90%+ code coverage across all UDM modules:

- **Main ETL Jobs**: Stage, Curation, DynamoDB-to-Parquet, JSON-to-Parquet
- **Utility Modules**: Data I/O, Transformations, Configuration, Auditing
- **Transformation Engine**: Rule-based transformations and schema evolution
- **Error Handling**: Exception scenarios and edge cases

## Key Testing Patterns

### 1. Mocking External Dependencies

All tests mock external dependencies like AWS services, Spark contexts, and Glue contexts:

```python
@patch('module.boto3.client')
def test_function(self, mock_boto3_client):
    mock_s3 = Mock()
    mock_boto3_client.return_value = mock_s3
    # Test implementation
```

### 2. Spark DataFrame Testing

Tests use real Spark DataFrames for transformation testing:

```python
def test_transformation(self, spark):
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("test",)], schema)
    result = transformation_function(df)
    assert result.count() == 1
```

### 3. Configuration Testing

Tests cover various configuration scenarios:

```python
def test_config_loading(self, sample_config):
    config = load_configs("s3://bucket/config.yaml")
    assert config.source_type == "dynamodb"
```

### 4. Error Scenario Testing

Tests include comprehensive error handling:

```python
def test_error_handling(self):
    with pytest.raises(ValueError, match="Expected error message"):
        function_that_should_fail()
```

## Fixtures

### Global Fixtures (conftest.py)

- `spark`: Spark session for testing
- `mock_glue_context`: Mocked AWS Glue context
- `sample_dataframe`: Sample DataFrame for testing
- `mock_audit_manager`: Mocked audit manager
- `sample_config`: Sample configuration object

### Test-Specific Fixtures

Each test file includes fixtures specific to the module being tested.

## Best Practices

1. **Test Isolation**: Each test is independent and doesn't rely on other tests
2. **Comprehensive Coverage**: Tests cover happy paths, edge cases, and error scenarios
3. **Clear Naming**: Test names clearly describe what is being tested
4. **Minimal Mocking**: Only mock external dependencies, not the code under test
5. **Realistic Data**: Use realistic test data that represents actual use cases

## Continuous Integration

The test suite is designed to run in CI/CD pipelines with:
- Automated test execution
- Coverage reporting
- Failure notifications
- Performance monitoring

## Contributing

When adding new UDM functionality:

1. Create corresponding test files following the directory structure
2. Ensure 90%+ coverage for new code
3. Include tests for error scenarios
4. Update this README if adding new test patterns
5. Run the full test suite before submitting changes

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure PYTHONPATH includes source directories
2. **Spark Errors**: Verify PySpark installation and Java compatibility
3. **Mock Errors**: Check that all external dependencies are properly mocked
4. **Coverage Issues**: Use `--cov-report=html` to identify uncovered lines

### Debug Mode

Run tests with verbose output and no coverage for debugging:
```bash
pytest -v -s --tb=long tests/unit-tests/python/udm/tests/
```