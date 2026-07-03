# UDM Unit Tests

Unit testing framework for the UDM (Unified Data Model) Glue modules.

## Directory Structure

```
tests/unit-tests/python/udm/
├── tests/              # Test files
│   ├── test_core_transformations.py
│   ├── test_data_reader.py
│   ├── test_mock_transformations.py
│   ├── test_simple_demo.py
│   └── conftest.py
├── config/             # Configuration files
│   ├── pytest.ini
│   └── requirements.txt
├── scripts/            # Execution scripts
│   ├── run_tests.sh
│   └── Dockerfile
├── docs/               # Documentation
│   ├── README.md
│   └── DEMO_SUMMARY.md
├── htmlcov/           # Coverage HTML reports
├── cov_report.xml     # Coverage XML report
├── venv/              # Virtual environment
└── README.md          # This file
```

## Quick Start

```bash
# From repository root
cd tests/unit-tests/python/udm/scripts
bash run_tests.sh
```

## Coverage Reports

- **HTML**: `htmlcov/index.html`
- **XML**: `cov_report.xml`

## Test Types

- **Mock-based tests**: Work without Spark/Glue dependencies
- **Import validation**: Verify module imports
- **Structure tests**: Validate module organization
- **Coverage tracking**: Monitor code coverage (5% threshold)