#!/bin/bash

# UDM unit test runner using AWS Glue Docker container

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Running UDM unit tests in AWS Glue 5.0 Docker container...${NC}"

# Navigate to project root to ensure proper context for Docker build
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../../" && pwd)"
cd "$PROJECT_ROOT"

# Run tests in Glue container using the official pattern
docker run -i --rm \
    --platform=linux/amd64 \
    -v "$(pwd):/home/hadoop/workspace/" \
    --workdir /home/hadoop/workspace \
    -e PYTHONPATH="/home/hadoop/workspace/src/glue/udm:/home/hadoop/workspace/src/global_utils" \
    --name udm-glue-tests \
    public.ecr.aws/glue/aws-glue-libs:5 \
    -c "pip3 install --user -r tests/unit-tests/python/udm/config/requirements.txt && \
        cd /home/hadoop/workspace && \
        rm -rf tests/unit-tests/python/udm/htmlcov tests/unit-tests/python/udm/udm_cov_report.xml tests/unit-tests/python/udm/.coverage && \
        python3 -m pytest tests/unit-tests/python/udm/tests/ -v --tb=short \
        --cov=src/glue/udm \
        --cov-config=tests/unit-tests/python/udm/config/.coveragerc \
        --cov-report=html:tests/unit-tests/python/udm/htmlcov \
        --cov-report=xml:tests/unit-tests/python/udm/udm_cov_report.xml \
        --cov-report=term-missing"

echo -e "${GREEN}✅ UDM Unit Tests Completed in Glue 5.0 Docker Container!${NC}"
echo -e "${GREEN}Coverage reports generated:${NC}"
echo -e "  📊 HTML: tests/unit-tests/python/udm/htmlcov/index.html"
echo -e "  📄 XML:  tests/unit-tests/python/udm/udm_cov_report.xml"

echo -e "${GREEN}🎉 Container-based UDM test suite completed!${NC}"