#!/bin/bash

# Test runner script for copy_from_tdr_to_gcs_hca.py
# This script provides common test execution patterns

set -e

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}TDR to GCS HCA Copy Script Test Suite${NC}"
echo "=========================================="
echo "Project root: $PROJECT_ROOT"
echo "Test directory: $SCRIPT_DIR"

# Add src to PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}Error: pytest is not installed${NC}"
    echo "Please install test dependencies: pip install -r test-requirements.txt"
    exit 1
fi

# Function to run tests with specific options
run_tests() {
    local test_type="$1"
    local options="$2"
    
    echo -e "\n${YELLOW}Running $test_type tests...${NC}"
    if pytest $options; then
        echo -e "${GREEN}✓ $test_type tests passed${NC}"
    else
        echo -e "${RED}✗ $test_type tests failed${NC}"
        return 1
    fi
}

# Parse command line arguments
case "${1:-all}" in
    "unit")
        run_tests "unit" "-m unit -v"
        ;;
    "integration")
        run_tests "integration" "-m integration -v"
        ;;
    "coverage")
        run_tests "coverage" "--cov=copy_from_tdr_to_gcs_hca --cov-report=html --cov-report=term-missing"
        echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
        ;;
    "fast")
        run_tests "fast" "-x -q"
        ;;
    "parallel")
        if command -v pytest-xdist &> /dev/null; then
            run_tests "parallel" "-n auto"
        else
            echo -e "${YELLOW}Warning: pytest-xdist not installed, running sequentially${NC}"
            run_tests "all" "-v"
        fi
        ;;
    "all"|*)
        run_tests "all" "-v"
        ;;
esac

echo -e "\n${GREEN}Test execution completed!${NC}"
