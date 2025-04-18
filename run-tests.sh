#!/bin/bash

# Script to run tests in Docker environment

# Help function
function show_help {
  echo "Usage: ./run-tests.sh [options] [test-name]"
  echo ""
  echo "Options:"
  echo "  --clean             Clean environment before running tests"
  echo "  --single TEST_NAME  Run a single specific test"
  echo "  --help              Show this help message"
  echo ""
  echo "Examples:"
  echo "  ./run-tests.sh                         # Run all tests"
  echo "  ./run-tests.sh --clean                 # Clean and run all tests"
  echo "  ./run-tests.sh --single unit.core.StreamingConsumerSpec  # Run specific test"
  exit 0
}

CLEAN=false
SINGLE_TEST=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
      CLEAN=true
      shift
      ;;
    --single)
      SINGLE_TEST="$2"
      shift 2
      ;;
    --help)
      show_help
      ;;
    *)
      if [ -z "$SINGLE_TEST" ]; then
        SINGLE_TEST="$1"
      fi
      shift
      ;;
  esac
done

# Clean if requested
if [ "$CLEAN" = true ]; then
  echo "Cleaning Docker environment..."
  docker-compose -f docker-compose-test.yml down -v
fi

# Run tests
if [ -n "$SINGLE_TEST" ]; then
  echo "Running test: $SINGLE_TEST"
  docker-compose -f docker-compose-test.yml run --rm test-runner sbt "testOnly $SINGLE_TEST"
else
  echo "Running all tests..."
  docker-compose -f docker-compose-test.yml run --rm test-runner sbt test
fi