#!/bin/bash
set -e

# Parse command line arguments
TEST_TYPE=${1:-"transformer"}

echo "=========================================================="
echo "Running $TEST_TYPE tests with required Docker containers"
echo "=========================================================="

# Stop any existing containers
echo "Stopping any running test containers..."
docker-compose -f docker-compose-test.yml down --remove-orphans

# Start required containers
echo "Starting test containers..."
docker-compose -f docker-compose-test.yml up -d test-db test-rabbitmq

# Wait for containers to initialize (adjust as needed)
echo "Waiting for containers to initialize (30 seconds)..."
sleep 30

# Determine test command based on test type
case $TEST_TYPE in
  "transformer")
    echo "Running transformer integration test..."
    docker-compose -f docker-compose-test.yml run --rm -w /app test-runner sbt "testOnly integration.streaming.StreamingTransformerIntegrationSpec"
    ;;
  "streaming")
    echo "Running all streaming integration tests..."
    docker-compose -f docker-compose-test.yml run --rm -w /app test-runner sbt "testOnly integration.streaming.*"
    ;;
  "all")
    echo "Running all integration tests..."
    docker-compose -f docker-compose-test.yml run --rm -w /app test-runner sbt "testOnly integration.*"
    ;;
  *)
    echo "Unknown test type: $TEST_TYPE"
    echo "Available options: transformer, streaming, all"
    exit 1
    ;;
esac

exit_code=$?

# Clean up containers regardless of test outcome
echo "Cleaning up test containers..."
docker-compose -f docker-compose-test.yml down --remove-orphans

# Return the original exit code from the test
exit $exit_code