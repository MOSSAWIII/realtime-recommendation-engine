#!/bin/bash
set -e

# Parse command line arguments
TEST_TYPE=${1:-"transformer"}

echo "=========================================================="
echo "Running $TEST_TYPE tests with required Docker containers"
echo "=========================================================="

# Find Colima socket
COLIMA_SOCKET="$HOME/.colima/default/docker.sock"
if [ ! -S "$COLIMA_SOCKET" ]; then
    echo "Error: Colima socket not found at $COLIMA_SOCKET"
    echo "Please make sure Colima is running with: colima start"
    exit 1
fi

# Stop any existing containers
echo "Stopping any running test containers..."
docker-compose -f docker-compose-test.yml down --remove-orphans

# Start required containers
echo "Starting test containers..."
docker-compose -f docker-compose-test.yml up -d test-db test-rabbitmq

# Wait for containers to initialize (adjust as needed)
echo "Waiting for containers to initialize (45 seconds)..."
sleep 45

# Set up test queues in RabbitMQ
echo "Creating test queues in RabbitMQ..."
# Ensure rabbitmq management plugin is enabled
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmq-plugins enable rabbitmq_management || true
sleep 5
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl add_vhost test || true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl set_permissions -p test guest ".*" ".*" ".*" || true
# Create queues
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_events durable=true || true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events durable=true || true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=internal_recommendation_requests durable=true || true

# Determine test command based on test type
case $TEST_TYPE in
  "transformer")
    echo "Running transformer integration test..."
    docker-compose -f docker-compose-test.yml run \
      --rm \
      --env DOCKER_HOST=unix:///var/run/docker.sock \
      --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
      --env TESTCONTAINERS_RYUK_DISABLED=true \
      -v "$COLIMA_SOCKET:/var/run/docker.sock" \
      test-runner \
      sbt "testOnly integration.streaming.StreamingTransformerIntegrationSpec"
    ;;
  "streaming")
    echo "Running all streaming integration tests..."
    docker-compose -f docker-compose-test.yml run \
      --rm \
      --env DOCKER_HOST=unix:///var/run/docker.sock \
      --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
      --env TESTCONTAINERS_RYUK_DISABLED=true \
      -v "$COLIMA_SOCKET:/var/run/docker.sock" \
      test-runner \
      sbt "testOnly integration.streaming.*"
    ;;
  "all")
    echo "Running all integration tests..."
    docker-compose -f docker-compose-test.yml run \
      --rm \
      --env DOCKER_HOST=unix:///var/run/docker.sock \
      --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
      --env TESTCONTAINERS_RYUK_DISABLED=true \
      -v "$COLIMA_SOCKET:/var/run/docker.sock" \
      test-runner \
      sbt "testOnly integration.*"
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