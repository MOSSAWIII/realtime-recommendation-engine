#!/bin/bash
set -e

# Colors for output formatting
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Helper functions
function print_header() {
  echo -e "${YELLOW}=========================================================="
  echo -e "$1"
  echo -e "==========================================================${NC}"
}

function print_success() {
  echo -e "${GREEN}$1${NC}"
}

function print_error() {
  echo -e "${RED}$1${NC}"
}

function show_help() {
  echo "Usage: ./run-tests.sh [TEST_TYPE] [DEBUG]"
  echo ""
  echo "Run integration tests with Docker container dependencies."
  echo ""
  echo "Available TEST_TYPE options:"
  echo "  transformation     - Run StreamingTransformerIntegrationSpec"
  echo "  streaming         - Run StreamingTransformerIntegrationSpec"
  echo "  pipeline          - Run PipelineIntegrationSpec and CompletePipelineSpec"
  echo "  messaging         - Run TransformerRecommenderMessagingSpec"
  echo "  recommendation    - Run TransformerRecommenderIntegrationSpec"
  echo "  contextual        - Run ContextualRecommendationSpec (Phase 2)"
  echo "  specific:[class]  - Run a specific integration test class"
  echo "                      Example: ./run-tests.sh specific:integration.recommendation.ContextualRecommendationSpec"
  echo "  all               - Run all integration tests"
  echo ""
  echo "Add 'debug' as second parameter to enable verbose debugging"
  echo ""
  echo "Default if no option provided: transformation"
  exit 0
}

function check_prerequisites() {
  # Check if Colima is running
  COLIMA_SOCKET="$HOME/.colima/default/docker.sock"
  if [ ! -S "$COLIMA_SOCKET" ]; then
    print_error "Error: Colima socket not found at $COLIMA_SOCKET"
    print_error "Please make sure Colima is running with: colima start"
    exit 1
  fi
}

function cleanup_containers() {
  echo "Stopping any running test containers..."
  docker-compose -f docker-compose-test.yml down --remove-orphans
}

function setup_containers() {
  echo "Starting test containers..."
  docker-compose -f docker-compose-test.yml up -d test-db test-rabbitmq

  echo "Waiting for containers to initialize (45 seconds)..."
  sleep 45
}

function setup_rabbitmq_queues() {
  echo "Setting up RabbitMQ for testing..."
  # Ensure rabbitmq management plugin is enabled
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmq-plugins enable rabbitmq_management || true
  sleep 5
  
  # Setup vhost if needed
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl add_vhost test || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl set_permissions -p test guest ".*" ".*" ".*" || true
  
  # Create required queues
  echo "Creating test queues in RabbitMQ..."
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_events durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=internal_recommendation_requests durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=recommendations durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_interactions durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=event_updates durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=participation_updates durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=friendship_updates durable=true || true
  
  print_success "RabbitMQ setup completed"
}

function run_test() {
  local test_class=$1
  local debug_args=$2
  
  echo "Running test: $test_class"
  docker-compose -f docker-compose-test.yml run \
    --rm \
    --env DOCKER_HOST=unix:///var/run/docker.sock \
    --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
    --env TESTCONTAINERS_RYUK_DISABLED=true \
    --env TEST_MODE=true \
    --env TEST_MOCKING=false \
    -v "$COLIMA_SOCKET:/var/run/docker.sock" \
    test-runner \
    sbt "-Dtest.mode=true -Dtest.enableMocking=false -Dconfig.resource=application-test.conf $debug_args" "testOnly $test_class"
    
  return $?
}

# Main script execution starts here

# Display help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help
fi

# Parse command line arguments
TEST_TYPE=${1:-"transformation"}
DEBUG_MODE=${2:-""}

# Process debug flag
DEBUG_ARGS=""
if [ "$DEBUG_MODE" == "debug" ]; then
  DEBUG_ARGS="-Dtest.debug=true -Dloglevel.console=DEBUG"
  print_header "Running $TEST_TYPE tests with DEBUG MODE ENABLED"
else
  print_header "Running $TEST_TYPE tests"
fi

# Setup test environment
check_prerequisites
cleanup_containers
setup_containers
setup_rabbitmq_queues

# Run tests based on selected type
case $TEST_TYPE in
  "transformation")
    run_test "integration.transformation.StreamingTransformerIntegrationSpec" "$DEBUG_ARGS"
    ;;
  "streaming")
    run_test "integration.streaming.StreamingTransformerIntegrationSpec" "$DEBUG_ARGS"
    ;;
  "pipeline")
    run_test "integration.pipeline.PipelineIntegrationSpec" "$DEBUG_ARGS"
    run_test "e2e.pipeline.CompletePipelineSpec" "$DEBUG_ARGS"
    ;;
  "messaging")
    run_test "integration.messaging.TransformerRecommenderMessagingSpec" "$DEBUG_ARGS"
    ;;
  "recommendation")
    run_test "integration.recommendation.TransformerRecommenderIntegrationSpec" "$DEBUG_ARGS"
    ;;
  "contextual")
    run_test "integration.recommendation.ContextualRecommendationSpec" "$DEBUG_ARGS"
    ;;
  "all")
    # List all integration test classes explicitly
    run_test "integration.transformation.StreamingTransformerIntegrationSpec" "$DEBUG_ARGS"
    run_test "integration.streaming.StreamingTransformerIntegrationSpec" "$DEBUG_ARGS"
    run_test "integration.pipeline.PipelineIntegrationSpec" "$DEBUG_ARGS"
    run_test "e2e.pipeline.CompletePipelineSpec" "$DEBUG_ARGS"
    run_test "integration.messaging.TransformerRecommenderMessagingSpec" "$DEBUG_ARGS"
    run_test "integration.recommendation.TransformerRecommenderIntegrationSpec" "$DEBUG_ARGS"
    run_test "integration.recommendation.ContextualRecommendationSpec" "$DEBUG_ARGS"
    ;;
  specific:*)
    # Extract the specific test class name after 'specific:'
    TEST_CLASS="${TEST_TYPE#specific:}"
    run_test "$TEST_CLASS" "$DEBUG_ARGS"
    ;;
  *)
    print_error "Unknown test type: $TEST_TYPE"
    print_error "Run './run-tests.sh --help' for available options"
    cleanup_containers
    exit 1
    ;;
esac

exit_code=$?

# Clean up containers regardless of test outcome
cleanup_containers

if [ $exit_code -eq 0 ]; then
  print_success "Tests completed successfully!"
else
  print_error "Tests failed with exit code $exit_code"
fi

# Return the original exit code from the test
exit $exit_code