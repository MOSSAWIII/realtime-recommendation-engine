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
  echo "Run tests with Docker container dependencies based on the new test organization."
  echo ""
  echo "Available TEST_TYPE options:"
  echo "  unit             - Run all unit tests"
  echo "  unit:domain      - Run domain model unit tests"
  echo "  unit:application - Run application core unit tests"
  echo "  unit:infra       - Run infrastructure unit tests"
  echo ""
  echo "  integration      - Run all integration tests"
  echo "  integration:db   - Run database integration tests"
  echo "  integration:msg  - Run messaging integration tests"
  echo "  integration:api  - Run API integration tests"
  echo "  integration:txn  - Run transformation integration tests"
  echo ""
  echo "  functional       - Run all functional end-to-end tests"
  echo ""
  echo "  specific:[class] - Run a specific test class"
  echo "                     Example: ./run-tests.sh specific:unit.application.core.TransformerSpec"
  echo ""
  echo "  all              - Run all tests"
  echo ""
  echo "Add 'debug' as second parameter to enable verbose debugging"
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

  echo "Waiting for containers to initialize (20 seconds)..."
  sleep 20
}

function setup_rabbitmq_queues() {
  echo "Setting up RabbitMQ for testing..."
  # Ensure rabbitmq management plugin is enabled
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmq-plugins enable rabbitmq_management || true
  sleep 5
  
  # Setup vhost if needed
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl add_vhost test || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqctl set_permissions -p test guest ".*" ".*" ".*" || true
  
  # Create the required queues from the simplified diagram
  echo "Creating test queues in RabbitMQ..."
  
  # Create the main exchange according to the technical diagram
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare exchange name=user_actions_exchange type=topic durable=true || true
  
  # Create the queues according to the technical diagram
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events_queue durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=recommendations_queue durable=true || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=historical_data_queue durable=true || true
  
  # Create bindings with routing keys according to the architecture
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=processed_events_queue routing_key="event.processed.#" || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=recommendations_queue routing_key="recommendation.result.#" || true
  docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=historical_data_queue routing_key="historical.data.#" || true
  
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
TEST_TYPE=${1:-"unit"}
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

# Only start containers for integration and functional tests
if [[ "$TEST_TYPE" == "integration"* || "$TEST_TYPE" == "functional"* || "$TEST_TYPE" == "all" ]]; then
  setup_containers
  setup_rabbitmq_queues
fi

# Run tests based on selected type
case $TEST_TYPE in
  "unit")
    run_test "unit.**" "$DEBUG_ARGS"
    ;;
  "unit:domain")
    run_test "unit.domain.**" "$DEBUG_ARGS"
    ;;
  "unit:application")
    run_test "unit.application.**" "$DEBUG_ARGS"
    ;;
  "unit:infra")
    run_test "unit.infrastructure.**" "$DEBUG_ARGS"
    ;;
  "integration")
    run_test "integration.**" "$DEBUG_ARGS"
    ;;
  "integration:db")
    run_test "integration.db.**" "$DEBUG_ARGS"
    ;;
  "integration:msg")
    run_test "integration.messaging.**" "$DEBUG_ARGS"
    ;;
  "integration:api")
    run_test "integration.api.**" "$DEBUG_ARGS"
    ;;
  "integration:txn")
    run_test "integration.transformation.**" "$DEBUG_ARGS"
    ;;
  "functional")
    run_test "functional.**" "$DEBUG_ARGS"
    ;;
  "all")
    # Run all tests - unit first, then integration, then functional
    run_test "unit.**" "$DEBUG_ARGS"
    run_test "integration.**" "$DEBUG_ARGS"
    run_test "functional.**" "$DEBUG_ARGS"
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