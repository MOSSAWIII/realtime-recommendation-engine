#!/bin/bash
# filepath: /Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/run-tests.sh
set -e

# Colors for output formatting
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
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

function print_info() {
  echo -e "${BLUE}$1${NC}"
}

function show_help() {
  echo "Usage: ./run-tests.sh [TEST_TYPE] [DEBUG]"
  echo ""
  echo "Run tests with Docker container dependencies based on the test organization."
  echo ""
  echo "Available TEST_TYPE options:"
  echo ""
  echo "  === Test Categories ==="
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
  echo "  === Component-Specific Tests ==="
  echo "  component:transformer    - Run all tests for the Transformer component"
  echo "  component:recommender    - Run all tests for the Recommender component"
  echo "  component:datamanager    - Run all tests for the DataManager component"
  echo "  component:pipeline       - Run all tests for the Pipeline component"
  echo ""
  echo "  === Tagged Tests ==="
  echo "  tag:UnitTest     - Run tests tagged with UnitTest"
  echo "  tag:Fast         - Run tests tagged with Fast"
  echo "  tag:Isolated     - Run tests tagged with Isolated"
  echo "  tag:DatabaseTest - Run tests tagged with DatabaseTest"
  echo "  tag:RabbitMQTest - Run tests tagged with RabbitMQTest"
  echo ""
  echo "  === Specific Tests ==="
  echo "  specific:[class] - Run a specific test class"
  echo "                     Example: ./run-tests.sh specific:unit.application.core.TransformerSpec"
  echo ""
  echo "  === All Tests ==="
  echo "  all              - Run all tests"
  echo ""
  echo "  === Options ==="
  echo "  --use-env        - Use existing .test_env instead of creating a new environment"
  echo "  --debug          - Enable verbose debugging output"
  echo "  --help, -h       - Show this help message"
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

function load_existing_env() {
  if [ -f .test_env ]; then
    print_info "Loading existing test environment from .test_env"
    source .test_env
    
    # Verify that all required variables are set
    if [[ -z "$TEST_RUN_ID" || -z "$TEST_EXCHANGE" || -z "$TEST_PROCESSED_EVENTS_QUEUE" ]]; then
      print_error "Error: Required variables missing from .test_env"
      exit 1
    fi
    
    print_info "Using TEST_RUN_ID=${TEST_RUN_ID}"
    
    # Check if containers exist and are running
    if docker ps | grep -q "$TEST_DB_NAME" && docker ps | grep -q "$TEST_RABBITMQ_NAME"; then
      print_success "Found existing test containers"
    else
      print_error "Required test containers not running. Start them or remove .test_env"
      exit 1
    fi
    
    return 0
  else
    print_error "No .test_env file found"
    return 1
  fi
}

function cleanup_containers() {
  echo "Stopping any running test containers..."
  docker-compose -f docker-compose-test.yml down --remove-orphans
}

function generate_test_run_id() {
  # Generate a unique test run ID
  TEST_RUN_ID=$(date +%s)_$(openssl rand -hex 4)
  export TEST_RUN_ID
  print_info "Generated new TEST_RUN_ID=${TEST_RUN_ID}"
}

function setup_test_environment() {
  generate_test_run_id
  
  echo "Starting test containers with unique ID: ${TEST_RUN_ID}..."
  
  # Create container name suffixes with the unique test run ID
  export TEST_DB_NAME="test-db-${TEST_RUN_ID}"
  export TEST_RABBITMQ_NAME="test-rabbitmq-${TEST_RUN_ID}"
  
  # Start containers with unique names to avoid conflicts
  docker-compose -f docker-compose-test.yml up -d \
    --scale test-db=1 --scale test-rabbitmq=1 \
    --name ${TEST_DB_NAME} \
    --name ${TEST_RABBITMQ_NAME}

  # Wait for initialization
  echo "Waiting for containers to initialize..."
  sleep 10
  
  # Check if containers are healthy
  db_health=$(docker inspect --format='{{.State.Health.Status}}' ${TEST_DB_NAME} 2>/dev/null || echo "not_running")
  rabbitmq_health=$(docker inspect --format='{{.State.Health.Status}}' ${TEST_RABBITMQ_NAME} 2>/dev/null || echo "not_running")
  
  if [[ "$db_health" != "healthy" || "$rabbitmq_health" != "healthy" ]]; then
    echo "Containers still initializing, waiting an additional 10 seconds..."
    sleep 10
    
    # Check again
    db_health=$(docker inspect --format='{{.State.Health.Status}}' ${TEST_DB_NAME} 2>/dev/null || echo "not_running")
    rabbitmq_health=$(docker inspect --format='{{.State.Health.Status}}' ${TEST_RABBITMQ_NAME} 2>/dev/null || echo "not_running")
    
    if [[ "$db_health" != "healthy" || "$rabbitmq_health" != "healthy" ]]; then
      print_error "Containers failed to reach healthy state. DB: $db_health, RabbitMQ: $rabbitmq_health"
      cleanup_containers
      exit 1
    fi
  fi
  
  # Setup RabbitMQ resources with unique identifiers
  setup_rabbitmq_queues
  
  # Create a file to store the unique test run ID for cleanup
  echo "${TEST_RUN_ID}" > .test_run_id
  
  # Export all environment variables needed for tests
  cat > .test_env <<EOF
export TEST_RUN_ID=${TEST_RUN_ID}
export TEST_DB_NAME=${TEST_DB_NAME}
export TEST_RABBITMQ_NAME=${TEST_RABBITMQ_NAME}
export TEST_EXCHANGE=user_actions_exchange_${TEST_RUN_ID}
export TEST_PROCESSED_EVENTS_QUEUE=processed_events_queue_${TEST_RUN_ID}
export TEST_RECOMMENDATIONS_QUEUE=recommendations_queue_${TEST_RUN_ID}
export TEST_HISTORICAL_DATA_QUEUE=historical_data_queue_${TEST_RUN_ID}
EOF
  
  # Source the environment variables
  source .test_env
  
  echo "Test environment ready with ID: ${TEST_RUN_ID}"
}

function setup_rabbitmq_queues() {
  echo "Setting up RabbitMQ with unique ID: ${TEST_RUN_ID}..."
  
  # Use unique names for exchange and queues
  export TEST_EXCHANGE="user_actions_exchange_${TEST_RUN_ID}"
  export TEST_PROCESSED_EVENTS_QUEUE="processed_events_queue_${TEST_RUN_ID}"
  export TEST_RECOMMENDATIONS_QUEUE="recommendations_queue_${TEST_RUN_ID}"
  export TEST_HISTORICAL_DATA_QUEUE="historical_data_queue_${TEST_RUN_ID}"
  
  # Create exchange
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare exchange \
    name=${TEST_EXCHANGE} type=topic durable=true || true
  
  # Create queues
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare queue \
    name=${TEST_PROCESSED_EVENTS_QUEUE} durable=true || true
  
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare queue \
    name=${TEST_RECOMMENDATIONS_QUEUE} durable=true || true
  
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare queue \
    name=${TEST_HISTORICAL_DATA_QUEUE} durable=true || true
  
  # Create bindings
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare binding \
    source=${TEST_EXCHANGE} destination=${TEST_PROCESSED_EVENTS_QUEUE} routing_key="event.processed.#" || true
  
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare binding \
    source=${TEST_EXCHANGE} destination=${TEST_RECOMMENDATIONS_QUEUE} routing_key="recommendation.result.#" || true
  
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin declare binding \
    source=${TEST_EXCHANGE} destination=${TEST_HISTORICAL_DATA_QUEUE} routing_key="historical.data.#" || true
}

function cleanup_test_environment() {
  if [ -f .test_run_id ]; then
    local test_run_id=$(cat .test_run_id)
    echo "Cleaning up test environment with ID: ${test_run_id}..."
    
    # Source the environment variables if they exist
    if [ -f .test_env ]; then
      source .test_env
    fi
    
    # Clean up RabbitMQ resources
    local exchange="user_actions_exchange_${test_run_id}"
    local processed_queue="processed_events_queue_${test_run_id}"
    local recommendations_queue="recommendations_queue_${test_run_id}"
    local historical_queue="historical_data_queue_${test_run_id}"
    
    docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin delete queue name=${processed_queue} || true
    docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin delete queue name=${recommendations_queue} || true
    docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin delete queue name=${historical_queue} || true
    docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin delete exchange name=${exchange} || true
    
    # Stop containers
    docker-compose -f docker-compose-test.yml down
    
    # Remove temporary files
    rm -f .test_run_id .test_env
  else
    # Standard cleanup
    docker-compose -f docker-compose-test.yml down --remove-orphans
  fi
}

function run_test() {
  local test_class=$1
  local debug_args=$2
  
  echo "Running test: $test_class with TEST_RUN_ID=${TEST_RUN_ID}"
  
  # Pass all the test environment variables to the container
  docker-compose -f docker-compose-test.yml run \
    --rm \
    --env TEST_RUN_ID=${TEST_RUN_ID} \
    --env TEST_EXCHANGE=${TEST_EXCHANGE} \
    --env TEST_PROCESSED_EVENTS_QUEUE=${TEST_PROCESSED_EVENTS_QUEUE} \
    --env TEST_RECOMMENDATIONS_QUEUE=${TEST_RECOMMENDATIONS_QUEUE} \
    --env TEST_HISTORICAL_DATA_QUEUE=${TEST_HISTORICAL_DATA_QUEUE} \
    --env DOCKER_HOST=unix:///var/run/docker.sock \
    --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
    --env TESTCONTAINERS_RYUK_DISABLED=true \
    --env TEST_MODE=true \
    --env TEST_MOCKING=false \
    -v "$COLIMA_SOCKET:/var/run/docker.sock" \
    test-runner \
    sbt "-Dtest.runId=${TEST_RUN_ID} -Dtest.mode=true -Dtest.enableMocking=false -Dconfig.resource=application-test.conf $debug_args" "testOnly $test_class"
    
  return $?
}

# Main script execution starts here

# Process arguments
USE_EXISTING_ENV=false
DEBUG_MODE=false
TEST_TYPE="unit"

for arg in "$@"; do
  case $arg in
    --use-env)
      USE_EXISTING_ENV=true
      shift
      ;;
    --debug)
      DEBUG_MODE=true
      shift
      ;;
    -h|--help)
      show_help
      ;;
    *)
      TEST_TYPE=$arg
      shift
      ;;
  esac
done

# Process debug flag
DEBUG_ARGS=""
if [ "$DEBUG_MODE" = true ]; then
  DEBUG_ARGS="-Dtest.debug=true -Dloglevel.console=DEBUG"
  print_header "Running $TEST_TYPE tests with DEBUG MODE ENABLED"
else
  print_header "Running $TEST_TYPE tests"
fi

# Setup test environment
check_prerequisites

# Use existing environment or create new one
if [ "$USE_EXISTING_ENV" = true ]; then
  if ! load_existing_env; then
    print_error "Failed to load existing environment, aborting"
    exit 1
  fi
else
  # Clean up any existing containers unless using existing env
  cleanup_containers
  
  # Only start containers for integration and functional tests
  if [[ "$TEST_TYPE" == "integration"* || "$TEST_TYPE" == "functional"* || "$TEST_TYPE" == "all" || "$TEST_TYPE" == "tag:RabbitMQTest" || "$TEST_TYPE" == "tag:DatabaseTest" ]]; then
    setup_test_environment
  else
    # For unit tests, just generate an ID
    generate_test_run_id
  fi
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
  "component:transformer")
    run_test "**/application/core/Transformer*Spec" "$DEBUG_ARGS"
    ;;
  "component:recommender")
    run_test "**/application/core/Recommender*Spec" "$DEBUG_ARGS"
    ;;
  "component:datamanager")
    run_test "**/application/core/DataManager*Spec" "$DEBUG_ARGS"
    ;;
  "component:pipeline")
    run_test "**/application/core/Pipeline*Spec" "$DEBUG_ARGS"
    ;;
  "tag:UnitTest")
    run_test "*" "-n UnitTest $DEBUG_ARGS"
    ;;
  "tag:Fast")
    run_test "*" "-n Fast $DEBUG_ARGS"
    ;;
  "tag:Isolated")
    run_test "*" "-n Isolated $DEBUG_ARGS"
    ;;
  "tag:DatabaseTest")
    run_test "*" "-n DatabaseTest $DEBUG_ARGS"
    ;;
  "tag:RabbitMQTest")
    run_test "*" "-n RabbitMQTest $DEBUG_ARGS"
    ;;
  "mocks")
    run_test "common.mocks.**" "$DEBUG_ARGS"
    ;;
  "conditional")
    run_test "**/Conditional*Test" "$DEBUG_ARGS"
    ;;
  "all")
    # Run all tests - unit first, then integration, then functional
    run_test "unit.**" "$DEBUG_ARGS"
    unit_exit_code=$?
    
    run_test "integration.**" "$DEBUG_ARGS"
    integration_exit_code=$?
    
    run_test "functional.**" "$DEBUG_ARGS"
    functional_exit_code=$?
    
    # If any test suite failed, return non-zero
    if [ $unit_exit_code -ne 0 ] || [ $integration_exit_code -ne 0 ] || [ $functional_exit_code -ne 0 ]; then
      exit_code=1
    else
      exit_code=0
    fi
    ;;
  specific:*)
    # Extract the specific test class name after 'specific:'
    TEST_CLASS="${TEST_TYPE#specific:}"
    run_test "$TEST_CLASS" "$DEBUG_ARGS"
    exit_code=$?
    ;;
  *)
    print_error "Unknown test type: $TEST_TYPE"
    print_error "Run './run-tests.sh --help' for available options"
    if [ "$USE_EXISTING_ENV" != true ]; then
      cleanup_test_environment
    fi
    exit 1
    ;;
esac

# In 'all' case the exit_code is already set
if [ "$TEST_TYPE" != "all" ] && [ "${TEST_TYPE:0:9}" != "specific:" ]; then
  exit_code=$?
fi

# Clean up containers unless using existing environment
if [ "$USE_EXISTING_ENV" != true ]; then
  cleanup_test_environment
fi

if [ $exit_code -eq 0 ]; then
  print_success "Tests completed successfully!"
else
  print_error "Tests failed with exit code $exit_code"
fi

# Return the original exit code from the test
exit $exit_code