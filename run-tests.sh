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
  
  # Export environment variables for docker-compose
  export TEST_DB_NAME="test-db-${TEST_RUN_ID}"
  export TEST_RABBITMQ_NAME="test-rabbitmq-${TEST_RUN_ID}"
  
  # Start containers avec gestion d'erreur améliorée
  if ! docker-compose -f docker-compose-test.yml up -d; then
    print_error "Failed to start containers. Checking logs..."
    
    # Afficher les logs détaillés pour diagnostiquer
    echo "=== RabbitMQ Logs ==="
    docker-compose -f docker-compose-test.yml logs test-rabbitmq
    echo "=== DB Logs ==="
    docker-compose -f docker-compose-test.yml logs test-db
    
    # Vérifier si c'est un problème de version RabbitMQ
    if docker-compose -f docker-compose-test.yml logs test-rabbitmq 2>&1 | grep -q "deprecated"; then
      print_error "RabbitMQ version issue detected. Try using rabbitmq:3.11-management instead"
    fi
    
    cleanup_containers
    exit 1
  fi
  
  # Attendre plus longtemps pour RabbitMQ
  echo "Waiting for containers to initialize..."
  sleep 30
  
  # Vérifier que RabbitMQ est vraiment prêt
  echo "Checking RabbitMQ health..."
  for i in {1..12}; do
    if docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1; then
      echo "RabbitMQ is ready"
      break
    fi
    echo "Waiting for RabbitMQ... ($i/12)"
    sleep 5
  done
  
  # Setup RabbitMQ resources
  setup_rabbitmq_queues
  
  # Create a comprehensive environment file
  cat > .test_env <<EOF
export TEST_RUN_ID=${TEST_RUN_ID}
export TEST_DB_NAME=${TEST_DB_NAME}
export TEST_RABBITMQ_NAME=${TEST_RABBITMQ_NAME}
export TEST_VHOST=test_env_${TEST_RUN_ID}
export TEST_EXCHANGE=user_actions_exchange
export TEST_PROCESSED_EVENTS_QUEUE=processed_events_queue
export TEST_RECOMMENDATIONS_QUEUE=recommendations_queue
export TEST_HISTORICAL_DATA_QUEUE=historical_data_queue
EOF
  
  # Source the environment variables
  source .test_env
  
  echo "Test environment ready with isolated vhost: test_env_${TEST_RUN_ID}"
}

function setup_rabbitmq_queues() {
  echo "Setting up RabbitMQ with production configuration..."
  
  # Use unique vhost for complete isolation
  export TEST_VHOST="test_env_${TEST_RUN_ID}"
  
  # Create test vhost with isolation guarantees
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl add_vhost ${TEST_VHOST}
  docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl set_permissions -p ${TEST_VHOST} guest ".*" ".*" ".*"
  
  # Export the vhost for the setup script
  export TEST_VHOST=${TEST_VHOST}
  export TEST_RUN_ID=${TEST_RUN_ID}
  
  # Run the updated setup script with isolation
  bash setup-test-queues.sh
  
  # Source the generated environment
  if [ -f .test-queues-env ]; then
    source .test-queues-env
    echo "Loaded isolated test environment for vhost: ${TEST_VHOST}"
  fi
}

function cleanup_test_environment() {
  local test_run_id=${TEST_RUN_ID}
  local test_vhost="test_env_${test_run_id}"
  
  if [ -n "$test_run_id" ]; then
    echo "Cleaning up test environment with ID: ${test_run_id}..."
    
    # Vérifier si RabbitMQ est en cours d'exécution avant de nettoyer
    if docker-compose -f docker-compose-test.yml ps test-rabbitmq 2>/dev/null | grep -q "Up"; then
      echo "Cleaning up RabbitMQ vhost: ${test_vhost}..."
      # Delete the entire vhost for complete cleanup
      docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl delete_vhost ${test_vhost} 2>/dev/null || true
    else
      echo "RabbitMQ not running, skipping queue cleanup (normal for unit tests)"
    fi
  fi
  
  # Arrêter les conteneurs (s'ils existent)
  docker-compose -f docker-compose-test.yml down --remove-orphans 2>/dev/null || true
  
  # Nettoyer les fichiers temporaires
  rm -f .test_run_id .test_env .test-queues-env
}

function run_test() {
  local test_pattern=$1
  local debug_args=$2
  
  echo "Running test: $test_pattern with TEST_RUN_ID=${TEST_RUN_ID}"
  
  # Pour les tests unitaires, pas besoin de conteneurs
  if [[ "$test_pattern" == "unit."* ]]; then
    # Test unitaire - utiliser sbt directement dans le conteneur
    docker-compose -f docker-compose-test.yml run \
      --rm \
      --no-deps \
      --env TEST_RUN_ID=${TEST_RUN_ID} \
      test-runner \
      sbt "testOnly $test_pattern"
  else
    # Test d'intégration - utiliser tous les services avec vhost
    docker-compose -f docker-compose-test.yml run \
      --rm \
      --env TEST_RUN_ID=${TEST_RUN_ID} \
      --env TEST_VHOST=${TEST_VHOST:-test_env_${TEST_RUN_ID}} \
      --env TEST_EXCHANGE=${TEST_EXCHANGE} \
      --env TEST_PROCESSED_EVENTS_QUEUE=${TEST_PROCESSED_EVENTS_QUEUE} \
      --env TEST_RECOMMENDATIONS_QUEUE=${TEST_RECOMMENDATIONS_QUEUE} \
      --env TEST_HISTORICAL_DATA_QUEUE=${TEST_HISTORICAL_DATA_QUEUE} \
      test-runner \
      sbt "testOnly $test_pattern"
  fi
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
  
  # Déterminer si on a besoin de conteneurs
  NEEDS_CONTAINERS=false
  case "$TEST_TYPE" in
    "integration"*|"functional"*|"all"|"tag:RabbitMQTest"|"tag:DatabaseTest")
      NEEDS_CONTAINERS=true
      ;;
    "component:"*)
      NEEDS_CONTAINERS=true
      ;;
    "specific:"*)
      # Check if the specific test is an integration test
      if [[ "$TEST_TYPE" == *"integration."* || "$TEST_TYPE" == *"functional."* || "$TEST_TYPE" == *"Messaging"* || "$TEST_TYPE" == *"demo."* ]]; then
        NEEDS_CONTAINERS=true
      fi
      ;;
  esac
  
  if [ "$NEEDS_CONTAINERS" = true ]; then
    setup_test_environment
  else
    # Pour les tests unitaires, juste générer un ID
    generate_test_run_id
    echo "Skipping container setup for unit tests"
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
    print_info "Running database integration tests with conditional tests enabled..."
    docker-compose -f docker-compose-test.yml exec -T test-runner \
      bash -c "
        # Test direct connection first
        echo 'Testing database connection...'
        PGPASSWORD=test_password psql -h test-db -U postgres -d test_recommendation_engine -c 'SELECT 1' && 
        # Run the tests with increased timeouts
        sbt -Dtest.database.enabled=true \
            -Dtest.mode=true \
            -Dtest.database.timeout=60000 \
            -Dconfig.resource=application-test.conf \
            'Test/testOnly integration.db.**'
      "
    exit_code=$?
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
    # Tests unitaires et d'intégration pour DataManager
    echo "Running DataManager unit tests..."
    run_test "unit.application.core.DataManagerSpec" "$DEBUG_ARGS"
    
    # Optionnel: ajouter les tests d'intégration si ils existent
    if [ "$1" == "--include-integration" ]; then
      echo "Running DataManager integration tests..."
      run_test "integration.db.DataManager*" "$DEBUG_ARGS"
    fi
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