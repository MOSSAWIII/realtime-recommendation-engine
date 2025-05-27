#!/bin/bash
# filepath: /Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/compile-for-debug.sh
set -e

# Colors for output formatting
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Generate a unique test run ID
TEST_RUN_ID=$(date +%s)_$(openssl rand -hex 4)
export TEST_RUN_ID

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

function check_prerequisites() {
  # Check if Colima is running
  COLIMA_SOCKET="$HOME/.colima/default/docker.sock"
  if [ ! -S "$COLIMA_SOCKET" ]; then
    print_error "Error: Colima socket not found at $COLIMA_SOCKET"
    print_error "Please make sure Colima is running with: colima start"
    exit 1
  fi
}

# Main execution
print_header "Setting up compilation environment for debugging with test run ID: ${TEST_RUN_ID}"

# Check prerequisites
check_prerequisites

# Build the test image if needed
print_header "Building test Docker image"
docker-compose -f docker-compose-test.yml build test-runner

# Export test environment variables
export TEST_DB_NAME="test-db-${TEST_RUN_ID}"
export TEST_RABBITMQ_NAME="test-rabbitmq-${TEST_RUN_ID}"
export TEST_EXCHANGE="user_actions_exchange_${TEST_RUN_ID}"
export TEST_PROCESSED_EVENTS_QUEUE="processed_events_queue_${TEST_RUN_ID}"
export TEST_RECOMMENDATIONS_QUEUE="recommendations_queue_${TEST_RUN_ID}"
export TEST_HISTORICAL_DATA_QUEUE="historical_data_queue_${TEST_RUN_ID}"

# Create test environment file
cat > .test_env <<EOF
export TEST_RUN_ID=${TEST_RUN_ID}
export TEST_DB_NAME=${TEST_DB_NAME}
export TEST_RABBITMQ_NAME=${TEST_RABBITMQ_NAME}
export TEST_EXCHANGE=${TEST_EXCHANGE}
export TEST_PROCESSED_EVENTS_QUEUE=${TEST_PROCESSED_EVENTS_QUEUE}
export TEST_RECOMMENDATIONS_QUEUE=${TEST_RECOMMENDATIONS_QUEUE}
export TEST_HISTORICAL_DATA_QUEUE=${TEST_HISTORICAL_DATA_QUEUE}
EOF

# Run only compilation with detailed output
print_header "Compiling project code with detailed output"
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
  -v "$COLIMA_SOCKET:/var/run/docker.sock" \
  test-runner \
  sbt "-Dtest.runId=${TEST_RUN_ID} -deprecation -verbose -Dlog.level=debug compile" "Test/compile"

# Check result
if [ $? -eq 0 ]; then
  print_success "Compilation completed successfully!"
  print_header "Ready for debugging"
  echo -e "Environment variables:"
  cat .test_env
  echo -e "\nYou can now debug your application using:"
  echo -e "source .test_env"
  echo -e "docker-compose -f docker-compose-test.yml run --rm --env-file .test_env test-runner sbt [your-debug-command]"
else
  print_error "Compilation failed"
  exit 1
fi