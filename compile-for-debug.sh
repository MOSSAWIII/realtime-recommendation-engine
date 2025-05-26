#!/bin/bash
# filepath: /Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/compile-for-debug.sh
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
print_header "Setting up compilation environment for debugging"

# Check prerequisites
check_prerequisites

# Build the test image if needed
print_header "Building test Docker image"
docker-compose -f docker-compose-test.yml build test-runner

# Run only compilation
print_header "Compiling project code"
docker-compose -f docker-compose-test.yml run \
  --rm \
  --env DOCKER_HOST=unix:///var/run/docker.sock \
  --env TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
  --env TESTCONTAINERS_RYUK_DISABLED=true \
  -v "$COLIMA_SOCKET:/var/run/docker.sock" \
  test-runner \
  sbt compile "Test/compile"

# Check result
if [ $? -eq 0 ]; then
  print_success "Compilation completed successfully!"
  print_header "Ready for debugging"
  echo -e "You can now debug your application using:"
  echo -e "docker-compose -f docker-compose-test.yml run --rm test-runner sbt [your-debug-command]"
else
  print_error "Compilation failed"
  exit 1
fi