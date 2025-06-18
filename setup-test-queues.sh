#!/bin/bash
set -e

# Generate unique vhost for each test run
TEST_RUN_ID=${TEST_RUN_ID:-$(date +%s)_$(openssl rand -hex 4)}
TEST_VHOST=${TEST_VHOST:-"test_env_${TEST_RUN_ID}"}

echo "Using unique test vhost: ${TEST_VHOST}"
echo "Test Run ID: ${TEST_RUN_ID}"

# Use PRODUCTION names (same as application.conf)
EXCHANGE="user_actions_exchange"
PROCESSED_QUEUE="processed_events_queue"
RECOMMENDATIONS_QUEUE="recommendations_queue"
RECOMMENDATION_REQUESTS_QUEUE="recommendation_requests_queue"  # ✅ NOUVEAU
HISTORICAL_QUEUE="historical_data_queue"
DLX_EXCHANGE="dlx"
DLX_QUEUE="dead_letter_queue"

echo "Setting up test queues with PRODUCTION names in isolated vhost..."
echo "Exchange: ${EXCHANGE}"
echo "Queues: ${PROCESSED_QUEUE}, ${RECOMMENDATIONS_QUEUE}, ${RECOMMENDATION_REQUESTS_QUEUE}, ${HISTORICAL_QUEUE}"
echo "DLX: ${DLX_EXCHANGE}, ${DLX_QUEUE}"

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to be ready..."
for i in {1..30}; do
  if docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl status > /dev/null 2>&1; then
    echo "RabbitMQ is ready"
    break
  fi
  echo "Waiting for RabbitMQ... ($i/30)"
  sleep 2
done

# Create test vhost if it doesn't exist
echo "Creating vhost ${TEST_VHOST}..."
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl add_vhost ${TEST_VHOST} || true
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqctl set_permissions -p ${TEST_VHOST} guest ".*" ".*" ".*"

# Create Dead Letter Exchange first
echo "Creating Dead Letter Exchange ${DLX_EXCHANGE}..."
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare exchange name=${DLX_EXCHANGE} type=direct durable=true

# Create Dead Letter Queue
echo "Creating Dead Letter Queue ${DLX_QUEUE}..."
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare queue name=${DLX_QUEUE} durable=true

# Bind DLX queue to DLX exchange
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${DLX_EXCHANGE} destination=${DLX_QUEUE} routing_key="#"

# Create main exchange
echo "Creating exchange ${EXCHANGE}..."
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare exchange name=${EXCHANGE} type=topic durable=true

# Create queues with PRODUCTION properties and consistent TTL format
echo "Creating queues with production TTL settings..."
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare queue name=${PROCESSED_QUEUE} durable=true arguments='{"x-message-ttl":86400000,"x-dead-letter-exchange":"dlx"}'
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare queue name=${RECOMMENDATIONS_QUEUE} durable=true arguments='{"x-message-ttl":1800000,"x-dead-letter-exchange":"dlx"}'
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare queue name=${RECOMMENDATION_REQUESTS_QUEUE} durable=true arguments='{"x-message-ttl":300000,"x-dead-letter-exchange":"dlx"}'  # ✅ NOUVEAU
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare queue name=${HISTORICAL_QUEUE} durable=true arguments='{"x-message-ttl":604800000,"x-dead-letter-exchange":"dlx"}'

# ✅ CORRECTION : Routing keys clairement séparés
echo "Creating bindings with SEPARATED routing keys..."
# Processed events queue
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${EXCHANGE} destination=${PROCESSED_QUEUE} routing_key="user.actions.#"
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${EXCHANGE} destination=${PROCESSED_QUEUE} routing_key="event.processed.#"

# Recommendation requests queue - pour que le Recommender consomme
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${EXCHANGE} destination=${RECOMMENDATION_REQUESTS_QUEUE} routing_key="recommendation.request.#"

# Recommendations queue - SEULEMENT pour les résultats finaux
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${EXCHANGE} destination=${RECOMMENDATIONS_QUEUE} routing_key="recommendation.result.#"

# Historical data queue
docker-compose -f docker-compose-test.yml exec -T test-rabbitmq rabbitmqadmin --vhost=${TEST_VHOST} declare binding source=${EXCHANGE} destination=${HISTORICAL_QUEUE} routing_key="historical.data.#"

# Export variables for use in tests
cat > .test-queues-env <<EOF
export TEST_RUN_ID=${TEST_RUN_ID}
export TEST_VHOST=${TEST_VHOST}
export TEST_EXCHANGE=${EXCHANGE}
export TEST_PROCESSED_EVENTS_QUEUE=${PROCESSED_QUEUE}
export TEST_RECOMMENDATIONS_QUEUE=${RECOMMENDATIONS_QUEUE}
export TEST_RECOMMENDATION_REQUESTS_QUEUE=${RECOMMENDATION_REQUESTS_QUEUE}
export TEST_HISTORICAL_DATA_QUEUE=${HISTORICAL_QUEUE}
export TEST_DLX_EXCHANGE=${DLX_EXCHANGE}
export TEST_DLX_QUEUE=${DLX_QUEUE}
EOF

echo "Test queues created with SEPARATED configuration in isolated vhost ${TEST_VHOST}"
echo "Configuration saved to .test-queues-env"