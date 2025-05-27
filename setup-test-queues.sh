#!/bin/bash
set -e

# Generate a unique ID for this test run if not provided
TEST_RUN_ID=${TEST_RUN_ID:-$(date +%s)_$(openssl rand -hex 4)}
echo "Using test run ID: ${TEST_RUN_ID}"

# Use unique names for exchange and queues
EXCHANGE="user_actions_exchange_${TEST_RUN_ID}"
PROCESSED_QUEUE="processed_events_queue_${TEST_RUN_ID}"
RECOMMENDATIONS_QUEUE="recommendations_queue_${TEST_RUN_ID}"
HISTORICAL_QUEUE="historical_data_queue_${TEST_RUN_ID}"

echo "Setting up test queues and bindings in RabbitMQ..."
echo "Exchange: ${EXCHANGE}"
echo "Queues: ${PROCESSED_QUEUE}, ${RECOMMENDATIONS_QUEUE}, ${HISTORICAL_QUEUE}"

# Create exchange
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare exchange name=${EXCHANGE} type=topic durable=true

# Create queues
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=${PROCESSED_QUEUE} durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=${RECOMMENDATIONS_QUEUE} durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=${HISTORICAL_QUEUE} durable=true

# Create bindings
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=${EXCHANGE} destination=${PROCESSED_QUEUE} routing_key="event.processed.#"
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=${EXCHANGE} destination=${RECOMMENDATIONS_QUEUE} routing_key="recommendation.result.#"
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=${EXCHANGE} destination=${HISTORICAL_QUEUE} routing_key="historical.data.#"

# Update setup-test-queues.sh or similar scripts if they had StreamingConsumer specific configurations

# Export variables for use in tests
cat > .test-queues-env <<EOF
export TEST_RUN_ID=${TEST_RUN_ID}
export TEST_EXCHANGE=${EXCHANGE}
export TEST_PROCESSED_EVENTS_QUEUE=${PROCESSED_QUEUE}
export TEST_RECOMMENDATIONS_QUEUE=${RECOMMENDATIONS_QUEUE}
export TEST_HISTORICAL_DATA_QUEUE=${HISTORICAL_QUEUE}
EOF

echo "Test queues and bindings created successfully"
echo "Environment variables saved to .test-queues-env"
echo "Run 'source .test-queues-env' to use these variables in your shell"