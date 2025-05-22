#!/bin/bash
set -e

echo "Setting up test queues and bindings in RabbitMQ..."

# Créer l'exchange
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare exchange name=user_actions_exchange type=topic durable=true

# Créer les queues selon l'architecture simplifiée
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events_queue durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=recommendations_queue durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=historical_data_queue durable=true

# Créer les bindings
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=processed_events_queue routing_key="event.processed.#"
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=recommendations_queue routing_key="recommendation.result.#"
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare binding source=user_actions_exchange destination=historical_data_queue routing_key="historical.data.#"

echo "Test queues and bindings created successfully"