#!/bin/bash
set -e

echo "Setting up test queues in RabbitMQ..."

# Create the necessary queues
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_events durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=internal_recommendation_requests durable=true

echo "Test queues created successfully"