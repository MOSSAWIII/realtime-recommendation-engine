#!/bin/bash
set -e

echo "Setting up test queues in RabbitMQ..."

# Create all necessary queues 
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_events durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=processed_events durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=internal_recommendation_requests durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=recommendations durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=user_interactions durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=event_updates durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=participation_updates durable=true
docker-compose -f docker-compose-test.yml exec test-rabbitmq rabbitmqadmin declare queue name=friendship_updates durable=true

echo "Test queues created successfully"