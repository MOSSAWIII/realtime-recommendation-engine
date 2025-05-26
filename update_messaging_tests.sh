#!/bin/bash

# Mettre à jour tous les tests de messaging
find src/test/scala/integration/messaging -name "*Spec.scala" -type f | while read file; do
  # Remplacer les imports de RabbitMQ
  sed -i '' 's/import common.utils.RabbitMQManager/import common.mocks.TestMockManager/g' "$file"
  sed -i '' 's/import common.mocks.RabbitMQTestManager/import common.mocks.TestMockManager/g' "$file"
  
  # Remplacer les références au RabbitMQManager
  sed -i '' 's/RabbitMQManager\./TestMockManager\./g' "$file"
  sed -i '' 's/RabbitMQTestManager\./TestMockManager\./g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends TransformerTestBase/extends IntegrationTestBase/g' "$file"
done
