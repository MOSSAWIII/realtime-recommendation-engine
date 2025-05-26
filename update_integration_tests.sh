#!/bin/bash

# Mettre à jour tous les tests d'intégration pour utiliser IntegrationTestBase
find src/test/scala/integration -name "*Spec.scala" -type f | while read file; do
  # Remplacer les imports obsolètes
  sed -i '' 's/import common.utils.IntegrationSpec/import common.base.IntegrationTestBase/g' "$file"
  sed -i '' 's/import common.utils.BaseIntegrationSpec/import common.base.IntegrationTestBase/g' "$file"
  sed -i '' 's/import org.scalatest.flatspec.AnyFlatSpec/import common.base.IntegrationTestBase/g' "$file"
  sed -i '' 's/import org.scalatest.wordspec.AnyWordSpec/import common.base.IntegrationTestBase/g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends AnyFlatSpec with Matchers with BeforeAndAfterAll/extends IntegrationTestBase/g' "$file"
  sed -i '' 's/extends AnyWordSpec with Matchers with BeforeAndAfterAll/extends IntegrationTestBase/g' "$file"
  sed -i '' 's/extends IntegrationSpec/extends IntegrationTestBase/g' "$file"
  sed -i '' 's/extends BaseIntegrationSpec/extends IntegrationTestBase/g' "$file"
  
  # Remplacer les mocks
  sed -i '' 's/import common.utils.RabbitMQManager/import common.mocks.TestMockManager/g' "$file"
  sed -i '' 's/import common.utils.MockDataManager/import common.mocks.TestMockManager.MockDataManager/g' "$file"
  sed -i '' 's/import common.mocks.MockDataManager/import common.mocks.TestMockManager.MockDataManager/g' "$file"
done