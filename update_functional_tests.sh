#!/bin/bash

# Mettre à jour tous les tests fonctionnels
find src/test/scala/functional -name "*Spec.scala" -type f | while read file; do
  # Remplacer les imports
  sed -i '' 's/import org.scalatest.flatspec.AnyFlatSpec/import common.base.FunctionalTestBase/g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends AnyFlatSpec with Matchers/extends FunctionalTestBase/g' "$file"
  sed -i '' 's/extends ComponentTestBase/extends FunctionalTestBase/g' "$file"
  
  # Remplacer les mocks
  sed -i '' 's/import common.utils.MockDataManager/import common.mocks.TestMockManager.MockDataManager/g' "$file"
  sed -i '' 's/import common.utils.RabbitMQManager/import common.mocks.TestMockManager/g' "$file"
done
