#!/bin/bash

# Mettre à jour tous les tests unitaires pour utiliser UnitTestBase
find src/test/scala/unit -name "*Spec.scala" -type f | while read file; do
  # Remplacer les imports obsolètes
  sed -i '' 's/import common.utils.UnitFlatSpec/import common.base.UnitTestBase/g' "$file"
  sed -i '' 's/import org.scalatest.flatspec.AnyFlatSpec/import common.base.UnitTestBase/g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends AnyFlatSpec with Matchers/extends UnitTestBase/g' "$file"
  sed -i '' 's/extends UnitFlatSpec/extends UnitTestBase/g' "$file"
  
  # Modifier les imports de TestDataFactory si nécessaire
  sed -i '' 's/import common.utils.TestDataFactory/import common.fixtures.TestDataFactory/g' "$file"
  
  # Modifier les imports de MockDataManager
  sed -i '' 's/import common.utils.MockDataManager/import common.mocks.TestMockManager.MockDataManager/g' "$file"
  sed -i '' 's/import common.mocks.MockDataManager/import common.mocks.TestMockManager.MockDataManager/g' "$file"
done
