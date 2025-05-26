#!/bin/bash

# Identifier les tests de repository
find src/test/scala/unit/infrastructure/persistence -name "*Spec.scala" -type f | while read file; do
  # Remplacer les mocks
  sed -i '' 's/import common.utils.MockManager/import common.mocks.TestMockManager/g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends AnyFunSuite/extends UnitTestBase/g' "$file"
  sed -i '' 's/extends AnyFlatSpec/extends UnitTestBase/g' "$file"
  
  # Remplacer les imports
  sed -i '' 's/import org.scalatest.funsuite.AnyFunSuite/import common.base.UnitTestBase/g' "$file"
  sed -i '' 's/import org.scalatest.flatspec.AnyFlatSpec/import common.base.UnitTestBase/g' "$file"
done
