#!/bin/bash

# Identifier les tests qui utilisent des containers
grep -r "ForAllTestContainer" src/test/scala/integration | cut -d: -f1 | sort -u | while read file; do
  # Remplacer les imports
  sed -i '' 's/import common.utils.ContainerTestBase/import common.base.ContainerTestBase/g' "$file"
  
  # Remplacer l'héritage
  sed -i '' 's/extends IntegrationSpec with ForAllTestContainer/extends ContainerTestBase/g' "$file"
  sed -i '' 's/extends IntegrationTestBase with ForAllTestContainer/extends ContainerTestBase/g' "$file"
done
