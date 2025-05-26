#!/bin/bash
# filepath: /Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/fix-package-structure-complete.sh
set -e

# Create backup
mkdir -p backup/src_$(date +%Y%m%d)
cp -r src backup/src_$(date +%Y%m%d)/

echo "Phase 1: Moving core files to application.core"
mkdir -p src/main/scala/application/core
for file in $(find src/main/scala/core -type f -name "*.scala"); do
    base_name=$(basename "$file")
    target_dir="src/main/scala/application/core/"
    echo "Moving $file to $target_dir$base_name"
    cp "$file" "$target_dir$base_name"
done

echo "Phase 2: Creating missing RabbitMQManager directory"
mkdir -p src/main/scala/infrastructure/messaging/rabbitmq

echo "Phase 3: Updating imports throughout the project"
find src -type f -name "*.scala" -exec sed -i '' 's/import core\./import application.core./g' {} \;
find src -type f -name "*.scala" -exec sed -i '' 's/package core/package application.core/g' {} \;
find src -type f -name "*.scala" -exec sed -i '' 's/import infrastructure.config.RabbitMQManager/import infrastructure.messaging.rabbitmq.RabbitMQManager/g' {} \;
find src -type f -name "*.scala" -exec sed -i '' 's/infrastructure.config.{RabbitMQManager/infrastructure.messaging.rabbitmq.RabbitMQManager/g' {} \;

echo "Phase 4: Fixing specific files"
sed -i '' 's/import infrastructure.messaging.rabbitmq.RabbitMQManager, DataManagerConfigSection}/import infrastructure.messaging.rabbitmq.RabbitMQManager\nimport infrastructure.config.DataManagerConfigSection/g' src/main/scala/application/core/DataManager.scala
sed -i '' 's/import infrastructure.messaging.rabbitmq.RabbitMQManager, ConfigurationService}/import infrastructure.messaging.rabbitmq.RabbitMQManager\nimport infrastructure.config.ConfigurationService/g' src/main/scala/application/core/Transformer.scala

echo "Package structure reorganization complete"
