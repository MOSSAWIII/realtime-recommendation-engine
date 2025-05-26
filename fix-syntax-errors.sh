#!/bin/bash
# filepath: /Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/fix-syntax-errors.sh

set -e

# Fix DataManager mapResultSetToEvent method
sed -i '' 's/private def mapResultSetToEvent(createdBy = 0L,rs: ResultSet): Event = {/private def mapResultSetToEvent(rs: ResultSet): Event = {/g' src/main/scala/application/core/DataManager.scala

# Fix DataManager extra bracket
sed -i '' 's/^}$//' src/main/scala/application/core/DataManager.scala

# Fix Recommender.scala opening brace
sed -i '' '1s/^/package application.core\n\n/' src/main/scala/application/core/Recommender.scala

echo "Fixed syntax errors in DataManager.scala and Recommender.scala"