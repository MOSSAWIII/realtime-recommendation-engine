# Guide d'Exécution - Système de Recommandation en Temps Réel

## 1. Vérifications Préliminaires

### Vérification de l'environnement
```bash
# Versions des outils
docker --version
docker-compose --version
curl --version

# Vérification des ports requis
sudo lsof -i :5432  # PostgreSQL
sudo lsof -i :5672  # RabbitMQ
sudo lsof -i :15672 # RabbitMQ Management
sudo lsof -i :8080  # Application
```

### Préparation du système
```bash
# Nettoyage docker si nécessaire
docker system prune -f
docker volume prune -f

# Construction des images
docker-compose build --no-cache
```

## 2. Configuration des Terminaux VS Code
Configurer 3 terminaux :
- Terminal 1 : Services Infrastructure (DB + RabbitMQ)
- Terminal 2 : Application
- Terminal 3 : Monitoring

## 3. Démarrage du Système

### Terminal 1 - Infrastructure
```bash
# Démarrage PostgreSQL
docker-compose up -d db
docker-compose logs -f db  # Attendre "database system is ready"

# Démarrage RabbitMQ
docker-compose up -d rabbitmq
docker-compose logs -f rabbitmq  # Attendre "Server startup complete"

# Vérification des services
docker-compose ps
```

### Terminal 2 - Application
```bash
# Démarrer l'application
docker-compose up recommendation-engine
```

### Terminal 3 - Commandes de Monitoring
```bash
# Logs en temps réel
docker-compose logs -f --tail=100

# Stats des conteneurs
docker stats

# Événements Docker
docker events
```

## 4. URLs Importantes
- RabbitMQ Management: http://localhost:15672 (guest/guest)
- API Application: http://localhost:8080

## 5. Tests et Validation

### Tests de base
```bash
# Test de santé
curl http://localhost:8080/health

# Test des recommendations (à adapter selon votre API)
curl -X GET "http://localhost:8080/api/recommendations?userId=1"
curl -X POST "http://localhost:8080/api/events" -H "Content-Type: application/json" \
  -d '{"userId":1,"eventType":"view","itemId":123}'
```

### Monitoring avancé
```bash
# Métriques détaillées RabbitMQ
curl -u guest:guest http://localhost:15672/api/overview | jq .

# Statistiques PostgreSQL
docker-compose exec db psql -U postgres -d recommendation_engine \
  -c "SELECT * FROM pg_stat_database WHERE datname = 'recommendation_engine';"
```

## 6. Commandes Utiles

### Maintenance
```bash
# Sauvegarde de la base de données
docker-compose exec db pg_dump -U postgres recommendation_engine > backup.sql

# Nettoyage des logs
docker-compose exec db vacuumdb -U postgres -d recommendation_engine --analyze

# Purge des queues RabbitMQ
docker-compose exec rabbitmq rabbitmqctl purge_queue user_events
docker-compose exec rabbitmq rabbitmqctl purge_queue recommendations
```

### Diagnostic
```bash
# Logs d'un service
docker-compose logs [service]

# Stats des conteneurs
docker stats

# Connexion shell
docker-compose exec [service] /bin/bash
```

### Vérification des services
```bash
# PostgreSQL
docker-compose exec db psql -U postgres -d recommendation_engine -c "\l"
docker-compose exec db psql -U postgres -d recommendation_engine -c "\dt"

# RabbitMQ
docker-compose exec rabbitmq rabbitmqctl list_users
docker-compose exec rabbitmq rabbitmqctl list_vhosts
docker-compose exec rabbitmq rabbitmqctl list_queues
```

## 7. Dépannage

### Base de données
```bash
# Connexions actives
docker-compose exec db psql -U postgres -c "SELECT * FROM pg_stat_activity;"

# Taille des tables
docker-compose exec db psql -U postgres -d recommendation_engine -c "\
SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) \
FROM information_schema.tables \
WHERE table_schema = 'public';"
```

### RabbitMQ
```bash
# Métriques RabbitMQ
docker-compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged

# Connexions actives
docker-compose exec rabbitmq rabbitmqctl list_connections

# Consommateurs
docker-compose exec rabbitmq rabbitmqctl list_consumers
```

## 8. Arrêt du Système

### Arrêt gracieux
```bash
# Arrêt des services
docker-compose down

# Nettoyage complet
docker-compose down -v --rmi all
docker system prune -f
```

### Sauvegarde finale
```bash
# Sauvegarde des données
docker-compose exec db pg_dump -U postgres recommendation_engine > backup_final.sql

# Export des configurations RabbitMQ
docker-compose exec rabbitmq rabbitmqctl export_definitions /tmp/rabbitmq-def.json
docker cp $(docker-compose ps -q rabbitmq):/tmp/rabbitmq-def.json ./rabbitmq-def.json
```

## Notes Importantes
- Toujours vérifier les logs avant toute opération de maintenance
- Faire des sauvegardes régulières
- Surveiller l'utilisation des ressources
- En cas de problème, vérifier dans l'ordre : réseau, base de données, RabbitMQ, application