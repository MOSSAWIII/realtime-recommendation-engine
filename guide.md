# Guide Détaillé - Système de Recommandation en Temps Réel

## 1. Prérequis
- Docker et Docker Compose installés
- Ports disponibles : 5432, 5672, 15672, 8080
- Git (pour le clone du projet)

## 2. Configuration Initiale

### 2.1 Vérification de l'Environnement
```bash
# Vérifier les versions
docker --version
docker-compose --version

# Vérifier les ports
netstat -tulpn | grep -E '5432|5672|15672|8080'
```

### 2.2 Structure du Projet
```
realtime-recommendation-engine/
├── src/
├── Dockerfile
├── docker-compose.yml
├── build.sbt
└── project/
```

## 3. Démarrage du Système

### 3.1 Préparation
```bash
# Nettoyage de l'environnement
docker-compose down -v        # Arrête et supprime les conteneurs et volumes
docker system prune -f       # Nettoie les ressources inutilisées
```

### 3.2 Construction et Démarrage
```bash
# Construction des images
docker-compose build --no-cache

# Démarrage des services
docker-compose up -d             # Démarrage en arrière-plan
docker-compose up               # Démarrage avec logs visibles
```

## 4. Validation du Système

### 4.1 Vérification des Services
```bash
# État des conteneurs
docker-compose ps

# Logs des services
docker-compose logs -f db        # Logs de PostgreSQL
docker-compose logs -f rabbitmq  # Logs de RabbitMQ
docker-compose logs -f recommendation-engine  # Logs de l'application
```

### 4.2 Tests des Points d'Accès
| Service | URL | Identifiants |
|---------|-----|--------------|
| RabbitMQ Management | http://localhost:15672 | guest/guest |
| API Application | http://localhost:8080 | - |

## 5. Utilisation de l'API

### 5.1 Vérification de Santé
```bash
curl http://localhost:8080/health
```

### 5.2 Opérations sur les Événements
```bash
# Envoi d'un événement de vue
curl -X POST "http://localhost:8080/api/events" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "eventType": "view",
    "itemId": 123,
    "timestamp": "2024-01-20T10:00:00Z"
  }'

# Envoi d'un événement d'achat
curl -X POST "http://localhost:8080/api/events" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "eventType": "purchase",
    "itemId": 123,
    "timestamp": "2024-01-20T10:05:00Z"
  }'
```

### 5.3 Obtention des Recommandations
```bash
# Recommandations pour un utilisateur
curl "http://localhost:8080/api/recommendations?userId=1&limit=5"

# Recommandations avec filtres
curl "http://localhost:8080/api/recommendations?userId=1&category=electronics&limit=10"
```

## 6. Maintenance

### 6.1 Gestion des Services
```bash
# Redémarrage d'un service spécifique
docker-compose restart rabbitmq
docker-compose restart db
docker-compose restart recommendation-engine

# Mise à jour des services
docker-compose pull   # Télécharge les nouvelles images
docker-compose up -d  # Redémarre avec les nouvelles images
```

### 6.2 Surveillance
```bash
# Métriques des conteneurs
docker stats

# Espace disque
docker system df

# Logs récents
docker-compose logs --tail=100 -f
```

### 6.3 Base de Données
```bash
# Connexion à PostgreSQL
docker-compose exec db psql -U postgres -d recommendation_engine

# Commandes utiles PostgreSQL
\dt                    # Liste des tables
\d+ nom_table         # Structure d'une table
SELECT count(*) FROM events;  # Nombre d'événements
```

### 6.4 RabbitMQ
```bash
# État des queues
docker-compose exec rabbitmq rabbitmqctl list_queues

# Purge d'une queue
docker-compose exec rabbitmq rabbitmqctl purge_queue events_queue

# Liste des connexions
docker-compose exec rabbitmq rabbitmqctl list_connections
```

## 7. Dépannage

### 7.1 Problèmes Courants
1. **Services qui ne démarrent pas**
```bash
# Vérifier les logs
docker-compose logs [service]

# Vérifier l'état
docker-compose ps
```

2. **Problèmes de connexion**
```bash
# Vérifier le réseau Docker
docker network ls
docker network inspect realtime-recommendation-engine_default
```

3. **Problèmes de performance**
```bash
# Surveillance des ressources
docker stats
top
```

### 7.2 Réinitialisation
```bash
# Réinitialisation complète
docker-compose down -v
docker system prune -f
docker-compose up -d
```

## 8. Arrêt du Système

### 8.1 Arrêt Normal
```bash
# Arrêt gracieux
docker-compose down

# Arrêt avec suppression des volumes
docker-compose down -v
```

### 8.2 Sauvegarde
```bash
# Sauvegarde de la base de données
docker-compose exec db pg_dump -U postgres recommendation_engine > backup.sql

# Export de la configuration RabbitMQ
docker-compose exec rabbitmq rabbitmqctl export_definitions > rabbitmq_backup.json
```

## Notes Importantes
- Toujours vérifier les logs avant toute opération de maintenance
- Faire des sauvegardes régulières de la base de données
- Surveiller l'utilisation des ressources système
- En cas de problème, consulter les logs dans l'ordre : DB → RabbitMQ → Application