# GUIDE D'EXÉCUTION DU SYSTÈME DE RECOMMANDATION EN TEMPS RÉEL
# =============================================================

Ce guide détaille l'exécution du système de recommandation en temps réel en utilisant 
des terminaux séparés dans VS Code pour un meilleur suivi et débogage.

## 1. PRÉPARATION DE L'ENVIRONNEMENT
# ----------------------------------

# Vérifier que Docker et Docker Compose sont installés
docker --version
docker-compose --version

# Créer les répertoires nécessaires s'ils n'existent pas
mkdir -p database sql-tables docs

# Vérifier la structure du projet
ls -la
# Assurez-vous que les fichiers suivants existent:
# - Dockerfile
# - docker-compose.yml
# - build.sbt
# - src/ (répertoire)
# - sql-tables/ (avec fichiers SQL d'initialisation)

# Si nécessaire, accordez les permissions d'exécution aux scripts
chmod +x *.sh  # Si vous avez des scripts shell

## 2. CONFIGURATION DE VS CODE
# --------------------------

# Ouvrez VS Code et configurez 5 terminaux distincts:
# Terminal 1: PostgreSQL
# Terminal 2: RabbitMQ
# Terminal 3: Redis
# Terminal 4: Moteur de recommandation
# Terminal 5: Commandes de diagnostic

# Pour renommer un terminal dans VS Code:
# - Cliquez droit sur l'onglet du terminal
# - Sélectionnez "Rename"
# - Donnez un nom descriptif (ex: "DB", "RabbitMQ", etc.)

## 3. TERMINAL 1: POSTGRESQL
# ------------------------

# Démarrer uniquement le service PostgreSQL
docker-compose up db

# Attendre les messages suivants:
# - "database system is ready to accept connections"
# - "database system is ready to accept connections"
# - "database system was shut down"
# - "database system is ready to accept connections"

# Ces messages confirment que PostgreSQL a démarré et a exécuté les scripts d'initialisation

## 4. TERMINAL 2: RABBITMQ
# ----------------------

# Démarrer uniquement le service RabbitMQ
docker-compose up rabbitmq

# Attendre jusqu'à voir:
# - "Server startup complete"
# - "Starting RabbitMQ 3.x.x"
# - "completed with x plugins."

# La console de gestion RabbitMQ sera accessible à:
# http://localhost:15672
# Identifiants: guest/guest (définis dans docker-compose.yml)

## 5. TERMINAL 3: REDIS
# -------------------

# Démarrer uniquement le service Redis
docker-compose up redis

# Attendre jusqu'à voir:
# - "Ready to accept connections"
# - "The server is now ready to accept connections on port 6379"

## 6. TERMINAL 5: VÉRIFICATION DES SERVICES (avant de lancer l'application)
# ----------------------------------------------------------------------

# Vérifier que tous les services sont en cours d'exécution
docker-compose ps

# Vérifier la connexion à PostgreSQL
docker-compose exec db psql -U postgres -d recommendation_engine -c "SELECT 1"

# Lister les tables dans la base de données
docker-compose exec db psql -U postgres -d recommendation_engine -c "\dt"

# Vérifier que les tables sont bien créées avec la structure attendue
docker-compose exec db psql -U postgres -d recommendation_engine -c "\d+ nom_de_votre_table"

# Vérifier la connexion à RabbitMQ
docker-compose exec rabbitmq rabbitmqctl status | grep -A 5 "Listeners"
docker-compose exec rabbitmq rabbitmqctl list_queues

# Vérifier la connexion à Redis
docker-compose exec redis redis-cli ping
# Devrait retourner "PONG"

# Vérifier que les volumes sont correctement montés
docker volume ls | grep real_time_rec_sys
docker volume inspect real_time_rec_sys_postgres_data

## 7. TERMINAL 4: MOTEUR DE RECOMMANDATION
# --------------------------------------

# Seulement après avoir vérifié que tous les services sont opérationnels
docker-compose up recommendation-engine

# Observez attentivement les logs de démarrage:
# - Connexion à la base de données
# - Connexion à RabbitMQ
# - Connexion à Redis
# - Initialisation des modèles de recommandation
# - Messages indiquant que le serveur HTTP est démarré (si applicable)

## 8. TERMINAL 5: VÉRIFICATION ET DIAGNOSTIC DU SYSTÈME COMPLET
# -----------------------------------------------------------

# Vérifier l'état de tous les conteneurs
docker-compose ps

# Vérifier la santé de l'application
curl http://localhost:8080/health

# Tester un endpoint de l'API (à adapter selon votre API)
curl -X GET "http://localhost:8080/api/recommendations?userId=123"

# Vérifier les logs de l'application spécifiquement
docker-compose logs recommendation-engine

# Vérifier les variables d'environnement dans le conteneur
docker-compose exec recommendation-engine env | grep -E "DATABASE|RABBITMQ|REDIS"

# Vérifier la communication réseau entre les conteneurs
docker-compose exec recommendation-engine ping -c 3 db
docker-compose exec recommendation-engine ping -c 3 rabbitmq
docker-compose exec recommendation-engine ping -c 3 redis

# Vérifier l'utilisation des ressources
docker stats

## 9. COMMANDES DE DÉPANNAGE UTILES
# -------------------------------

# Redémarrer un service spécifique si nécessaire
docker-compose restart recommendation-engine

# Reconstruire et redémarrer l'application après modifications
docker-compose up -d --build recommendation-engine

# Se connecter à un conteneur en shell pour diagnostic avancé
docker-compose exec recommendation-engine /bin/bash
docker-compose exec db /bin/bash

# Vérifier les logs d'un service spécifique
docker-compose logs db
docker-compose logs rabbitmq
docker-compose logs redis

# Vérifier la configuration réseau
docker network ls
docker network inspect realtime-recommendation-engine_default

# Pour vérifier l'activité dans RabbitMQ
docker-compose exec rabbitmq rabbitmqctl list_queues
docker-compose exec rabbitmq rabbitmqctl list_exchanges
docker-compose exec rabbitmq rabbitmqctl list_bindings

# Pour inspecter les données Redis
docker-compose exec redis redis-cli
# Puis dans le CLI Redis:
# KEYS *
# GET nom_de_cle
# Quitter avec 'exit'

## 10. NETTOYAGE ET ARRÊT
# ---------------------

# Arrêter tous les services mais conserver les données
docker-compose down

# Arrêter tous les services et supprimer les volumes (réinitialisation complète)
docker-compose down -v

# Supprimer les images créées
docker rmi recommendation-engine

## 11. CONSEILS DE DÉVELOPPEMENT
# ---------------------------

# Pour développer et tester une composante à la fois:
# 1. Modifiez le code source
# 2. Reconstruisez uniquement l'image de l'application
docker-compose build recommendation-engine
# 3. Redémarrez le service
docker-compose up -d recommendation-engine
# 4. Vérifiez les logs pour voir les changements
docker-compose logs -f recommendation-engine

# Pour déboguer des problèmes spécifiques:
# - Vérifiez les journaux d'erreurs
# - Utilisez des outils de monitoring comme cAdvisor ou Prometheus
# - Ajoutez des points de journalisation dans votre code

# Pour surveiller les performances:
docker stats

## 12. NOTES SUPPLÉMENTAIRES
# -----------------------

# - Pour les modifications du code Scala, vous devez reconstruire l'image
# - Surveillez l'utilisation de la mémoire, particulièrement pour Scala/JVM
# - Assurez-vous que votre application gère correctement les reconnexions
#   en cas d'indisponibilité temporaire des services dépendants
# - Pour les tests de charge, utilisez des outils comme JMeter ou Gatling

# Fin du guide d'exécution