# Pipeline du système de recommandation en temps réel

Ce document décrit le flux de données à travers notre système de recommandation.

## Pipeline global

[Événements user] → [RabbitMQ [Streaming, Affichage]] → [Service de transformation de données ↔ BD historisés] → [Service de recommandation] → [Affichage] → [Recommandation]

## Composants et responsabilités

1. **Sources d'événements utilisateur**
   - Applications web/mobile génèrent des événements (clics, vues, recherches)
   - Format: `UserEvent` (userId, eventType, itemId, properties, timestamp)

2. **RabbitMQ - Ingestion**
   - Queue: `USER_EVENTS` - événements utilisateur en temps réel
   - Queues secondaires pour les mises à jour de données de référence:
     - `USER_INTERACTIONS` - mises à jour utilisateurs
     - `EVENT_UPDATES` - mises à jour événements
     - `PARTICIPATION_UPDATES` - mises à jour participations
     - `FRIENDSHIP_UPDATES` - mises à jour amitiés

3. **Consumers**
   - `StreamingConsumer` - traite les événements utilisateur en temps réel
   - `ConsumerManager` - gère les mises à jour des données de référence

4. **Transformation (Transformer)**
   - Nettoie et enrichit les événements bruts
   - Convertit `UserEvent` en `ProcessedEvent`
   - Déclenche la génération de recommandations

5. **Base de données (PostgreSQL)**
   - Tables principales:
     - `UTILISATEUR`, `EVENEMENT`, `PARTICIPATION_EVENEMENT` (données de référence)
     - `processed_events` (événements traités)
     - `recommendations` (recommandations générées)

6. **Service de recommandation (Recommender)**
   - Génère des recommandations basées sur les événements traités
   - Algorithme: combine fréquence (70%) et récence (30%)
   - Publie les résultats dans la queue `RECOMMENDATIONS`

7. **Affichage**
   - Systèmes externes qui consomment les recommandations
   - Format: `Recommendation` (userId, recommendedItems, score, timestamp)

## Flux de données

1. Un utilisateur interagit avec l'application (vue, clic, etc.)
2. L'événement est envoyé à la queue `USER_EVENTS` de RabbitMQ
3. `StreamingConsumer` récupère l'événement et le transmet au `Transformer`
4. `Transformer` traite l'événement et le stocke dans `processed_events`
5. `Transformer` déclenche `Recommender` pour générer/mettre à jour les recommandations
6. `Recommender` calcule les recommandations à partir des événements traités
7. `Recommender` stocke les recommandations dans `recommendations` et les publie dans RabbitMQ
8. Les systèmes d'affichage consomment les recommandations à partir de RabbitMQ

## Flux de données de référence

Parallèlement, le système maintient à jour ses données de référence:

1. Des mises à jour (utilisateurs, événements, etc.) sont envoyées aux queues dédiées
2. `ConsumerManager` gère ces mises à jour et les stocke dans:
   - La base de données PostgreSQL via les repositories
   - Le cache en mémoire via `CacheManager`

Ce modèle dual permet un traitement optimisé à la fois des données de référence et des événements en temps réel.