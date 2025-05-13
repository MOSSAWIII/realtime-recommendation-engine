# Architecture Technique: Système de Recommandation en Temps Réel

## Vue d'ensemble

L'architecture technique présentée ici décrit un système de recommandation en temps réel organisé en couches distinctes et interconnectées. Ce système est conçu pour traiter efficacement les interactions utilisateurs et générer des recommandations personnalisées.

## Couches du Système

### Interface Utilisateur
- **Application Cliente**: Point d'entrée du système qui interagit directement avec les utilisateurs

### Couche de Messagerie
- **RabbitMQ**: Implémenté via le **RabbitMQManager** qui assure le cycle de vie des connexions et des canaux
- **Files d'attente principales**:
  - `USER_EVENTS` - Capture des événements utilisateurs
  - `PROCESSED_EVENTS` - Événements après transformation
  - `INTERNAL_REQUESTS` - Requêtes internes
  - `RECOMMENDATIONS` - Recommandations générées
  - `USER_INTERACTIONS`, `EVENT_UPDATES`, `PARTICIPATION_UPDATES`, `FRIENDSHIP_UPDATES` - Files pour les mises à jour

### Pipeline de Traitement
La pipeline contient cinq composants clés:

1. **StreamingConsumer**: Capture les événements utilisateurs bruts depuis la file `USER_EVENTS`
2. **Transformer**: Convertit les événements en données exploitables, les stocke et publie dans `PROCESSED_EVENTS`
3. **Recommender**: Analyse les données transformées et génère des recommandations personnalisées
4. **ConsumerManager**: Maintient une communication bidirectionnelle pour gérer les données contextuelles
5. **DataLoader**: Assure le chargement initial des données et met à jour les consumers en fonction des modifications

### Couche de Données
- **Repositories spécialisés**:
  - `UserRepository`
  - `EventRepository`
  - `ParticipationRepository`
  - `FriendsRepository`
  - `UserInteractionRepository`
- Tous héritent d'un **Repository générique**
- Connectés à une base de données **PostgreSQL**
- Fournissent une interface structurée pour accéder et persister les entités

### Modèles de Données
Les entités centrales du système:
- `User`
- `Event`
- `UserEvent`
- `ProcessedEvent`
- `Recommendation`
- `Participation`
- `Friendship`
- `UserInteraction`

## Algorithme de Recommandation

L'algorithme utilise une approche pondérée:
- 60% basé sur le comportement actuel de l'utilisateur
- 30% basé sur son contexte social
- 10% basé sur son historique d'interactions

## Flux de Données

### Flux Principal
Le flux principal (représenté par des lignes épaisses vertes dans le diagramme technique):
1. Application cliente → RabbitMQ
2. RabbitMQ → StreamingConsumer
3. StreamingConsumer → Transformer
4. Transformer → Repositories et PROCESSED_EVENTS
5. Recommender → Algorithme de recommandation
6. Recommender → RabbitMQ (`RECOMMENDATIONS`)
7. RabbitMQ → Application cliente

### Flux Secondaires
- DataLoader ↔ Repositories (chargement initial des données)
- Files d'attente ↔ ConsumerManager (gestion des données de référence)

## Conclusion

Cette architecture modulaire permet un traitement efficace des données en temps réel tout en maintenant un découplage entre les différents composants du système. La séparation claire des responsabilités entre les couches facilite la maintenance et l'évolution du système.