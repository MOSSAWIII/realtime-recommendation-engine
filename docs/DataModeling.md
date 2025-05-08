# Documentation de la Modélisation des Données - Système de Recommandation en Temps Réel

## Modèles de Données Principaux

Ce document présente la modélisation des données mise en œuvre dans notre système de recommandation en temps réel:

### 1. Événements et Interactions

#### UserEvent
```scala
case class UserEvent(
  userId: Long,
  eventType: String,
  itemId: Option[Long] = None,
  properties: Option[Map[String, String]] = None,
  timestamp: Long = System.currentTimeMillis()
)
```

- Représente les données brutes d'une action utilisateur
- `itemId` est optionnel car certains événements peuvent ne pas concerner un item spécifique
- `properties` stocke des métadonnées additionnelles sous forme de paires clé-valeur

#### ProcessedEvent
```scala
case class ProcessedEvent(
  id: Option[Long] = None,
  userId: Long,
  eventType: String,
  itemId: Option[Long],
  properties: Map[String, String] = Map.empty,
  timestamp: Long,
  processingTimestamp: Long = System.currentTimeMillis()
)
```

- Version enrichie et persistée d'un `UserEvent`
- Ajoute un `id` de base de données et un `processingTimestamp`
- `properties` devient une Map non-optionnelle (valeur par défaut: Map vide)

### 2. Recommandations

#### RecommendationRequest
```scala
case class RecommendationRequest(
  userId: Long,
  timestamp: Long = System.currentTimeMillis()
)
```

- Demande simple pour générer des recommandations pour un utilisateur

#### Recommendation
```scala
case class Recommendation(
  id: Option[Long] = None,
  userId: Long,
  recommendedItems: List[RecommendedItem],
  score: Double,
  timestamp: Long = System.currentTimeMillis()
)
```

- Contient une liste d'items recommandés et un score global

#### RecommendedItem
```scala
case class RecommendedItem(
  itemId: Long,
  score: Double,
  reason: String
)
```

- Un item individuel dans une recommandation
- Inclut un score de pertinence et une explication

### 3. Utilisateurs et Profils

#### User
```scala
case class User(
  userId: Long,
  name: Option[String] = None,
  emailAddress: Option[String] = None,
  locale: String = "en_US",
  birthYear: Option[Int] = None,
  gender: Option[String] = None,
  location: Option[String] = None,
  timezone: Option[Int] = None,
  isGenerated: Boolean = false
)
```

- Informations démographiques et de profil utilisateur

### 4. Données Sociales et Événements

#### Event
```scala
case class Event(
  id: Option[Long],
  name: String,
  description: Option[String] = None,
  startTime: Long,
  endTime: Option[Long] = None,
  location: Option[String] = None,
  createdBy: Long,
  status: String = "active",
  keywords: List[String] = List.empty,
  keywordFrequencies: Map[String, Int] = Map.empty
)
```

- Représente un événement avec métadonnées et statistiques

#### EventParticipation / Participation
```scala
case class EventParticipation(
  id: Option[Long],
  userId: Long,
  eventId: Long,
  status: String
)

case class Participation(
  userId: Long,
  eventId: Long,
  status: String
)
```

- Lie un utilisateur à un événement avec un statut de participation

#### Friendship
```scala
case class Friendship(
  id: Option[Long],
  userId1: Long,
  userId2: Long,
  status: String,
  createdAt: Long
)
```

- Représente une relation entre deux utilisateurs

#### UserFriends
```scala
case class UserFriends(
  userId: Long,
  friendIds: List[Long]
)
```

- Structure optimisée pour stocker la liste d'amis d'un utilisateur

## Schéma de Base de Données

Le système utilise deux tables principales dans PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS processed_events (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  event_type VARCHAR(255) NOT NULL,
  item_id BIGINT,
  properties JSONB NOT NULL,
  timestamp BIGINT NOT NULL,
  processing_timestamp BIGINT NOT NULL
)

CREATE TABLE IF NOT EXISTS recommendations (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  recommended_items JSONB NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  timestamp BIGINT NOT NULL
)
```

- **processed_events**: Stocke les événements utilisateurs traités
- **recommendations**: Stocke les recommandations générées
- Utilisation du format JSONB pour les structures complexes (properties, recommended_items)

## Flux de Données

1. Les événements bruts (`UserEvent`) arrivent via RabbitMQ
2. Le Transformer les convertit en `ProcessedEvent` et les stocke en base de données
3. Pour les événements avec un `itemId`, une `RecommendationRequest` est générée
4. Le Recommender utilise l'historique des `ProcessedEvent` pour générer des recommandations
5. Les recommandations sont enregistrées en base de données et publiées si nécessaire

## Caractéristiques Techniques

- Utilisation systématique d'`Option[T]` pour les champs facultatifs
- Horodatages en millisecondes (epoch time) pour tous les événements temporels
- Structures de données immuables (case classes)
- Propriétés flexibles via `Map[String, String]`
- Sérialisation/désérialisation JSON avec la bibliothèque circe
- Accès aux données via Repository pattern et JDBC

## Justification du modèle de données du système de recommandation

Le système adopte une modélisation de données orientée événements qui sépare clairement les données brutes des données transformées, permettant un traitement efficace en temps réel et une flexibilité maximale. L'utilisation de case classes immuables avec des champs optionnels (Option[T]) offre une sécurité de type tout en accommodant des données potentiellement incomplètes. La distinction entre UserEvent (événement brut) et ProcessedEvent (événement enrichi) permet une transformation progressive des données sans perte d'information. Le stockage dans PostgreSQL avec format JSONB pour les propriétés variables (properties) combine le meilleur des mondes: la structure stable d'un schéma relationnel et la flexibilité d'un stockage semi-structuré. Cette approche facilite l'évolution du système sans migrations de schéma complexes. L'ajout de timestamps à chaque niveau (création, traitement) assure une traçabilité complète des données et permet des analyses temporelles précises. La modélisation du graphe social (User, Friendship) et des interactions contextuelles (Event, EventParticipation) enrichit les données comportementales pour des recommandations plus pertinentes. Enfin, l'architecture orientée messages avec RabbitMQ garantit le découplage entre les producteurs d'événements et les consommateurs, assurant ainsi une haute disponibilité et une excellente scalabilité horizontale du système.
