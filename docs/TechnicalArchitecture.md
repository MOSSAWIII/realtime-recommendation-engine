# Architecture Technique: Système de Recommandation en Temps Réel

## Vue d'ensemble

L'architecture technique présentée ici décrit un système de recommandation en temps réel organisé en couches distinctes et interconnectées. Ce système est conçu pour traiter efficacement les interactions utilisateurs et générer des recommandations personnalisées.

## Couches du Système

### Interface Utilisateur
- **Application Cliente**: Point d'entrée du système qui interagit directement avec les utilisateurs

### Couche de Messagerie (CORRIGÉE)
- **RabbitMQ**: Implémenté via le **RabbitMQManager**
- **Files d'attente principales** (noms EXACTS du code):
  - `user_actions_exchange` - Exchange principal (type: topic)
  - `processed_events_queue` - Queue des événements traités
  - `recommendations_queue` - Queue des recommandations uniquement (pas les demandes)
  - `historical_data_queue` - Queue des données historiques
  - `dead_letter_queue` - Queue pour les messages en erreur (via DLX)

### Configuration RabbitMQ (DÉTAILS IMPLÉMENTÉS)
- **RabbitMQConfigSection**: Configuration centralisée via application.conf
- **Exchanges configurables**: Via `exchanges` map avec ExchangeConfig
- **Queues configurables**: Via `queues` map avec QueueConfig (TTL, DLX)
- **Bindings configurables**: Via `bindings` list avec BindingConfig
- **Publisher Confirms**: Activé via `confirmSelect()` et callbacks
- **Connection Management**: Auto-recovery, network recovery interval

### Routing Keys RÉELS selon le code:
- `user.actions.#` → processed_events_queue (consommation UserEvent)
- `event.processed.{eventType}` → processed_events_queue (publication ProcessedEvent)
- `recommendation.request.{userId}` → recommendations_queue (demandes)
- `recommendation.result.{userId}` → recommendations_queue (résultats)
- `historical.data.#` → historical_data_queue

**IMPORTANT**: La queue `recommendations_queue` reçoit BOTH les demandes ET les résultats.

**CORRECTION**: Les demandes de recommandations PASSENT par RabbitMQ via le Transformer.

### Pipeline de Traitement (ARCHITECTURE RÉELLE CORRIGÉE)

1. **Transformer**: 
   - Consomme depuis `user_actions_exchange`
   - Traite et enrichit les UserEvent → ProcessedEvent
   - Publie dans `processed_events_queue` avec routing key `event.processed.{eventType}`
   - **GÉNÈRE** des RecommendationRequest via `publishRecommendationRequest()`
   - Publie les demandes avec routing key `recommendation.request.{userId}`

2. **Recommender**: 
   - Consomme depuis `processed_events_queue` 
   - **CONSOMME AUSSI** les RecommendationRequest depuis l'exchange
   - Génère les recommandations selon l'algorithme pondéré
   - Publie dans `recommendations_queue` avec routing key `recommendation.result.{userId}`

3. **DataManager**: 
   - Gestion centralisée de TOUTES les données PostgreSQL
   - **Remplace complètement** les repositories individuels
   - Gestion des transactions via `DataManagerTransactions`
   - Cache intégré avec TTL configurable
   - Notifications de synchronisation via `historical_data_queue`

### Couche de Cache (ARCHITECTURE DÉTAILLÉE)
Le **Recommender** implémente un système de cache multi-niveaux sophistiqué:

#### Caches LRU Intégrés CORRECTS:
- `userProfileCache: LRUCache[Long, User]` - Profils utilisateurs (max 10000 entrées)
- `eventCache: TrieMap[Long, Event]` - Événements actifs par ID
- `userFriendsCache: TrieMap[Long, List[UserFriend]]` - Relations d'amitié par utilisateur
- `recentUserEvents: TrieMap[Long, mutable.Queue[ProcessedEvent]]` - Queue limitée à 100 événements/utilisateur
- `recentRecommendations: TrieMap[Long, (Recommendation, Long)]` - Cache avec timestamp (TTL configurable)

**CORRECTION**: La taille du userProfileCache est 10000, pas 1000.

#### Mécanismes de Cache Avancés:
- **TTL dynamique** : 30 secondes pour recommandations, configurable via `recommendationCacheTTL`
- **LRU eviction** : Éviction automatique des anciennes entrées
- **Maintenance automatique** : Scheduler toutes les 5 minutes via `scheduleBackgroundTasks()`
- **Invalidation intelligente** : Via messages de synchronisation DataManager (`startDataManagerSyncConsumer()`)
- **Circuit Breaker** : Protection contre les surcharges (seuil 5 échecs, reset 30s)

### Couche de Données (CONSOLIDÉE)
- **DataManager UNIQUE** : Remplace tous les repositories (UserRepository, EventRepository, etc.)
- **Interface unifiée** : Toutes les opérations CRUD via DataManager
- **Connection Management** : HikariCP avec pool configuré
- **Transaction Management** : `DataManagerTransactions` avec isolation configurable
- **Cache intégré** : Cache L1 avec TTL pour réduire la charge DB

### Modèles de Données (EXACTS)
Les entités centrales selon le code:
- `User` - Profils utilisateurs
- `Event` - Événements système  
- `UserEvent` - Événements bruts utilisateurs
- `ProcessedEvent` - Événements enrichis et traités
- `Recommendation` - Recommandations avec `RecommendedItem`
- `RecommendedItem` - Items recommandés avec score et raison
- `Participation` - Participation aux événements
- `UserFriend` - Relations d'amitié avec `strength` (pas affinity)
- `RecommendationRequest` - Demandes internes (via RabbitMQ)

## Algorithme de Recommandation (IMPLÉMENTATION EXACTE)
L'algorithme utilise les poids définis dans `RecommenderConstants`:
- **60% (CurrentBehaviorWeight = 0.6)** - Comportement récent et fréquence
- **30% (ContextualWeight = 0.3)** - Contexte géographique et social  
- **10% (HistoricalWeight = 0.1)** - Patterns historiques long-terme

### Composants de Score IMPLÉMENTÉS:
#### Behavior Score (60%) - `calculateBehaviorScore()`:
- **Fréquence** : `math.min(10.0 * itemEvents.size, 50.0)`
- **Récence** : Score dégressif (50 pts < 24h, 30 pts < 3j, 15 pts < 1 semaine)
- **Type d'interaction** : purchase/register (20 pts), click (5 pts), view (1 pt)

#### Contextual Score (30%) - `calculateContextualScore()`:
- **Géographique** (50%) : Même lieu (50 pts) > Proximité (30 pts) > Même région (15 pts)
- **Social** (50%) : Amis participants (30 pts base + 5 pts/ami + pondération par `strength`)

#### Historical Score (10%) - `calculateHistoricalScore()`:
- **Correspondance catégories** : 15 points par match de catégorie
- **Plafonné à 100** pour normalisation

## Flux de Données (ARCHITECTURE RÉELLE)

### Flux Principal CORRIGÉ:
1. **Application cliente** → `user_actions_exchange` (UserEvent avec routing `user.actions.#`)
2. **Transformer** consomme user_actions_exchange
3. **Transformer** → `processed_events_queue` (ProcessedEvent avec routing `event.processed.{eventType}`)
4. **Transformer** → `recommendations_queue` (RecommendationRequest avec routing `recommendation.request.{userId}`)
5. **Recommender** consomme les RecommendationRequest depuis `recommendations_queue`
6. **Recommender** génère les recommandations via `generateRecommendations()`
7. **Recommender** → `recommendations_queue` (Recommendation avec routing `recommendation.result.{userId}`)
8. **Application cliente** consomme les résultats depuis `recommendations_queue`

### Flux Secondaires:
- **DataManager** ↔ PostgreSQL (transactions via HikariCP)
- **DataManager** → `historical_data_queue` (notifications de mise à jour)
- **Recommender** consomme `historical_data_queue` → invalidation cache
- **Recommender** ↔ Caches internes (mise à jour/éviction automatique)

## Déclenchement des Recommandations (ARCHITECTURE RÉELLE)

Les recommandations sont déclenchées par **deux mécanismes**:

1. **Via RecommendationRequest** (flux principal):
   - Transformer génère RecommendationRequest après ProcessedEvent
   - Condition: `if (userEvent.itemId.isDefined)` dans `processEvent()`
   - Publication vers `recommendations_queue` avec routing `recommendation.request.{userId}`

2. **Via mise à jour du cache** (flux interne):
   - `updateUserEventCache()` appelée lors de la consommation des ProcessedEvent
   - Maintient la cohérence du cache interne pour les futures recommandations

### Résilience et Monitoring (IMPLÉMENTÉ)

#### Circuit Breaker Intégré:
- **Variables** : `circuitOpen`, `consecutiveFailures`, `lastFailureTime`
- **Seuil** : 5 échecs consécutifs (`failureThreshold`)
- **Reset** : 30 secondes (`resetTimeout`)
- **Protection** : Reject des demandes quand circuit ouvert

#### Métriques CODÉES:
- `requestTimer` - Durée via `MetricsRegistry.buildTimer()`
- `recommendationsCounter` - Compteur via `MetricsRegistry.buildCounter()`
- `cacheHitRatio` - Gauge via `MetricsRegistry.buildGauge()` 
- `scoreHistogram` - Distribution via `MetricsRegistry.buildHistogram()`

#### Health Checks IMPLÉMENTÉS:
- **Méthode** : `healthCheck()` avec validation complète
- **Vérifications** : caches, RabbitMQ channels, DataManager
- **Registry** : Enregistrement dans `HealthCheckRegistry`
- **Statuts** : Healthy/Degraded avec détails

## Architecture de Test (RÉELLE)

### Composants de Test EXACTS:
- `MockDataManager` - Mock complet de DataManager avec données en mémoire
- `TestRepository` - Repository de test (DISTINCT de MockDataManager)
- `TestDataFactory` - Factory pour données cohérentes
- `TestMockManager` - Gestion des queues de test (pas MockManager)
- `IntegrationTestBase` - Base pour tests d'intégration
- `TransformerRecommenderMessagingSpec` - Tests end-to-end messaging

### Infrastructure de Test:
- **Isolation** : Vhosts RabbitMQ uniques par `TEST_RUN_ID`
- **TestContainers** : PostgreSQL et RabbitMQ isolés
- **Configuration** : `application-test.conf` avec overrides de test
- **Scripts** : `run-tests.sh` avec support unitaire sans conteneurs

## Conclusion

Cette architecture modulaire avec cache multi-niveaux et circuit breaker permet un traitement efficace en temps réel. La consolidation via DataManager et l'auto-déclenchement des recommandations simplifie l'architecture tout en maintenant les performances.