# To-Do Liste Détaillée pour l'Alignement avec l'Architecture Technique de Référence

## 1. Suppression du DataLoader et consolidation dans DataManager

### Tâches:
- Supprimer complètement DataLoader.scala
- Vérifier les dépendances du constructeur dans les classes qui l'utilisent
- Modifier les services qui injectent DataLoader pour utiliser directement DataManager
- Mettre à jour les tests qui référencent DataLoader
- Intégrer les méthodes utiles de DataLoader dans DataManager
  - Vérifier si loadAllAsync() et loadAll() sont nécessaires; si oui, les ajouter à DataManager
  - Migrer la méthode getEventKeywords() vers DataManager si elle est utilisée ailleurs
- S'assurer que le consommateur historical_data_queue est correctement implémenté dans DataManager
  - Confirmer que initHistoricalDataConsumer() dans DataManager est complet et fonctionnel
  - S'assurer que le traitement des messages suit la même logique que celle qui était dans DataLoader

## 2. Intégration de StreamingConsumer dans Transformer

### Tâches:
- Modifier la classe Transformer
  ```scala
  def start(): Unit = {
    logger.info("Starting Transformer consumer...")
    
    // Créer un canal pour le consumer
    val channel = RabbitMQManager.createChannel("transformer-consumer")
    
    // Déclarer le topic exchange
    val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
    channel.exchangeDeclare(exchange, "topic", true)
    
    // Logique du consommateur (inspiré de StreamingConsumer)
    // ...
  }
  ```
- Ajouter un gestionnaire de fermeture à Transformer
- Migrer les métriques de StreamingConsumer vers Transformer
- Supprimer la classe StreamingConsumer
  - Vérifier et adapter toutes les références à StreamingConsumer dans le code
  - Mettre à jour les tests utilisant StreamingConsumer pour qu'ils utilisent Transformer directement

## 3. Révision des Repositories et consolidation dans DataManager

### Tâches:
- Analyser l'utilisation des repositories
- Migrer les méthodes manquantes des repositories vers DataManager
- Simplifier les dépendances d'injection

## 4. Nettoyage des configurations RabbitMQ obsolètes

### Tâches:
- Mettre à jour RabbitMQManager
  ```scala
  object Queues {
    val PROCESSED_EVENTS = "processed_events_queue"
    val RECOMMENDATIONS = "recommendations_queue"
    val HISTORICAL_DATA = "historical_data_queue"
  }
  ```
- Corriger les routing keys
- Vérifier les bindings

## 5. Nettoyage du code mort dans les classes principales

### Tâches:
- Nettoyer Transformer.scala
- Nettoyer DataManager.scala
- Nettoyer les fichiers de configuration

## 6. Mise à jour des fichiers de test

### Tâches:
- Mettre à jour les tests d'intégration
- Mettre à jour les mocks
- Adapter les tests e2e et d'intégration

## 7. Mise à jour de la documentation et des exemples

### Tâches:
- Mettre à jour la documentation technique
- Créer ou mettre à jour les exemples d'utilisation

## 8. Vérification finale et validation

### Tâches:
- Exécuter tous les tests
- Validation de l'architecture
- Revue de performance



Comment mettre à jour la classe TransformerSpec.scala pour qu'elle reflète les changements suivants:
1. Le constructeur de Transformer prend maintenant DataManager comme paramètre
2. La méthode start() gère désormais la consommation des messages
3. La méthode close() ferme désormais les canaux de consommation et de publication

Comment adapter la classe de test TransformerUnderTest pour qu'elle fonctionne avec la nouvelle signature du constructeur de Transformer et évite les appels RabbitMQ réels pendant les tests?

Comment identifier et supprimer toutes les références à StreamingConsumer dans les tests d'intégration et remplacer par l'utilisation directe de Transformer? Par exemple dans PipelineIntegrationSpec.scala.

Comment mettre à jour TransformerRecommenderMessagingSpec.scala pour utiliser directement Transformer à la place de StreamingConsumer et adapter les méthodes de setup et teardown?

Comment refactoriser StreamingTransformerIntegrationSpec.scala pour utiliser la nouvelle conception où Transformer consomme directement les messages? Que faire de la classe TestStreamingConsumer?

Comment créer une implémentation mock de DataManager pour remplacer les mocks des repositories individuels dans les tests?

Comment adapter CompletePipelineSpec.scala pour utiliser la nouvelle architecture sans StreamingConsumer et avec DataManager au lieu des repositories?

Comment refactoriser l'initialisation des composants dans les tests pour tenir compte de la nouvelle chaîne de dépendances (DataManager → Transformer → Recommender)?

Comment adapter les assertions dans les tests pour vérifier le comportement correct après les refactorisations? Quelles nouvelles vérifications devraient être ajoutées?

Comment mettre à jour run-tests.sh pour refléter les changements dans l'architecture et s'assurer que les tests s'exécutent correctement?

Comment adapter la classe MockRepository pour qu'elle fonctionne comme un mock de DataManager plutôt que comme un Repository? Quelles méthodes doivent être implémentées?

Quels nouveaux tests unitaires ajouter à TransformerSpec.scala pour vérifier les fonctionnalités consommateur qui ont été migrées depuis StreamingConsumer?

Comment adapter ou remplacer RabbitMQTestManager pour qu'il fonctionne correctement avec la nouvelle structure des échanges et des files d'attente?

Comment mettre à jour le script setup-test-queues.sh pour qu'il corresponde aux nouvelles définitions des échanges et files d'attente dans RabbitMQManager?

Le fichier Dockerfile.test doit-il être modifié pour tenir compte des changements dans la structure du projet? Si oui, comment?

