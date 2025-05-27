# Recommandations pour l'amélioration de la suite de tests

## Structure et organisation
- Adopter une structure de package identique entre main et test
  ```
  test/scala/
    application/
      core/
      services/
    domain/
      models/
    infrastructure/
      config/
      messaging/
      persistence/
    utils/
      error/
      metrics/
      retry/
  ```
- Consolider la hiérarchie des classes de base:
  ```
  BaseSpec (trait de base)
    ├── UnitSpec (tests unitaires - FlatSpec)
    ├── IntegrationSpec (tests d'intégration - WordSpec)
    └── FunctionalSpec (tests fonctionnels)
  ```
- Centraliser la configuration des tests
- Utiliser des annotations @Category plutôt que des traits
- Déplacer les utilitaires communs vers test/scala/common/utils

## Gestion des dépendances
- Remplacer PowerMock par l'injection de dépendances
- Convertir les singletons en services injectables:
  - DatabaseConfigProvider (créer interface)
  - RabbitMQManager (implémenter interface injectable)
- Unifier les implémentations de mocks:
  - Consolider MockDataManager et TestRepository
  - Créer une MockFactory centralisée

## Isolation des tests
- Implémenter TestContainerManager de manière cohérente
- Utiliser TestResourceManager.uniqueId() pour les identifiants
- Standardiser registerForCleanup via IsolatedTestBase
- Migrer vers des bases de données éphémères
- Garantir l'exécution parallèle des tests
- Convertir les scripts shell en tâches SBT dans test_build.sbt

## Couverture et qualité
- Intégrer scoverage
- Ajouter des tests:
  - Gestionnaires d'erreurs dans utils/error/
  - Tests de résilience pour RetryPolicy.scala
  - Cas d'erreur spécifiques
- Implémenter une suite de tests de performance
  - Ajouter des assertions dans StreamingTransformerIntegrationSpec
  - Définir des seuils de performance cohérents

## Outils et infrastructure
- Standardiser sur l'API Scala de TestContainers
- Intégrer dans le pipeline CI/CD
- Configurer les rapports automatisés
- Définir des seuils de couverture minimale

## Techniques spécifiques
- Convertir les utilitaires statiques en objets Scala
- Préférer les traits aux classes abstraites
- Standardiser avec TestDataFactory
- Utiliser Mockito plutôt que le mocking ad-hoc
- Implémenter des générateurs de données de test

## Refactoring et consolidation
- Supprimer le code dupliqué
- Consolider la gestion RabbitMQ via RabbitMQTestBase
- Unifier le système de tags avec TestTags.scala
- Centraliser les fixtures de test
- Nettoyer les imports inutilisés

## Documentation
- Mettre à jour le README.md avec la nouvelle structure
- Ajouter des JavaDoc aux classes de base
- Documenter l'utilisation des containers de test
- Standardiser les patterns de test



# 1. Démarrer par les tests unitaires critiques
./run-tests.sh tag:Fast

# 2. Continuer avec le reste des tests unitaires
./run-tests.sh unit:domain
./run-tests.sh unit:application

# 3. Tests par composant individuel
./run-tests.sh component:datamanager
./run-tests.sh component:transformer
./run-tests.sh component:recommender

# 3. Passer aux tests d'intégration par couche
./run-tests.sh integration:db
./run-tests.sh integration:msg
./run-tests.sh integration:txn

# 4. Terminer par les tests fonctionnels
./run-tests.sh functional