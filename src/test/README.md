# Tests du système de recommandation en temps réel

Ce dossier contient les tests du système organisés en différentes catégories:

## Organisation des tests

- **unit/**: Tests unitaires isolés des dépendances externes
  - **domain/**: Tests des modèles de domaine
  - **application/**: Tests de la logique métier
  - **infrastructure/**: Tests des composants d'infrastructure

- **integration/**: Tests d'intégration entre composants
  - **db/**: Tests d'intégration avec la base de données
  - **messaging/**: Tests d'intégration avec le système de messaging
  - **api/**: Tests d'intégration avec l'API

- **functional/**: Tests fonctionnels de bout en bout

- **common/**: Code partagé entre les tests
  - **fixtures/**: Données de test partagées
  - **mocks/**: Implémentations simulées des dépendances
  - **utils/**: Utilitaires pour les tests

## Exécution des tests

- Tests unitaires: `sbt "testOnly unit.*"`
- Tests d'intégration: `sbt "testOnly integration.*"`
- Tests fonctionnels: `sbt "testOnly functional.*"`
- Tous les tests: `sbt test`
