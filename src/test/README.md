# Tests du système de recommandation en temps réel

## Structure des tests

### Organisation
- **unit/**: Tests unitaires isolés des dépendances externes
- **integration/**: Tests d'intégration entre composants
- **functional/**: Tests fonctionnels de bout en bout

### Classes de base
Tous les tests utilisent une hiérarchie de traits unifiée:
- `TestBase`: Trait de base pour tous les tests
- `UnitSpec`: Pour les tests unitaires (extends AnyFlatSpec)
- `UnitWordSpec`: Pour les tests unitaires style WordSpec
- `IntegrationSpec`: Pour les tests d'intégration
- `FunctionalSpec`: Pour les tests fonctionnels

### Utilitaires communs
- `MockManager`: Gestionnaire centralisé pour tous les mocks
- `TestDataFactory`: Factory pour générer des données de test
- `UnifiedMockDataManager`: Mock du DataManager pour les tests
- `IntegrationTestManager`: Gestion des containers pour les tests d'intégration

## Exécution des tests
- Tests unitaires: `sbt "testOnly unit.*"`
- Tests d'intégration: `sbt "testOnly integration.*"`
- Tests fonctionnels: `sbt "testOnly functional.*"`
- Tous les tests: `sbt test`
