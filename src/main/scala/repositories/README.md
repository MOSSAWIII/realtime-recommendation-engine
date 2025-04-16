# Architecture des repositories

## Approches utilisées

Notre système utilise JDBC direct pour toutes les opérations d'accès aux données, pour des raisons de performance et de contrôle.

### Architecture à deux niveaux

1. **JDBC direct avec exécution optimisée**
   - Tous les repositories utilisent JDBC avec des prepared statements
   - Chaque repository gère ses propres connexions via le pool centralisé

2. **Séparation par domaine**
   - `Repository` : Gère les tables d'événements transformés et recommandations à haute fréquence
   - Repositories spécifiques : `UserRepository`, `EventRepository`, etc. pour les données de référence

### Justification technique

Cette architecture a été choisie pour:
- Maximiser les performances sur l'ensemble du système
- Garder un contrôle précis des requêtes SQL
- Permettre l'optimisation spécifique par type de données
- Rester cohérent dans tout le codebase avec une seule approche

## Gestion des connexions

- Toutes les connexions sont obtenues via `DatabaseConfig.getConnection()`
- Les connexions sont automatiquement retournées au pool après utilisation
- Pattern `withConnection`, `withPreparedStatement` et `withResultSet` pour la gestion des ressources