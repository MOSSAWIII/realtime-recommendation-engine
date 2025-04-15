# Architecture des repositories

## Approches utilisées

Notre système utilise deux approches différentes pour l'accès aux données:

### 1. JDBC direct (Repository.scala)

Le `Repository.scala` utilise JDBC direct avec la méthode `withConnection` pour gérer:
- Les événements traités (processed_events)
- Les recommandations (recommendations)

Cette approche a été choisie pour ces tables car:
- Elles sont optimisées pour les opérations à haute fréquence
- Elles ont besoin d'un contrôle précis des transactions
- Elles utilisent des structures JSON qui nécessitent un traitement personnalisé

### 2. Slick ORM (UserRepository, EventRepository, etc.)

Les autres repositories utilisent Slick (ORM) pour:
- Les tables de référence (users, events, participations, etc.)
- Les opérations moins fréquentes et plus standard

Cette approche a été choisie pour ces tables car:
- Elles bénéficient de l'abstraction fournie par l'ORM
- Elles ont des structures simples et relationnelles
- Elles sont moins sollicitées en termes de fréquence d'accès

## Justification technique

Les deux approches coexistent pour offrir le meilleur des deux mondes:
- Performance et contrôle fin pour les opérations à haute fréquence
- Facilité de développement et abstraction pour les opérations standard

Cette architecture hybride est délibérée et nous permet d'optimiser chaque partie du système
selon ses besoins spécifiques.