# Comment nous testons notre système de recommandation en temps réel

## Introduction

Dans le cadre du développement de notre système de recommandation en temps réel, nous avons adopté une approche rigoureuse pour concevoir et exécuter des tests d'intégration et des tests de bout en bout (E2E). Notre architecture repose sur un pipeline événementiel, où chaque composant joue un rôle spécifique dans le traitement des données utilisateur, la génération de recommandations et leur distribution. Tester un tel système nécessite une stratégie adaptée pour garantir la fiabilité, la performance et la précision des recommandations dans des scénarios réalistes.

Les tests d'intégration permettent de valider le bon fonctionnement des interactions entre les différents composants du pipeline, tandis que les tests E2E vérifient le comportement global du système dans des conditions proches de la production. Dans cette documentation, nous détaillons les méthodologies, outils et stratégies que nous avons mis en place pour garantir la qualité de notre système.

## Architecture et contexte des tests

Notre système est basé sur une architecture événementielle utilisant **RabbitMQ** comme middleware de messagerie et **PostgreSQL** comme base de données principale. Les événements utilisateur (*UserEvent*) sont capturés en temps réel, transformés en événements enrichis (*ProcessedEvent*) par un composant appelé **Transformer**, puis transmis au moteur de recommandation (**Recommender**) pour générer des suggestions personnalisées. Ces recommandations sont ensuite publiées dans une file d'attente dédiée et consommées par des systèmes externes.

Cette architecture en pipeline impose des défis spécifiques pour les tests :

* **Asynchronisme** : Les messages transitent entre les composants via RabbitMQ, ce qui nécessite de synchroniser les tests avec les événements.
* **Dépendances externes** : Les tests doivent inclure des services comme RabbitMQ et PostgreSQL, tout en garantissant leur isolation.
* **Scénarios complexes** : Les tests doivent couvrir des cas comme le démarrage à froid, les changements de comportement utilisateur et les pics de charge.

## Tests d'intégration : méthodologie et outils

### Objectif des tests d'intégration

Les tests d'intégration visent à valider les interactions entre deux ou plusieurs composants critiques du pipeline. Voici les aspects supplémentaires que nous avons testés :

* **Validation des schémas de messages RabbitMQ** : Nous avons vérifié que les messages publiés dans les files d'attente respectent les schémas JSON définis, incluant les champs obligatoires (userId, eventType, itemId, etc.) et les formats attendus.
* **Gestion des erreurs dans RabbitMQ** : Nous avons simulé des scénarios où des messages mal formés ou invalides sont publiés dans les files d'attente. Le système doit ignorer ces messages ou les rediriger vers une file d'attente dédiée aux erreurs (dead-letter queue).
* **Durabilité des files d'attente** : Nous avons testé que les files d'attente RabbitMQ sont configurées avec l'option `durable=true` pour garantir la persistance des messages en cas de redémarrage du service.

### Environnement de test

Nous avons enrichi l'environnement de test avec des configurations spécifiques pour simuler des scénarios réalistes :

* **Snapshots de base de données PostgreSQL** : Avant chaque test, nous restaurons un snapshot contenant des données utilisateur et des événements historiques pour simuler un environnement proche de la production.

### Stratégies de test

#### Tests de cohérence des données :

Après chaque transformation, nous avons vérifié que les données enrichies (*ProcessedEvent*) sont cohérentes avec les données brutes (*UserEvent*), en particulier pour les champs calculés comme les scores ou les métadonnées.

#### Tests de résilience :

Nous avons simulé des pannes de RabbitMQ ou PostgreSQL pendant l'exécution des tests pour vérifier que le système peut se reconnecter automatiquement et reprendre le traitement des messages sans perte.

### Outils utilisés

En complément des outils déjà mentionnés :

* **RabbitMQ Management Plugin** : Utilisé pour surveiller les files d'attente, les taux de publication/consommation et les messages non consommés.
* **PostgreSQL Query Logs** : Activé pour analyser les requêtes exécutées pendant les tests et identifier les éventuels problèmes de performance ou de verrouillage.

### Résultats attendus

Pour chaque test, nous avons validé :

1. La conformité des messages publiés avec les spécifications du système.
2. La capacité du système à gérer des volumes élevés de messages sans dégradation des performances.
3. La robustesse face aux erreurs et aux pannes, avec une reprise automatique du traitement.

Ces tests approfondis garantissent que notre pipeline événementiel fonctionne de manière fiable et efficace dans des conditions variées.

## Tests de bout en bout (E2E) : méthodologie et scénarios

### Objectif des tests E2E

Les tests E2E valident le fonctionnement global du système, depuis la capture d'un événement utilisateur jusqu'à la publication des recommandations. Voici des aspects supplémentaires que nous avons testés :

* **Validation des recommandations contextuelles** : Nous avons simulé des utilisateurs sans historique pour vérifier que le système génère des recommandations basées uniquement sur des signaux contextuels, comme l'heure, la localisation ou la popularité des items.
* **Tests de cohérence inter-composants** : Nous avons vérifié que les données transmises entre les composants (Transformer, Recommender, RabbitMQ) restent cohérentes tout au long du pipeline, même en cas de forte charge.
* **Tests de persistance des recommandations** : Nous avons validé que les recommandations générées sont correctement enregistrées dans la base de données PostgreSQL et qu'elles peuvent être récupérées ultérieurement via des requêtes API.

### Scénarios de test supplémentaires

#### Recommandations multi-utilisateurs :

Nous avons simulé plusieurs utilisateurs interagissant simultanément avec le système pour vérifier que les recommandations générées sont spécifiques à chaque utilisateur et qu'il n'y a pas de mélange de données.

#### Tests de compatibilité des formats de données :

Nous avons testé différents formats de messages JSON (avec des champs optionnels ou des structures imbriquées) pour garantir que le système peut traiter correctement les variations dans les données d'entrée.

#### Tests de reprise après panne :

Nous avons simulé des interruptions de RabbitMQ ou PostgreSQL pendant l'exécution des tests pour vérifier que le système peut reprendre le traitement des messages en attente une fois les services restaurés.

### Validation des résultats

Pour chaque scénario, nous avons vérifié :

* **Exactitude des recommandations** : Les items recommandés doivent être pertinents par rapport aux interactions utilisateur ou aux signaux contextuels.
* **Performance globale** : Le temps total de traitement d'un événement utilisateur jusqu'à la génération de recommandations doit rester dans les limites définies (par exemple, moins de 500 ms).
* **Absence de perte de données** : Tous les événements utilisateur publiés doivent être traités, même en cas de panne temporaire d'un composant.

### Outils et configurations spécifiques

* **RabbitMQ Management Plugin** : Utilisé pour surveiller les files d'attente et vérifier que les messages sont consommés correctement.
* **PostgreSQL Query Logs** : Activé pour analyser les requêtes exécutées pendant les tests et identifier les éventuels problèmes de performance.
* **Snapshots de base de données** : Restaurés avant chaque test pour garantir un état initial cohérent et reproductible.

Ces tests E2E approfondis nous permettent de valider que notre système de recommandation fonctionne de manière fiable et efficace dans des scénarios réalistes et complexes.

## Défis rencontrés et solutions

* **Synchronisation des tests avec RabbitMQ** : Les messages étant traités de manière asynchrone, nous avons introduit des mécanismes de polling pour attendre les résultats dans les files d'attente.
* **Isolation des tests** : Chaque test utilise une base de données PostgreSQL distincte et des files d'attente RabbitMQ uniques pour éviter les interférences.
* **Performance des tests** : Pour réduire le temps d'exécution, nous utilisons des snapshots de base de données et des conteneurs préconfigurés.

## Améliorations futures

Pour rendre notre système plus robuste, nous prévoyons d'ajouter les tests suivants :

1. **Ordonnancement des messages** : Vérifier que les messages sont consommés dans l'ordre où ils ont été publiés, en particulier pour les événements utilisateur liés à une même session.
2. **Tests multi-consommateurs RabbitMQ** : Configurer plusieurs consommateurs pour une même file d'attente afin de tester la répartition des messages (round-robin) et la gestion des conflits d'accès.
3. **Tests de latence des messages** : Mesurer le temps écoulé entre la publication d'un message dans user_events et sa consommation par le Transformer, pour garantir que le traitement reste en temps réel.
4. **Tests de montée en charge** : Simuler un grand volume d'événements utilisateur (plusieurs milliers par seconde) pour vérifier que le Transformer et le Recommender peuvent traiter les messages sans perte ni ralentissement significatif.
5. **Tests de déduplication des événements** : Publier des événements utilisateur en double dans la file user_events pour vérifier que le système détecte et ignore les doublons, évitant ainsi des recommandations redondantes.
6. **Gestion des délais d'attente** : Introduire des délais artificiels dans le traitement des messages pour vérifier que le système respecte les timeouts configurés et ne bloque pas les autres processus.

## Conclusion

Les tests d'intégration et E2E jouent un rôle crucial dans la validation de notre système de recommandation en temps réel. En combinant des outils comme TestContainers, ScalaTest et Docker Compose, nous avons construit une infrastructure de test robuste et reproductible. Cette approche nous permet de garantir la qualité de notre système tout en identifiant rapidement les régressions et les problèmes de performance. Grâce à ces tests, nous pouvons déployer notre système avec confiance, en sachant qu'il répondra aux attentes des utilisateurs dans des scénarios réels.
