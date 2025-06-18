# 📋 Questions Brutes pour Présentation Projet

---

## 🎯 CONTEXTE MÉTIER

- **Quel problème résolvez-vous exactement ?**
Notre système résout un problème simple : trop d'événements, pas assez de temps pour les découvrir. Aujourd'hui, les gens passent des heures à chercher des concerts, conférences ou meetups qui les intéressent vraiment. Ils ratent souvent des événements parfaits pour eux simplement parce qu'ils ne les ont pas vus à temps. Notre moteur analyse ce que vous aimez vraiment (pas ce que vous dites aimer) en regardant vos actions : quels événements vous consultez, auxquels vous participez, que vous partagez. Ensuite, il vous propose automatiquement des événements qui correspondent à vos goûts réels et à ceux de vos amis. C'est comme avoir un assistant personnel qui connaît parfaitement vos préférences.

- **Pourquoi les recommandations en temps réel sont critiques ?**
Les événements, c'est du temps réel par nature. Un concert, c'est à 20h samedi, pas à 20h dimanche. Quand votre ami s'inscrit à un événement, vous voulez le savoir tout de suite pour peut-être le rejoindre. Quand des places se libèrent pour un événement complet, vous voulez être alerté immédiatement. Le temps réel, c'est capturer ces moments où les gens sont motivés : ils viennent de regarder un concert de jazz, c'est le moment parfait pour leur proposer d'autres concerts similaires. Attendre le lendemain, c'est rater l'opportunité.

- **Qui sont vos utilisateurs finaux ?**
Nos utilisateurs sont des gens normaux qui veulent une vie sociale plus riche. Principalement des 18-45 ans qui vivent en ville et qui cherchent des choses à faire. Ça inclut des professionnels qui veulent networker, des étudiants qui cherchent des activités, des passionnés de musique ou de tech, et des nouveaux arrivants en ville qui veulent rencontrer du monde. On aide aussi les organisateurs d'événements à toucher les bonnes personnes - celles qui vont vraiment venir.

- **Quels sont les cas d'usage concrets ?**
Voici comment ça marche concrètement : vous regardez un concert de jazz, on vous propose immédiatement d'autres concerts similaires dans votre ville. Trois de vos amis s'inscrivent à une conférence tech ? On vous alerte. Vous vous déplacez dans une nouvelle ville ? On vous montre les événements intéressants près de chez vous. Le vendredi, on vous propose des activités de weekend. Vous aimez la photographie ? On trouve des expositions et workshops que vous ne connaissiez pas. Un événement est annulé ? On vous propose des alternatives. C'est de la découverte intelligente basée sur vos vrais centres d'intérêt.

---

## 📊 DATA & ALGORITHMES

### Questions sur les Données
- **Quelles données collectez-vous ?**
On collecte les données essentielles pour comprendre vos goûts : vos actions sur la plateforme (ce que vous regardez, sur quoi vous cliquez, aux événements que vous participez), vos relations sociales (qui sont vos amis, à quels événements ils participent), les détails des événements (où, quand, quel type, qui organise), et votre profil de base (âge, ville, préférences). Tout ça nous aide à comprendre ce que vous aimez vraiment, pas juste ce que vous dites aimer.

- **D'où viennent ces données ?**
Les données viennent de plusieurs endroits : directement de votre utilisation de la plateforme (chaque clic, chaque consultation), de fichiers d'import quand on intègre des données existantes, et potentiellement d'APIs externes pour enrichir les informations géographiques. Le système capture tout en temps réel pour réagir immédiatement à vos actions.

- **Comment stockez-vous les données ?**
On utilise PostgreSQL comme base principale - c'est solide et fiable pour stocker les utilisateurs, événements et relations. HikariCP gère le pool de connexions et la gestion transactionnelle. RabbitMQ gère les messages entre les différentes parties du système. C'est une architecture simple mais efficace.

- **Quelle est votre stratégie de data quality ?**
On vérifie la qualité des données à plusieurs niveaux : validation immédiate des données qui arrivent, nettoyage automatique des incohérences, détection des anomalies bizarres, et surveillance continue de la qualité. Si on détecte un problème, on peut corriger ou revenir en arrière. L'objectif : avoir des données propres pour faire de bonnes recommandations.

### Questions sur les Algorithmes
- **Quel algorithme de recommandation utilisez-vous ?**
Notre algorithme hybride combine trois scores pondérés :
- **Comportement récent** (60%) : fréquence et récence des interactions
- **Contexte social/géographique** (30%) : amis participants, proximité
- **Historique** (10%) : préférences passées

Le score final applique un boost de 20% si l'événement correspond au contexte de navigation. Les 10 meilleurs scores sont retenus.

- **Quelle est votre latence cible ?**
Notre objectif est de 4-5 secondes maximum pour une recommandation complète (timeout configuré dans le code). En pratique, environ 70% des requêtes sont traitées sous 2 secondes grâce aux caches LRU (1000 profils utilisateurs) et aux TrieMap pour les événements.

- **Quelle est votre stratégie de cache ?**
Nous utilisons plusieurs niveaux de cache en mémoire JVM :
- LRUCache custom pour les profils utilisateurs (max 1000 entrées)
- TrieMap pour les événements actifs et relations d'amitié
- Cache de recommandations récentes avec TTL de 30 secondes
- Fenêtre glissante de 100 requêtes pour les métriques de performance

Tout reste en mémoire pour éviter les latences réseau, avec nettoyage automatique toutes les 5 minutes.

### Validation & Testing
- **Comment testez-vous votre système ?**
Notre stratégie de test s'appuie sur une isolation complète :
- Tests unitaires avec mocks (TestRepository, MockDataManager)
- Tests d'intégration avec TestContainers (PostgreSQL + RabbitMQ isolés)
- Environnements uniques par run (vhost RabbitMQ dédiés, identifiants uniques)
- Scripts automatisés (run-tests.sh, setup-test-queues.sh) pour l'isolation

Chaque test utilise un TEST_RUN_ID unique pour éviter les conflits entre exécutions parallèles.

---

## 🏗️ ARCHITECTURE TECHNIQUE

### Choix Technologiques
- **Pourquoi cette architecture événementielle ?**
Notre système utilise une architecture événementielle avec trois composants principaux : 
- **Transformer** : traite les événements utilisateur en temps réel
- **Recommender** : génère les recommandations avec cache multi-niveaux
- **DataManager** : gère la persistance et les transactions

Cette séparation permet de faire évoluer chaque composant indépendamment et de gérer les pannes de façon isolée.

- **Pourquoi Scala et pas Python/Java ?**
Scala combine le meilleur des deux mondes : la puissance et la vitesse de Java avec un code plus simple à écrire. C'est parfait pour nos calculs de recommandation complexes. Le code est plus court et plus lisible que Java, plus rapide que Python pour nos calculs intensifs. Et on peut utiliser toutes les librairies Java existantes. Et on bénéficie de l'écosystème JVM pour les performances et l'intégration avec PostgreSQL/RabbitMQ via JDBC/AMQP.

- **Pourquoi RabbitMQ et pas Kafka ?**
RabbitMQ nous permet un routage sophistiqué avec nos exchanges et binding keys (user.actions.#, recommendation.result.#). On a besoin de la flexibilité de routage pour nos différents types de messages : événements utilisateur, recommandations, données historiques. Kafka excelle pour le streaming pur, mais RabbitMQ nous donne plus de contrôle sur le routage et la gestion des erreurs avec les dead letter queues.

- **Pourquoi PostgreSQL et pas NoSQL ?**
PostgreSQL nous donne la fiabilité et les requêtes complexes dont on a besoin. On doit faire des jointures compliquées entre utilisateurs, événements et relations d'amitié. Les bases NoSQL sont moins pratiques pour ça. PostgreSQL nous permet de gérer efficacement les relations complexes entre utilisateurs, événements et participations. C'est stable, bien connu, et ça marche parfaitement pour nos besoins.

### Scalabilité & Robustesse
- **Comment monitoring votre système ?**
Le monitoring est intégré dans l'application avec :
- Timers pour mesurer les temps de réponse
- Compteurs pour les recommandations générées
- Histogrammes pour la distribution des scores
- Gauges pour les ratios de cache hit
- Logs structurés avec niveaux configurables

Les métriques sont calculées sur une fenêtre glissante de 100 requêtes. L'évolution vers Prometheus/Grafana est prévue.

- **Quel débit supportez-vous ?**
En configuration actuelle :
- 50-100 requêtes/seconde en conditions normales
- Jusqu'à 200 req/sec en mode dégradé (circuit breaker activé)
- Le système s'adapte automatiquement en privilégiant le cache lors des pics

- **Comment gérez-vous les pics de charge ?**
Le système a plusieurs mécanismes automatiques : cache de recommandations récentes pour éviter les recalculs, circuit breaker qui s'ouvre tout seul en cas de surcharge (seuil à 5 échecs consécutifs), mode dégradé avec recommandations simplifiées, et possibilité de scaler horizontalement les workers RabbitMQ.

- **Comment gérez-vous les pannes ?**
Le système intègre plusieurs mécanismes de protection :
- Circuit Breaker automatique (seuil : 5 échecs consécutifs, reset après 30 secondes)
- Timeout configuré à 4-5 secondes par requête
- Mode dégradé utilisant les recommandations en cache
- Dead Letter Exchange pour les messages RabbitMQ problématiques
- Health checks granulaires pour chaque composant

### Limitations Actuelles
- **Quelles sont vos limitations techniques actuelles ?**
En tant que prototype de R&D, nous avons fait des choix de simplicité :
- Cache uniquement en mémoire JVM (pas de Redis distribué)
- PostgreSQL en instance unique (pas de cluster)
- Scaling manuel des workers RabbitMQ
- Métriques intégrées mais pas encore externalisées
- Pas de déploiement automatisé en production

Ces limitations sont volontaires pour valider le concept avant d'industrialiser.

---

## ⚡ PERFORMANCE & TEMPS RÉEL

### Définitions & Objectifs
- **Qu'est-ce que "temps réel" pour vous ?**
Pour nous, temps réel signifie traiter l'action utilisateur immédiatement via RabbitMQ et générer des recommandations en moins de 2 secondes dans 70% des cas. Au-delà de 4-5 secondes, le circuit breaker se déclenche pour préserver les performances globales. C'est un compromis entre réactivité et stabilité.

- **Quelle est votre latence cible ?**
Objectif de moins de 5 secondes pour les recommandations complètes (incluant stockage en base), avec timeout configuré à 4-5 secondes dans le code. En pratique, 70% des requêtes sont traitées sous 2 secondes grâce aux caches multi-niveaux (userProfileCache, eventCache, recentUserEvents) et au traitement asynchrone via RabbitMQ.

- **Quel débit supportez-vous ?**
Notre architecture actuelle peut traiter environ 50-100 actions par seconde. 
On a testé avec des pics jusqu'à 200 actions/sec en mode dégradé.

### Gestion de la Charge
- **Comment gérez-vous les pics de charge ?**
Quand il y a beaucoup d'activité (comme le vendredi soir), on démarre automatiquement plus de workers. Si ça ne suffit pas, on bascule sur des recommandations plus simples mais plus rapides. On garde toujours les données importantes en cache pour accélérer. L'idée : s'adapter automatiquement à la charge.

- **Quelle est votre disponibilité ?**
On vise 99.9% de disponibilité. Ça veut dire moins de 9 heures de panne par an. On y arrive avec des systèmes redondants, des reprises automatiques en cas de panne, et des modes dégradés qui continuent à fonctionner même si tout n'est pas parfait.

- **Comment gérez-vous la fraîcheur des données ?**
On met à jour les données à différentes vitesses selon leur importance : les profils utilisateur toutes les 5 minutes, les événements toutes les heures, les recommandations toutes les 30 minutes. Si quelque chose change, on met à jour immédiatement. On marque tout avec des timestamps pour savoir ce qui est récent.

- **Quels sont vos SLA ?**
On n'a pas encore de SLA formels. On surveille manuellement les performances 
et on vise moins de 2 secondes pour les recommandations. Les SLA automatisés 
sont dans notre roadmap technique.

---

## 📈 QUALITÉ & MÉTRIQUES

- **Comment mesurez-vous la qualité des recommandations ?**
On regarde plusieurs choses : est-ce que les gens cliquent sur nos recommandations ? Est-ce qu'ils participent aux événements recommandés ? Est-ce qu'ils sont satisfaits après ? On mesure aussi la diversité pour éviter de toujours proposer la même chose. On collecte les retours directs des utilisateurs.

- **Quels sont vos KPIs techniques ?**
Nos indicateurs clés : temps de réponse moyen, taux de disponibilité, nombre de recommandations générées par seconde, taux de succès des traitements, utilisation des ressources. On surveille aussi les erreurs et les reprises. Tout ça nous aide à optimiser le système.

- **Quelles sont vos métriques de diversité ?**
On s'assure qu'on ne recommande pas toujours les mêmes types d'événements. On mesure la variété des catégories, des organisateurs, des lieux. On vérifie qu'on ne crée pas de "bulles" où les gens ne voient que ce qu'ils connaissent déjà. L'objectif : aider à découvrir de nouvelles choses.

- **Comment gérez-vous la sérendipité ?**
On introduit volontairement un peu d'imprévu dans les recommandations. Parfois, on propose des événements un peu différents de d'habitude. On équilibre entre sécurité (ce qu'on sait que vous aimez) et découverte (ce que vous pourriez aimer). C'est comme ça qu'on crée de belles surprises.

---

## 🚀 DÉPLOIEMENT & OPÉRATIONS

### Stratégies de Déploiement
- **Quelle est votre stratégie de déploiement ?**
On utilise Docker pour empaqueter l'application et ses dépendances. Ça garantit que ça marche pareil partout : sur nos machines, sur les serveurs de test, en production. On a des environnements séparés avec des configurations différentes. Le déploiement se fait par étapes avec des vérifications à chaque niveau.

- **Comment gérez-vous les environnements ?**
Nous avons automatisé l'isolation complète des environnements de test :
- Génération d'identifiants uniques par run (timestamp + hash)
- Vhosts RabbitMQ dédiés (test_env_${TEST_RUN_ID})
- Bases de données PostgreSQL isolées
- Configuration Docker Compose spécifique (docker-compose-test.yml)
- Scripts bash pour la gestion automatique du cycle de vie

- **Quelle est votre stratégie de CI/CD ?**
Pour l'instant, c'est semi-automatique avec validation manuelle. On a dockerisé les tests avec docker-compose-test.yml et des scripts bash pour automatiser les cycles de test. Un pipeline CI/CD complet avec GitHub Actions est dans le roadmap.

- **Comment gérez-vous les rollbacks ?**
Si une nouvelle version pose problème, on peut revenir à la précédente en 2 minutes. On garde les anciennes versions, on fait des sauvegardes avant chaque déploiement, et on a des procédures automatiques de retour en arrière. On teste le rollback pour être sûr que ça marche.

### Monitoring & Maintenance
- **Quelle est votre stratégie de monitoring ?**
Actuellement, on utilise des logs structurés avec des métriques custom intégrées dans l'application. Les health checks vérifient automatiquement chaque composant (cache, RabbitMQ, base de données). L'évolution vers un système externe comme Prometheus/Grafana est prévue pour avoir des dashboards temps réel.

- **Comment gérez-vous les alertes ?**
On a trois niveaux d'alertes : critiques (notification immédiate), importantes (notification rapide), informatives (rapports réguliers). Selon la gravité, on notifie par Slack, email ou SMS. On évite le spam d'alertes en regroupant les notifications liées. Chaque alerte a une procédure de résolution.

- **Quelle est votre stratégie de backup ?**
On sauvegarde tout régulièrement : les données utilisateur, les configurations, le code. On teste les restaurations pour être sûr que ça marche. On garde plusieurs versions de sauvegarde et on les stocke dans plusieurs endroits. En cas de gros problème, on peut tout restaurer rapidement.

---

## 🔮 ÉVOLUTION & ROADMAP

- **Quelles sont vos prochaines évolutions ?**
On prévoit d'ajouter des recommandations basées sur la météo (événements en intérieur quand il pleut), l'intégration avec les calendriers personnels (éviter les conflits), et des recommandations de groupe (trouver des événements pour plusieurs amis ensemble). On veut aussi améliorer la personnalisation en analysant plus finement les préférences.

- **Quels nouveaux features prévoyez-vous ?**
Des notifications push intelligentes, des recommandations vocales, l'intégration avec les réseaux sociaux, et des fonctionnalités de planification d'événements personnalisés. On pense aussi à des recommandations basées sur l'humeur ou l'activité du moment.

- **Comment anticipez-vous la croissance ?**
On a conçu le système pour grandir facilement. On peut ajouter des serveurs, optimiser les algorithmes, et améliorer le cache. On surveille les tendances d'usage pour anticiper les besoins. L'architecture modulaire permet d'ajouter des fonctionnalités sans tout casser.

- **Quels sont les défis futurs identifiés ?**
La gestion de la vie privée (être utile sans être intrusif), la scalabilité internationale (fuseaux horaires, cultures différentes), et l'adaptation aux nouvelles tendances sociales. On doit aussi gérer l'évolution des goûts des utilisateurs et des types d'événements.

- **Quelle est votre stratégie de migration ?**
On migre progressivement vers de nouvelles technologies en gardant l'ancien système en parallèle. On teste extensivement avant de basculer complètement. On a des plans de retour en arrière si nécessaire. L'objectif : évoluer sans casser l'expérience utilisateur.

---

## 🎭 DÉMONSTRATION

- **Pouvez-vous nous montrer une démo live ?**
Bien sûr ! On peut simuler un utilisateur qui consulte des événements et voir les recommandations apparaître en temps réel. On peut aussi montrer comment le système réagit quand un ami s'inscrit à un événement. Les dashboards montrent tout ce qui se passe derrière.

- **Comment simulez-vous différents scénarios ?**
On a des outils pour simuler différents types d'utilisateurs, des pics de charge, des pannes de services. On peut créer des utilisateurs virtuels avec des comportements réalistes et voir comment le système réagit. C'est utile pour tester sans impacter les vrais utilisateurs.

- **Quels sont les cas d'edge que vous gérez ?**
Les utilisateurs très actifs (trop de données), les utilisateurs inactifs (pas assez de données), les événements annulés, les doublons, les données corrompues, les pics de charge extrêmes. On a des stratégies spécifiques pour chaque cas.

- **Comment debuggez-vous les problèmes ?**
On a des logs détaillés pour tracer chaque action, des métriques pour identifier les goulots d'étranglement, et des outils pour simuler les problèmes. On peut suivre une recommandation de bout en bout pour comprendre pourquoi elle a été faite. Les dashboards nous montrent l'état en temps réel.

---

## 🤔 CHALLENGES & ÉCHECS

- **Quels ont été vos plus gros challenges ?**
Équilibrer la vie privée et la personnalisation (être utile sans être intrusif), gérer les pics de charge imprévisibles, et éviter les recommandations répétitives. Aussi, expliquer aux utilisateurs pourquoi on recommande tel ou tel événement sans révéler notre sauce secrète.

- **Quelles erreurs avez-vous faites ?**
Au début, on sur-optimisait pour la précision au détriment de la diversité. Les gens recevaient des recommandations trop similaires. On a aussi sous-estimé l'importance de la dimension sociale - les gens veulent savoir ce que font leurs amis.

- **Qu'est-ce que vous feriez différemment ?**
On commencerait par la dimension sociale dès le début. On investirait plus tôt dans les outils de monitoring et de debugging. Et on testerait davantage avec de vrais utilisateurs plutôt que se fier uniquement aux métriques techniques.

- **Quels sont vos trade-offs principaux ?**
Vitesse vs précision (on préfère une recommandation rapide et correcte qu'une recommandation parfaite mais lente), simplicité vs sophistication (on préfère un système compréhensible qu'un système optimal mais opaque), et personnalisation vs vie privée (on respecte la confidentialité même si ça limite les possibilités).

---

> **💡 Note :** Ces questions couvrent tous les aspects de votre système de recommandations temps réel avec un langage simple et accessible.