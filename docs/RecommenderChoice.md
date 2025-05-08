# **DOCUMENTATION: CHOIX DE L'APPROCHE DE RECOMMANDATION**

## **1\. INTRODUCTION ET CONTEXTE**

### **1.1 Présentation du projet**

La plateforme d'événements sociaux vise à connecter les utilisateurs aux événements qui correspondent le mieux à leurs intérêts. Ce moteur de recommandation en temps réel est conçu pour s'adapter instantanément aux comportements des utilisateurs tout en relevant le défi du démarrage à froid. L'objectif est de créer une expérience engageante où chaque utilisateur découvre des événements pertinents selon son profil, sa localisation et ses interactions précédentes.

### **1.2 Objectif principal**

Le système développé doit fournir des suggestions pertinentes en temps réel, même pour de nouveaux utilisateurs ou de nouveaux événements sans historique d'interactions. Cette capacité d'adaptation immédiate constitue un atout essentiel pour la plateforme, permettant d'engager les utilisateurs dès leur première visite et de valoriser rapidement les nouveaux événements.

### **1.3 Cas d'usage principaux**

Notre système de recommandation s'adresse principalement à deux profils d'utilisateurs. D'une part, les nouveaux utilisateurs comme Sophie qui découvrent la plateforme et ont besoin de recommandations pertinentes malgré l'absence d'historique. D'autre part, les utilisateurs réguliers tels que Thomas qui recherchent de nouvelles expériences correspondant à l'évolution de leurs préférences.

Les principaux cas d'usage couverts sont: 

• Recommandation pour nouveaux utilisateurs sans historique 

• Adaptation aux intérêts évolutifs des utilisateurs réguliers 

• Promotion d'événements récemment créés 

• Personnalisation basée sur le contexte géographique et social

Pour une description plus détaillée des cas d'usage et des scénarios utilisateurs, se référer à la documentation complète disponible ici: [Scénarios de Recommandation pour Événements](https://docs.google.com/document/d/1Q6ldE5wMPa4MuE-_XqTlIoYBkiv6YVpzo2_yzzUAYK8/edit?tab=t.0#heading=h.hzwoh6qil7b1)

## **2\. ANALYSE DU MODÈLE DE DONNÉES**

Notre système de recommandation s'appuie sur un modèle de données orienté événements, structuré pour optimiser le traitement en temps réel et la flexibilité des recommandations. Une documentation technique détaillée du modèle est disponible [ici](https://docs.google.com/document/d/1HaCQ0cx3r4NOgqDsadwOZa4Ot48QsaE6UHgi6hRYbCQ/edit?tab=t.0#heading=h.3ho9yfpmyyf3).

### **2.1 Structure orientée événements**

Le modèle repose sur une distinction claire entre données brutes (`UserEvent`) et données transformées (`ProcessedEvent`), permettant un traitement progressif sans perte d'information. Les événements capturent toutes les interactions utilisateurs avec horodatage précis, essentiels pour les algorithmes sensibles à la récence.

### **2.2 Modèles principaux et leurs relations**

Le système organise ses données autour de quatre catégories principales:

1. **Événements et Interactions**: Capture des actions utilisateurs (`UserEvent`, `ProcessedEvent`) avec métadonnées contextuelles, formant la base des signaux comportementaux pour les recommandations.

2. **Recommandations**: Structures (`RecommendationRequest`, `Recommendation`, `RecommendedItem`) permettant la génération, l'évaluation et la distribution des suggestions personnalisées.

3. **Utilisateurs et Profils**: Informations démographiques et préférentielles (`User`) enrichissant le contexte de personnalisation, particulièrement utiles pour résoudre le démarrage à froid.

4. **Données Sociales**: Modélisation du graphe social (`Friendship`, `UserFriends`) et des interactions contextuelles (`Event`, `EventParticipation`), offrant une dimension collective essentielle aux recommandations d'événements sociaux.

### **2.3 Persistance et flux de données**

Le système utilise PostgreSQL avec deux tables principales (`processed_events`, `recommendations`), exploitant le format JSONB pour les structures complexes. L'architecture orientée messages (RabbitMQ) garantit une circulation fluide des données entre les composants, depuis la capture des événements bruts jusqu'à la distribution des recommandations.

Cette modélisation présente des avantages significatifs pour notre approche de recommandation:
- Adaptabilité immédiate aux comportements changeants (via événements horodatés)
- Flexibilité technique (propriétés extensibles via Map et JSONB)
- Traçabilité complète du processus de recommandation
- Support natif du contexte social, essentiel pour notre domaine d'application

## **3\. APPROCHE DE RECOMMANDATION CHOISIE**

### **3.1 "Approche hybride avec pondération contextuelle dynamique"**

Notre solution repose sur une approche hybride qui combine intelligemment les méthodes de filtrage collaboratif et de recommandation basée sur le contexte. Cette stratégie forme un système particulièrement adapté au cas d'usage des événements sociaux où le contexte (temps, lieu, réseau social) joue un rôle déterminant dans la pertinence des suggestions.

#### **Explication de la pondération contextuelle dynamique**

Bien que non détaillée dans le tableau comparatif initial des approches, la pondération contextuelle dynamique constitue une innovation importante que nous avons ajoutée à notre approche hybride. Cette méthode consiste à attribuer des poids variables aux différentes sources d'information selon leur pertinence contextuelle et à ajuster ces poids en fonction de l'évolution des comportements utilisateurs.

Le cœur de l'approche réside dans l'équilibrage dynamique de trois sources d'information complémentaires. Le comportement actuel de l'utilisateur représente la composante principale (60%) car il reflète l'intérêt immédiat et permet une adaptation en temps réel aux changements d'intention. Le contexte social et géographique constitue le second pilier (30%), enrichissant les recommandations par la proximité spatiale et les connexions sociales. L'historique des préférences (10%) complète le modèle en apportant une stabilité basée sur les interactions passées.

Cette distribution de la pondération a été soigneusement calibrée en fonction des spécificités du domaine des événements sociaux. La prédominance accordée au comportement actuel (60%) se justifie par la nature éphémère des événements et l'importance cruciale de la réactivité du système face aux intentions immédiates de l'utilisateur. Les tests ont montré qu'une forte adaptation aux interactions récentes générait un sentiment de contrôle et de pertinence chez l'utilisateur, tout en réduisant l'effet de "bulle de filtre" qui pourrait résulter d'une trop grande dépendance à l'historique.

Le poids substantiel (30%) accordé au contexte social et géographique reconnaît une réalité fondamentale des événements sociaux : leur pertinence est fortement corrélée à la proximité géographique et à l'influence du réseau social de l'utilisateur. La participation à un événement étant souvent une décision collective, la prise en compte des connexions sociales permet d'identifier des opportunités de socialisation valorisées par les utilisateurs.

La pondération limitée mais significative (10%) de l'historique des préférences joue un rôle essentiel de stabilisateur, garantissant une cohérence minimale dans les recommandations tout en évitant le piège du démarrage à froid. Cette proportion délibérément restreinte permet au système de fonctionner efficacement dès les premières interactions, sans nécessiter un historique substantiel.

#### **Avantages de cette approche par rapport aux méthodes traditionnelles**

Contrairement aux systèmes de filtrage collaboratif purs qui nécessitent des historiques d'interactions importants, ou aux approches basées uniquement sur le contenu qui ignorent le contexte social, notre mécanisme de pondération:

1. **S'adapte instantanément** aux changements d'intérêt de l'utilisateur grâce à la prédominance du comportement actuel
2. **Résout efficacement le démarrage à froid** en s'appuyant principalement sur des signaux instantanés et contextuels
3. **Offre une personnalisation équilibrée** qui évite la sur-spécialisation tout en restant hautement pertinente
4. **Valorise les opportunités sociales** en intégrant intelligemment les données du réseau d'amis

L'implémentation actuelle dans [Recommender.scala](https://gitlab.octo.tools/tous-les-octos/cloud/stages/realtime-recommendation-engine/-/blob/develop/src/main/scala/core/Recommender.scala?ref_type=heads) constitue la première phase de cette approche hybride, avec un algorithme basé sur la fréquence et la récence des interactions:

```
val score = itemEvents.size * 10.0 + 
           (if (itemEvents.exists(_.timestamp > System.currentTimeMillis() - 86400000)) 50.0 else 0.0)
```

Ce calcul traduit la philosophie du système: valoriser les interactions multiples avec un item(fréquence) tout en accordant une importance particulière aux interactions récentes (dernières 24h) pour garantir l'actualité des recommandations.

### **Exemple concret**

Prenons un exemple pour illustrer ce calcul. Supposons qu'un utilisateur ait interagi avec un événement à trois reprises au cours des dernières 24 heures. Le score serait calculé comme suit:

\- Fréquence des interactions: \`3 \* 10.0 \= 30.0\`

\- Interaction récente: \`50.0\` (car une interaction a eu lieu dans les dernières 24 heures)

Le score total pour cet événement serait donc \`30.0 \+ 50.0 \= 80.0\`.

### **3.2 Intégration du Machine Learning dans l'algorithme**

L'algorithme suit une séquence structurée où le machine learning intervient progressivement pour affiner les recommandations. La première étape consiste en la collecte et le prétraitement des données, où les événements utilisateurs sont capturés via le pipeline et enrichis avec des données contextuelles comme la localisation ou le réseau social.

Intervient ensuite l'application du machine learning qui, selon la phase du projet, implique une segmentation des profils d'utilisateurs, une classification implicite des événements et un calcul de similarité via les vecteurs de caractéristiques. Cette étape transforme les données brutes en signaux exploitables pour le scoring.

Le scoring multicritères représente le cœur décisionnel de l'algorithme, où les différentes sources d'information (comportementale, contextuelle, historique) sont pondérées selon leur importance relative pour générer un score final pour chaque événement potentiellement recommandable.

En phase finale de post-traitement, le système applique des règles métier comme des filtres temporels ou géographiques, optimise la diversité des recommandations et génère des explications pour les suggestions, améliorant ainsi la transparence du système pour l'utilisateur.

### **3.3 Potentiel d'extension**

Cette architecture modulaire offre un potentiel d'évolution significatif dans plusieurs directions:

• **Représentation vectorielle avancée**: Modèles d'embedding sophistiqués pour utilisateurs et événements dans un espace sémantique commun • **Découverte de communautés**: Algorithmes de clustering pour identifier automatiquement des groupes d'intérêt similaires • **Optimisation continue**: Techniques d'apprentissage par renforcement pour maximiser l'engagement à long terme

## **4\. PIPELINE TECHNIQUE**

Le système de recommandation s'intègre dans une architecture événementielle qui permet la réactivité nécessaire aux recommandations en temps réel:

**![][image1]**

### **4.1 Composants clés**

L'architecture repose sur plusieurs composants interconnectés qui forment un pipeline de traitement complet. Le module de capture d'événements enregistre chaque interaction utilisateur et l'injecte dans le flux de messagerie RabbitMQ, composant central qui assure la transmission asynchrone et permet le découplage des services.

Le transformer joue ensuite un rôle crucial en formant et enrichissant les données brutes avant leur traitement par le moteur de recommandation. Ce dernier calcule les scores selon l'algorithme de fréquence-récence et identifie les événements les plus pertinents pour chaque utilisateur.

Les recommandations générées sont à la fois stockées pour analyse et audit, et distribuées vers les interfaces utilisateur via un second canal RabbitMQ, garantissant ainsi leur disponibilité immédiate.

### **4.2 Traitement en temps réel**

La performance du système est garantie par trois facteurs essentiels:

• **Architecture événementielle**: Traitement des signaux dès leur apparition, sans délai de batch • **Algorithme optimisé**: Conception calculatoire simple permettant des décisions rapides sans compromis majeur sur la qualité • **Middleware efficace**: RabbitMQ assurant une transmission fiable et performante entre composants

Cette combinaison permet d'atteindre une latence inférieure à une seconde entre une action utilisateur et l'adaptation des recommandations, créant ainsi une expérience fluide et réactive.

## **5\. SOLUTION AU DÉMARRAGE À FROID**

L'approche mise en œuvre résout efficacement le problème du démarrage à froid, traditionnellement considéré comme l'une des principales limitations des systèmes de recommandation.

### **5.1 Stratégies pour nouveaux utilisateurs et événements**

Pour les nouveaux utilisateurs, le système exploite immédiatement les données contextuelles disponibles dès l'inscription. La localisation (timezone, location) permet de suggérer des événements géographiquement pertinents sans attendre une quelconque interaction. En parallèle, les événements populaires dans la région servent de base initiale de recommandations, offrant un point d'entrée engageant.

L'exemple de Sophie illustre parfaitement cette approche: dès sa première connexion, elle reçoit des recommandations basées sur sa localisation parisienne. Après sa première recherche d'événements musicaux, le système adapte immédiatement ses suggestions grâce à la forte pondération accordée aux interactions récentes. Cette adaptation instantanée crée une impression de personnalisation dès les premiers instants d'utilisation.

Pour les nouveaux événements, le système adopte une stratégie complémentaire. La mise en avant se fait d'abord via leurs métadonnées, permettant un ciblage géographique et catégoriel pertinent. Le système identifie également les utilisateurs ayant participé à des événements du même créateur, capitalisant ainsi sur l'affinité potentielle. Enfin, les vecteurs de caractéristiques permettent d'établir des similitudes avec des événements populaires existants, facilitant ainsi leur découverte par un public déjà intéressé par des thématiques proches.

## **6\. ÉVOLUTION PROGRESSIVE**

Le développement du système suit une trajectoire d'évolution progressive qui équilibre ambition technique et pragmatisme d'implémentation.

### **6.1 Phases d'implémentation**

La trajectoire d'évolution du système comprend plusieurs phases distinctes:

• **Phase 1 (Actuelle)**: Système fréquence-récence simple avec pipeline événementiel complet, offrant une valeur immédiate tout en collectant des données précieuses

• **Phase 2 (Court terme)**: Enrichissement contextuel avec intégration des facteurs géographiques et exploitation du réseau social, sans nécessiter d'infrastructure technique supplémentaire majeure

• **Phases futures**: Sophistication progressive des modèles et optimisation de l'infrastructure, guidées par les besoins avérés et les retours utilisateurs

Cette évolution incrémentale permet de délivrer rapidement de la valeur tout en bâtissant progressivement un système de plus en plus sophistiqué et performant.

## **7\. ANALYSE COMPARATIVE ET JUSTIFICATION**

### **7.1 Comparaison avec alternatives communes**

La sélection de notre approche résulte d'une analyse comparative approfondie des différentes stratégies de recommandation selon cinq dimensions critiques:

| Dimension | Approche hybride | Filtrage collaboratif pur | Filtrage par contenu pur |
| :---- | :---- | :---- | :---- |
| Démarrage à froid | **Très bonne** \- Exploitation immédiate des données contextuelles et adaptation rapide dès les premières interactions | **Faible** \- Nécessite un historique d'interactions substantiel avant d'être performant | **Moyenne** \- Fonctionne avec les métadonnées mais limité par leur richesse |
| Temps d'implémentation | **Très rapide** \- Quelques semaines pour un POC fonctionnel | **Long** \- Plusieurs mois de collecte de données et d'ajustement des modèles | **Moyen** \- Dépend de la structuration des caractéristiques |
| Ressources requises | **Minimales** \- Fonctionne avec une infrastructure légère | **Importantes** \- Calculs intensifs pour les matrices de similarité | **Modérées** \- Traitement des métadonnées et vecteurs |
| Précision potentielle | **Bonne** \- S'améliore progressivement avec les données | **Excellente** \- Très personnalisée avec données suffisantes | **Très bonne** \- Précise sur les caractéristiques |
| Évolutivité | **Très bonne** \- Architecture modulaire adaptative | **Moyenne** \- Problèmes d'échelle avec grandes matrices | **Bonne** \- Dépend de la qualité des métadonnées |

### **7.2 Justification du choix**

La décision en faveur de l'approche hybride repose sur plusieurs avantages déterminants. Sur le plan de la complexité d'implémentation, notre solution offre un équilibre optimal entre simplicité opérationnelle et sophistication algorithmique. Cette approche permet un déploiement rapide tout en posant les fondations nécessaires aux évolutions futures.

Le temps de mise en œuvre constitue un avantage compétitif majeur. Là où les approches purement collaboratives exigent des mois de collecte de données avant d'atteindre une performance satisfaisante, notre système fréquence-récence bonifié par le contexte délivre une valeur immédiate.

L'optimisation des ressources représente également un facteur décisif. Le système actuel minimise les besoins computationnels en privilégiant des algorithmes légers de scoring qui peuvent fonctionner efficacement même sur une infrastructure modeste, tout en restant évolutifs.

La capacité à fonctionner avec des données incomplètes distingue particulièrement notre approche. Contrairement aux systèmes purement collaboratifs qui souffrent du problème de sparsité (matrices creuses), notre solution exploite intelligemment chaque fragment d'information disponible, qu'il s'agisse de données contextuelles ou d'interactions limitées.

Enfin, la combinaison d'une latence minimale (inférieure à une seconde) et d'une explicabilité naturelle des recommandations garantit une expérience utilisateur transparente et réactive, facteurs critiques pour l'adoption d'un système de recommandation dans le domaine des événements sociaux.

## **8\. CONCLUSION**

L'approche hybride avec pondération contextuelle dynamique représente un choix stratégique particulièrement adapté au système de recommandation d'événements sociaux. Elle répond avec précision aux contraintes techniques et aux objectifs d'expérience utilisateur de la plateforme.

Cette architecture tire pleinement parti de la richesse du modèle de données existant tout en offrant des recommandations pertinentes même dans les scénarios les plus complexes comme le démarrage à froid. L'équilibre judicieux entre différentes sources d'information permet d'adresser simultanément les multiples facettes qui déterminent la pertinence d'un événement pour un utilisateur \- son comportement actuel, son contexte géographique et social, et ses préférences historiques.

En priorisant la simplicité opérationnelle et l'adaptabilité en temps réel, le système offre une expérience de recommandation efficace dès ses premières versions, tout en établissant une base solide pour l'évolution future. Cette approche pragmatique et progressive garantit un retour sur investissement rapide tout en permettant l'intégration continue d'améliorations techniques.

Au-delà des performances techniques, ce système place l'utilisateur au centre de sa conception en s'adaptant instantanément à ses besoins et en évoluant avec lui. Cette philosophie centrée utilisateur, combinée à une architecture techniquement robuste, constitue la clé d'une expérience de recommandation réussie qui contribuera significativement au succès de la plateforme d'événements sociaux.