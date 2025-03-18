# Scénarios de recommandation en temps réel - Focus sur le démarrage à froid et l'adaptation instantanée

## Scénario 1 : Nouvel utilisateur sans historique (Cold Start Problem)

### Description détaillée
**Utilisateur** : Sophie, 28 ans, vient de s'inscrire sur la plateforme il y a 10 minutes.

**Contexte d'inscription** :
- S'est inscrite via son compte Google
- Localisation actuelle : Lyon, France (détectée via GPS)
- Moment de l'inscription : Mardi, 12h30 (heure du déjeuner)
- Appareil : Application mobile (iOS)
- N'a fourni aucune préférence explicite lors de l'inscription
- N'a encore établi aucune connexion sociale sur la plateforme

**Comportements instantanés observés** :
1. A consulté immédiatement la section "Événements culturels"
2. A passé 45 secondes à regarder un événement d'exposition d'art contemporain
3. A effectué une recherche avec les mots-clés "atelier créatif week-end"
4. A activé les notifications pour sa zone géographique
5. A consulté les horaires de trois événements différents pour le samedi à venir
6. Est restée sur un événement de type workshop photo pendant 1 minute 20 secondes

**Défis spécifiques** :
- Absence totale d'historique de participation
- Aucune donnée sur ses préférences passées
- Réseau social vide (aucun ami connecté)
- Nécessité de captiver son intérêt rapidement pour encourager l'engagement

**Approche de recommandation** :
1. **Recommandation immédiate** : Proposer un mix d'événements culturels dans sa zone géographique pour le week-end à venir, en priorisant les ateliers créatifs et photographiques basés sur son comportement de navigation
2. **Invitation à l'exploration** : Suggérer des événements populaires dans différentes catégories pour observer ses réactions et affiner ses préférences
3. **Utilisation des données contextuelles** : Recommander des événements adaptés à sa tranche horaire actuelle (pause déjeuner) comme des "lunch & learn" ou des expositions à visiter rapidement
4. **Incitation au social** : Suggérer des événements avec une forte participation pour maximiser les chances de connexions sociales

**Message de recommandation** :
"Bienvenue Sophie ! Voici des ateliers créatifs ce week-end près de chez vous qui pourraient vous plaire. Le workshop photo de samedi attire déjà beaucoup d'intérêt. Explorez également nos suggestions dans d'autres catégories pour affiner vos recommandations."

## Scénario 2 : Utilisateur avec historique + comportement instantané

### Description détaillée
**Utilisateur** : Thomas, 34 ans, membre actif depuis 14 mois.

**Données historiques** :
- A participé à 17 événements (12 confirmés "yes", 3 "maybe", 2 "no après yes")
- Préférence marquée pour les événements tech (75% de ses participations)
- Tendance à participer à des événements en soirée (après 18h)
- Réseau social établi de 23 connexions
- Rayon géographique habituel : ne dépasse généralement pas 15 km de son domicile
- Notation moyenne donnée aux événements : 4.2/5
- Pattern de participation : plutôt actif en semaine, moins le week-end

**Comportements instantanés actuels** (divergents de son pattern habituel) :
1. Se connecte un mardi à 12h23 (inhabituel - généralement actif en soirée)
2. Consulte des événements dans une nouvelle catégorie : gastronomie et ateliers culinaires
3. Recherche explicite : "événements week-end famille"
4. Élargit son rayon de recherche à 30 km
5. Consulte le profil d'un chef renommé organisant un atelier parent-enfant
6. Partage un événement culinaire avec un contact externe via la fonction "inviter"

**Contexte supplémentaire inféré** :
- Changement potentiel dans sa situation personnelle (intérêt soudain pour activités familiales)
- Disponibilité inhabituelle (consultation en journée)
- Volonté d'explorer de nouveaux centres d'intérêt (gastronomie vs tech)

**Approche de recommandation** :
1. **Adaptation rapide au nouveau comportement** : Prioriser les événements gastronomiques familiaux pour le week-end à venir
2. **Équilibre historique/instantané** : Intégrer quelques événements tech mais dans un format familial (ex: ateliers de programmation parent-enfant)
3. **Exploitation du réseau social** : Identifier si certaines de ses connexions ont participé à des événements similaires
4. **Reconnaissance du changement** : Adapter le ton de la communication pour reconnaître cette exploration de nouveaux horizons

**Message de recommandation** :
"Thomas, découvrez ces ateliers culinaires en famille ce week-end. Nous avons également sélectionné cet atelier d'initiation à la programmation pour enfants qui pourrait combiner vos intérêts tech habituels avec votre recherche actuelle. Votre ami Marc a participé à un événement similaire le mois dernier."