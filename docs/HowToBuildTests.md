# PROMPT: Convertir des tests mockés vers des tests de vrais composants

## Contexte
Je travaille sur [DESCRIPTION DU PROJET] et j'ai des tests qui utilisent principalement des mocks au lieu de tester les vrais composants du code de production. Je veux m'assurer que mes tests valident réellement le comportement de mon code main.

## Objectif
Convertir mes tests pour qu'ils utilisent les **vrais composants** du code de production au lieu de mocks, tout en gardant les tests fiables et rapides.

## Principes à suivre

### 1. **Utiliser le vrai code de production**
- ✅ Importer et utiliser les vraies classes du package `main/`
- ✅ Utiliser les vraies méthodes et constructeurs
- ❌ Éviter de redéfinir/override des méthodes dans les tests
- ❌ Éviter de créer des implémentations de test qui remplacent la logique métier

### 2. **Infrastructure réelle pour les tests d'intégration**
- ✅ Utiliser de vraies bases de données (TestContainers, bases temporaires)
- ✅ Utiliser de vrais message brokers (RabbitMQ, Kafka en mode test)
- ✅ Utiliser les vraies configurations (avec des valeurs de test)
- ❌ Ne pas mocker l'infrastructure dans les tests d'intégration

### 3. **Garder les mocks uniquement pour**
- ✅ Les dépendances externes (APIs tierces, services distants)
- ✅ Les composants coûteux en temps/ressources
- ✅ Les cas d'erreur difficiles à reproduire naturellement
- ✅ L'isolation dans les tests unitaires stricts

### 4. **Structure des tests**
```
unit/          → Tests unitaires (mocks autorisés pour isolation)
integration/   → Tests d'intégration (vrais composants + vraie infrastructure)
functional/    → Tests end-to-end (système complet)
```

## Instructions spécifiques

### Analysez le test actuel et identifiez :
1. **Quels composants sont mockés mais devraient être réels ?**
2. **Quelle infrastructure est nécessaire pour le test ?**
3. **Quelles données de test réelles utiliser ?**
4. **Comment valider que le comportement est correct ?**

### Transformez le test pour :
1. **Remplacer les mocks par les vraies classes**
   ```scala
   // ❌ Au lieu de
   val mockService = mock[MyService]
   
   // ✅ Faire
   val realService = new MyService(realDependencies)
   ```

2. **Utiliser les vraies fabriques/constructeurs**
   ```scala
   // ❌ Au lieu de créer manuellement
   val fakeEvent = new UserEvent(...)
   
   // ✅ Utiliser les factory methods du code main
   val realEvent = UserEventFactory.create(...)
   ```

3. **Configurer l'infrastructure réelle**
   ```scala
   // ✅ Pour tests d'intégration
   val realDatabase = TestDatabase.start()
   val realMessageBroker = TestMessageBroker.start()
   ```

4. **Valider les effets de bord réels**
   ```scala
   // ✅ Vérifier que les données sont vraiment persistées
   val storedData = database.findById(id)
   storedData should contain(expectedData)
   
   // ✅ Vérifier que les messages sont vraiment envoyés
   val receivedMessages = messageConsumer.getMessages()
   receivedMessages should contain(expectedMessage)
   ```

## Checklist de validation

- [ ] Le test utilise les vraies classes du code de production
- [ ] Le test utilise la vraie infrastructure (DB, messaging, etc.)
- [ ] Le test utilise les vraies méthodes de factory/création
- [ ] Le test valide les vrais effets de bord
- [ ] Le test peut détecter les vrais bugs dans le code de production
- [ ] Le test reste maintenable et pas trop lent
- [ ] Les données de test sont réalistes
- [ ] Le nettoyage des ressources est correct

## Exemple de transformation

### Avant (mocké) :
```scala
val mockManager = mock[DataManager]
when(mockManager.storeEvent(any)).thenReturn(Future.successful(()))
// Test ne valide pas le vrai comportement
```

### Après (réel) :
```scala
val realDatabase = TestDatabase.start()
val realManager = new DataManager(realDatabase)
realManager.storeEvent(testEvent)
val storedEvent = realDatabase.findEvent(testEvent.id)
storedEvent shouldBe defined
```

---

**IMPORTANT** : Gardez à l'esprit que l'objectif est de **tester le vrai code de production** pour s'assurer qu'il fonctionne correctement, pas de tester des implémentations artificielles créées uniquement pour les tests.