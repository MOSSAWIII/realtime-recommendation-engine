// filepath: src/test/scala/common/tags/TestTagsGuide.scala
package common.tags

import org.scalatest.Tag

/**
 * Tags pour catégoriser les tests
 */
object TestTagsGuide {
  // Types de tests
  object UnitTest extends Tag("UnitTest")
  object IntegrationTest extends Tag("IntegrationTest") 
  object FunctionalTest extends Tag("FunctionalTest")
  
  // Ressources utilisées
  object DatabaseTest extends Tag("DatabaseTest")
  object RabbitMQTest extends Tag("RabbitMQTest")
  object HttpTest extends Tag("HttpTest")
  
  // Dépendances entre tests
  object Sequential extends Tag("Sequential")
  object Isolated extends Tag("Isolated")
  
  // Durée d'exécution
  object Slow extends Tag("Slow")
  object Fast extends Tag("Fast")
  
  // État de migration
  object Migrated extends Tag("Migrated")
  object Legacy extends Tag("Legacy")
}

/**
 * Guide d'utilisation des tags
 */
object TestTagsGuidelines {  // Renommé pour éviter le conflit
  /**
   * Directives d'utilisation des tags
   */
  val guidelines = """
    | # Guide d'utilisation des tags de test
    |
    | ## Tags par type de test
    | - `@UnitTest`: Tests unitaires isolés sans dépendances externes
    | - `@IntegrationTest`: Tests d'intégration entre composants
    | - `@FunctionalTest`: Tests fonctionnels de bout en bout
    |
    | ## Tags par ressource
    | - `@DatabaseTest`: Tests qui requièrent une base de données
    | - `@RabbitMQTest`: Tests qui utilisent RabbitMQ
    | - `@HttpTest`: Tests qui font des appels HTTP
    |
    | ## Tags d'exécution
    | - `@Sequential`: Tests qui doivent s'exécuter séquentiellement
    | - `@Isolated`: Tests garantis sans effets de bord
    | - `@Slow`: Tests qui prennent plus de 5 secondes
    | - `@Fast`: Tests rapides (moins de 500ms)
    |
    | ## Exemples d'utilisation
    |
    | ```scala
    | class MyTest extends IsolatedTestBase {
    |   "My test" should "do something" taggedAs(UnitTest, Fast) in {
    |     // Test code
    |   }
    | }
    | ```
    |
    | ## Exécution avec tags
    |
    | Pour exécuter les tests avec un tag spécifique:
    | ```
    | sbt "testOnly -- -n UnitTest"
    | ```
    |
    | Pour exclure les tests avec un tag spécifique:
    | ```
    | sbt "testOnly -- -l Slow"
    | ```
  """.stripMargin
  
  /**
   * Règles d'application des tags
   */
  val rules = Map(
    "UnitTest" -> "Tous les tests unitaires doivent avoir ce tag",
    "IntegrationTest" -> "Tous les tests d'intégration doivent avoir ce tag",
    "FunctionalTest" -> "Tous les tests fonctionnels doivent avoir ce tag",
    "DatabaseTest" -> "Tout test qui accède à une base de données doit avoir ce tag",
    "RabbitMQTest" -> "Tout test qui utilise RabbitMQ doit avoir ce tag",
    "Sequential" -> "À utiliser uniquement si le test ne peut pas s'exécuter en parallèle",
    "Isolated" -> "Indique que le test est complètement isolé et réentrant"
  )
}