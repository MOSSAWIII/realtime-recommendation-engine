package common.tags

import org.scalatest.Tag

/**
 * Tags pour catégoriser les tests
 */
object TestTags {
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
}