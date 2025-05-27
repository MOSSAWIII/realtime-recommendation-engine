package common.utils

import java.util.UUID

/**
 * Gestionnaire de ressources de test pour garantir l'unicité des identifiants
 */
object TestResourceManager {
  
  /**
   * Génère un nom de queue unique avec préfixe pour tests RabbitMQ
   */
  def uniqueQueueName(prefix: String = "test_queue"): String = {
    s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"
  }
  
  /**
   * Génère un nom de base de données unique pour tests avec TestContainers
   */
  def uniqueDbName(prefix: String = "test_db"): String = {
    s"${prefix}_${UUID.randomUUID().toString.take(8)}"
  }
  
  /**
   * Génère un identifiant unique pour les ressources de test
   */
  def uniqueId(): Long = {
    System.currentTimeMillis() + scala.util.Random.nextInt(1000000)
  }
}

