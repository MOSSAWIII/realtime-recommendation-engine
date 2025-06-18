package common.utils

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * Gestionnaire de ressources de test pour garantir l'unicité des identifiants
 */
object TestResourceManager {
  private val idCounter = new AtomicLong(1000L)
  
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
    idCounter.getAndIncrement()
  }
  
  /**
   * Réinitialise le compteur d'identifiants uniques
   */
  def reset(): Unit = {
    idCounter.set(1000L)
  }
}

