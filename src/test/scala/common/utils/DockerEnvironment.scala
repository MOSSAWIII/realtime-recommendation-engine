// filepath: src/test/scala/common/utils/DockerEnvironment.scala
package common.utils

import scala.util.Try
import java.io.File
import org.slf4j.LoggerFactory

/**
 * Utilitaire pour gérer les variables d'environnement Docker dans les tests
 */
object DockerEnvironment {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Détection de l'environnement Docker
  val isDockerEnvironment: Boolean = 
    new File("/.dockerenv").exists() || 
    Try(scala.io.Source.fromFile("/proc/1/cgroup").getLines().exists(_.contains("/docker/"))).getOrElse(false)
  
  // Récupération des variables d'environnement avec valeurs par défaut
  def getEnvOrDefault(name: String, default: String): String = 
    Option(System.getenv(name)).getOrElse(default)
  
  // Valeurs par défaut pour les services selon l'environnement
  val dbHost: String = if (isDockerEnvironment) "test-db" else "localhost" 
  val dbPort: String = if (isDockerEnvironment) "5432" else "5433"
  val dbName: String = getEnvOrDefault("DATABASE_NAME", "test_recommendation_engine")
  val dbUser: String = getEnvOrDefault("DATABASE_USER", "postgres")
  val dbPassword: String = getEnvOrDefault("DATABASE_PASSWORD", "test_password")
  
  val rmqHost: String = if (isDockerEnvironment) "test-rabbitmq" else "localhost"
  val rmqPort: String = if (isDockerEnvironment) "5672" else "5673"
  val rmqUser: String = getEnvOrDefault("RABBITMQ_USERNAME", "guest")
  val rmqPassword: String = getEnvOrDefault("RABBITMQ_PASSWORD", "guest")
  
  // Test run ID pour créer des ressources uniques
  val testRunId: String = Option(System.getenv("TEST_RUN_ID"))
    .orElse(Option(System.getProperty("test.runId")))
    .getOrElse(TestResourceManager.uniqueId().toString)
  
  /**
   * Exporte toutes les variables d'environnement pertinentes pour les tests
   */
  def exportAllVariables(): Unit = {
    logger.debug(s"Using test run ID: $testRunId")
    
    val vars = Map(
      "TEST_RUNID" -> testRunId,
      "DATABASE_HOST" -> dbHost,
      "DATABASE_PORT" -> dbPort,
      "DATABASE_NAME" -> dbName,
      "DATABASE_USER" -> dbUser,
      "DATABASE_PASSWORD" -> dbPassword,
      "RABBITMQ_HOST" -> rmqHost,
      "RABBITMQ_PORT" -> rmqPort,
      "RABBITMQ_USERNAME" -> rmqUser,
      "RABBITMQ_PASSWORD" -> rmqPassword,
      "TEST_EXCHANGE" -> s"user_actions_exchange_${testRunId}",
      "TEST_PROCESSED_EVENTS_QUEUE" -> s"processed_events_queue_${testRunId}",
      "TEST_RECOMMENDATIONS_QUEUE" -> s"recommendations_queue_${testRunId}",
      "TEST_HISTORICAL_DATA_QUEUE" -> s"historical_data_queue_${testRunId}"
    )
    
    // Définir les propriétés système
    vars.foreach { case (key, value) =>
      System.setProperty(key.toLowerCase.replace("_", "."), value)
    }
    
    logger.debug("Environment variables exported to system properties")
  }
  
  /**
   * Obtient l'URL JDBC pour la base de données de test
   */
  def getJdbcUrl: String = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
  
  /**
   * Initialise l'environnement pour les tests
   */
  def initialize(): Unit = {
    logger.info(s"Initializing test environment (Docker: $isDockerEnvironment)")
    exportAllVariables()
  }
}

