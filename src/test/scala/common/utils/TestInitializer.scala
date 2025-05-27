package common.utils

import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import java.io.File
import scala.concurrent.duration._

/**
 * Initialisation des tests avec la configuration des conteneurs
 */
object TestInitializer {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  
  /**
   * Initialise l'environnement de test
   */
  def initializeTestEnvironment(): Config = {
    val testRunId = getTestRunId
    logger.info(s"Initializing test environment with ID: $testRunId")
    
    // Charger la configuration
    val baseConfig = ConfigFactory.load("application-test.conf")
    
    // Créer une configuration avec des ressources uniques
    baseConfig
      .withValue("rabbitmq.exchange.name", 
                ConfigValueFactory.fromAnyRef(s"user_actions_exchange_$testRunId"))
      .withValue("test.runId", 
                ConfigValueFactory.fromAnyRef(testRunId))
  }
  
  /**
   * Récupère l'ID unique du run de test depuis l'environnement
   */
  def getTestRunId: String = {
    // Priorité: variable d'environnement > fichier > généré
    Option(System.getenv("TEST_RUN_ID"))
      .orElse(Try(scala.io.Source.fromFile(".test_run_id").getLines().mkString.trim).toOption)
      .getOrElse(TestResourceManager.uniqueId().toString)
  }
  
  /**
   * Vérifie si l'environnement de test est en mode Docker ou local
   */
  def isDockerEnvironment: Boolean = {
    new File("/.dockerenv").exists() || 
    Try(scala.io.Source.fromFile("/proc/1/cgroup").getLines().exists(_.contains("/docker/"))).getOrElse(false)
  }
}

