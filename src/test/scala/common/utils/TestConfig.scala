package common.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager}
import com.rabbitmq.client.Channel
import scala.concurrent.duration._

/**
 * Configuration centralisée pour les tests
 */
object TestConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Configuration principale
  val config: Config = ConfigFactory.load("application-test.conf")
    .withFallback(ConfigFactory.load())
    .resolve()
  
  // Méthodes d'accès aux sections de configuration
  def getDatabaseConfig: Config = config.getConfig("database")
  def getRabbitMQConfig: Config = config.getConfig("rabbitmq")
  
  // Paramètres de test courants
  val enableMocking: Boolean = getConfigBooleanWithFallback("test.enableMocking", true)
  val isTestMode: Boolean = getConfigBooleanWithFallback("test.mode", true)
  
  // Helper pour récupérer des valeurs avec fallback
  private def getConfigBooleanWithFallback(path: String, default: Boolean): Boolean = {
    try {
      Option(System.getProperty(path))
        .orElse(Option(System.getenv(path.replace(".", "_").toUpperCase)))
        .map(_.toBoolean)
        .getOrElse(config.getBoolean(path))
    } catch {
      case e: Exception => 
        logger.warn(s"Error getting config $path: ${e.getMessage}")
        default
    }
  }
}

/**
 * DatabaseConfig for tests with proper error handling
 */
object DatabaseConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def getConnection: Connection = {
    if (TestConfig.enableMocking) {
      // For unit tests with mocking
      import org.mockito.Mockito.mock
      mock(classOf[Connection])
    } else {
      // For integration tests with real DB
      try {
        val config = TestConfig.config
        
        // Make sure we have the database config section
        if (!config.hasPath("database")) {
          throw new RuntimeException("Missing 'database' section in test configuration")
        }
        
        val dbConfig = config.getConfig("database")
        val url = dbConfig.getString("url")
        val user = dbConfig.getString("user")
        val password = dbConfig.getString("password")
        
        logger.info(s"Connecting to test database at $url")
        DriverManager.getConnection(url, user, password)
      } catch {
        case e: Exception =>
          val errorMessage = s"Failed to connect to database: ${e.getMessage}"
          logger.error(errorMessage, e)
          throw new RuntimeException(errorMessage, e)
      }
    }
  }
}

