package common.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager}
import scala.concurrent.duration._

/**
 * Centralized test configuration
 */
object TestConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Main configuration
  val config: Config = ConfigFactory.load("application-test.conf")
    .withFallback(ConfigFactory.load())
    .resolve()
  
  // Common test parameters
  val enableMocking: Boolean = getConfigBooleanWithFallback("test.enableMocking", true)
  val isTestMode: Boolean = getConfigBooleanWithFallback("test.mode", true)
  
  // Configuration sections
  def getDatabaseConfig: Config = config.getConfig("database")
  def getRabbitMQConfig: Config = config.getConfig("rabbitmq")
  
  // Helper for getting values with fallback
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
  
  // Create dynamic test configuration with container connection info
  def createContainerConfig(
    dbUrl: String, 
    dbUser: String, 
    dbPassword: String,
    rabbitmqHost: String, 
    rabbitmqPort: Int
  ): Config = {
    config
      .withValue("database.url", ConfigValueFactory.fromAnyRef(dbUrl))
      .withValue("database.user", ConfigValueFactory.fromAnyRef(dbUser))
      .withValue("database.password", ConfigValueFactory.fromAnyRef(dbPassword))
      .withValue("rabbitmq.host", ConfigValueFactory.fromAnyRef(rabbitmqHost))
      .withValue("rabbitmq.port", ConfigValueFactory.fromAnyRef(rabbitmqPort))
  }
  
  // Database connection for tests
  object DatabaseConfig {
    def getConnection: Connection = {
      if (enableMocking) {
        // For unit tests with mocking
        import org.mockito.Mockito.mock
        mock(classOf[Connection])
      } else {
        // For integration tests with real DB
        try {
          val dbConfig = getDatabaseConfig
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
}

