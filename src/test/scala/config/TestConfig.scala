package config

import com.typesafe.config.{Config, ConfigFactory}
import java.sql.{Connection, DriverManager}
import org.slf4j.LoggerFactory
import com.rabbitmq.client.Channel

/**
 * Test configuration that loads application-test.conf
 */
object TestConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Load test configuration
  val config: Config = ConfigFactory.load("application-test.conf")
    .withFallback(ConfigFactory.load())
    .resolve()
  
  // Safe check for test mode
  def isTestMode: Boolean = {
    try {
      Option(System.getProperty("test.mode"))
        .orElse(Option(System.getenv("TEST_MODE")))
        .map(_.toBoolean)
        .getOrElse(true) // Default to true in test environment
    } catch {
      case e: Exception => 
        logger.warn(s"Error checking test mode: ${e.getMessage}")
        true // Default to true if there's an error
    }
  }
  
  // Check if mocking should be used in tests
  def enableMocking: Boolean = {
    try {
      Option(System.getProperty("test.enableMocking"))
        .orElse(Option(System.getenv("TEST_MOCKING")))
        .map(_.toBoolean)
        .getOrElse(config.getBoolean("test.enableMocking"))
    } catch {
      case e: Exception => 
        logger.warn(s"Error checking mocking flag: ${e.getMessage}")
        true // Default to true for tests
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

object RabbitMQConfig {
  def createChannel(): Channel = {
    // For tests, return a channel based on test configuration
    val testMode = TestConfig.enableMocking
    if (testMode) {
      // Return a mock channel for unit tests
      import org.mockito.Mockito.mock
      mock(classOf[Channel])
    } else {
      // Return a real channel for integration tests
      val factory = new com.rabbitmq.client.ConnectionFactory()
      factory.setHost(TestConfig.config.getString("rabbitmq.host"))
      factory.setPort(TestConfig.config.getInt("rabbitmq.port"))
      factory.setUsername(TestConfig.config.getString("rabbitmq.username"))
      factory.setPassword(TestConfig.config.getString("rabbitmq.password"))
      val connection = factory.newConnection()
      connection.createChannel()
    }
  }
  
  object Queues {
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates"
    val PARTICIPATION_UPDATES = "participation_updates"
    val FRIENDSHIP_UPDATES = "friendship_updates"
    val INTERNAL_RECOMMENDATION_REQUESTS = "internal_recommendation_requests"
    val RECOMMENDATIONS = "recommendations"
    val USER_EVENTS = "user_events"
  }
}

