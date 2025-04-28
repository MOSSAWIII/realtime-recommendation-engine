package config

import com.typesafe.config.{Config, ConfigFactory}
import java.sql.Connection
import com.rabbitmq.client.Channel

object TestConfig {
  // Force loading of test configuration
  lazy val config: Config = ConfigFactory.load("application-test.conf")
  
  // Access test-specific settings
  val enableMocking: Boolean = config.getBoolean("test.enableMocking")
}

object DatabaseConfig {
  def getConnection: Connection = {
    // For tests, return a connection based on test configuration
    val testMode = TestConfig.config.getBoolean("test.enableMocking")
    if (testMode) {
      // Return a mock connection for unit tests
      import org.mockito.Mockito.mock
      mock(classOf[Connection])
    } else {
      // Return a real connection for integration tests
      val url = TestConfig.config.getString("database.url")
      val user = TestConfig.config.getString("database.user")
      val password = TestConfig.config.getString("database.password")
      java.sql.DriverManager.getConnection(url, user, password)
    }
  }
}

object RabbitMQConfig {
  def createChannel(): Channel = {
    // For tests, return a channel based on test configuration
    val testMode = TestConfig.config.getBoolean("test.enableMocking")
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
    val USER_INTERACTIONS = "user-interactions"
    val EVENT_UPDATES = "event-updates"
    val PARTICIPATION_UPDATES = "participation-updates"
    val FRIENDSHIP_UPDATES = "friendship-updates"
    val USER_EVENTS = "user-events"
    val RECOMMENDATIONS = "recommendations"
  }
}

