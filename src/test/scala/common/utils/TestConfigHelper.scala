package common.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.slf4j.LoggerFactory
import java.util.Properties

object TestConfigHelper {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Base test config
  lazy val testConfig: Config = ConfigFactory.load("application-test.conf")
  
  def getRabbitMQConfig: Config = {
    try {
      testConfig.getConfig("rabbitmq")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load RabbitMQ config: ${e.getMessage}")
        throw e
    }
  }
  
  def getDatabaseConfig: Config = {
    try {
      testConfig.getConfig("database")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load database config: ${e.getMessage}")
        throw e
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
    testConfig
      .withValue("database.url", ConfigValueFactory.fromAnyRef(dbUrl))
      .withValue("database.user", ConfigValueFactory.fromAnyRef(dbUser))
      .withValue("database.password", ConfigValueFactory.fromAnyRef(dbPassword))
      .withValue("rabbitmq.host", ConfigValueFactory.fromAnyRef(rabbitmqHost))
      .withValue("rabbitmq.port", ConfigValueFactory.fromAnyRef(rabbitmqPort))
  }
  
  // Set system properties from config
  def setSystemProperties(config: Config): Unit = {
    System.setProperty("database.url", config.getString("database.url"))
    System.setProperty("database.user", config.getString("database.user"))
    System.setProperty("database.password", config.getString("database.password"))
    System.setProperty("rabbitmq.host", config.getString("rabbitmq.host"))
    System.setProperty("rabbitmq.port", config.getInt("rabbitmq.port").toString)
  }
}