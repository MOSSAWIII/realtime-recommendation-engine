package testutil

import com.rabbitmq.client.{Channel, ConnectionFactory, Connection}
import config.{RabbitMQConfig, RabbitMQManager}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

/**
 * Configuration for RabbitMQ in tests
 */
object RabbitMQTestConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Load test configuration
  private val testConfig = ConfigFactory.load("application-test.conf")
  
  // Access credentials from config
  val host: String = testConfig.getString("rabbitmq.host")
  val port: Int = testConfig.getInt("rabbitmq.port") 
  val username: String = testConfig.getString("rabbitmq.username")
  val password: String = testConfig.getString("rabbitmq.password")
  val vhost: String = "test"
  
  // Queue names
  val QUEUE_USER_EVENTS = "user_events"
  val QUEUE_PROCESSED_EVENTS = "processed_events"
  val QUEUE_RECOMMENDATION_REQUESTS = "internal_recommendation_requests"
  val QUEUE_RECOMMENDATIONS = "recommendations"
  
  // Create a test channel with a specific name
  def createTestChannel(name: String): Channel = {
    logger.info(s"Creating test channel with name: $name")
    RabbitMQManager.createChannel(name)
  }
  
  // Helper method to purge test queues
  def purgeTestQueues(): Unit = {
    logger.info("Purging test queues")
    val queuesToDelete = Seq(
      RabbitMQConfig.Queues.USER_EVENTS,
      RabbitMQConfig.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
      RabbitMQConfig.Queues.RECOMMENDATIONS,
      QUEUE_PROCESSED_EVENTS // Use the correct queue name
    )
    
    val channel = createTestChannel("test-purge")
    try {
      queuesToDelete.foreach { queueName =>
        try {
          logger.info(s"Purging queue: $queueName")
          channel.queuePurge(queueName)
        } catch {
          case e: Exception => 
            logger.warn(s"Error purging queue $queueName: ${e.getMessage}")
        }
      }
    } finally {
      RabbitMQManager.closeChannel("test-purge")
    }
  }
  
  def setupQueues(connection: Connection): Unit = {
    val channel = connection.createChannel()
    try {
      // Create queues
      channel.queueDeclare(RabbitMQConfig.Queues.USER_EVENTS, true, false, false, null)
      
      // Use the correct queue name - "processed_events" instead of PROCESSED_EVENTS
      channel.queueDeclare(QUEUE_PROCESSED_EVENTS, true, false, false, null)
      
      channel.queueDeclare(RabbitMQConfig.Queues.INTERNAL_RECOMMENDATION_REQUESTS, true, false, false, null)
      channel.queueDeclare(RabbitMQConfig.Queues.RECOMMENDATIONS, true, false, false, null)
    } finally {
      if (channel != null && channel.isOpen) channel.close()
    }
  }
}

