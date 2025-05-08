package config

import com.typesafe.config.ConfigFactory
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

/**
 * Compatibility layer for existing code that uses RabbitMQConfig
 * Delegates to RabbitMQManager for actual implementation
 */
object RabbitMQConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  private val config = ConfigFactory.load()
  
  val Host: String = sys.env.getOrElse("RABBITMQ_HOST", config.getString("rabbitmq.host"))
  val Port: Int = sys.env.getOrElse("RABBITMQ_PORT", config.getString("rabbitmq.port")).toInt
  val Username: String = sys.env.getOrElse("RABBITMQ_USERNAME", config.getString("rabbitmq.username"))
  val Password: String = sys.env.getOrElse("RABBITMQ_PASSWORD", config.getString("rabbitmq.password"))
  
  // Create a channel without component tracking for backward compatibility
  def createChannel(): Channel = {
    logger.debug("Creating channel via compatibility layer")
    RabbitMQManager.getChannel("legacy-channel")
  }
  
  def closeConnection(): Unit = {
    logger.debug("Closing connection via compatibility layer")
    RabbitMQManager.closeConnection()
  }
  
  // Object Queues should match the one in RabbitMQManager
  object Queues {
    val USER_EVENTS = "user_events"
    val PROCESSED_EVENTS = "processed_events" 
    val INTERNAL_RECOMMENDATION_REQUESTS = "internal_recommendation_requests"
    val RECOMMENDATIONS = "recommendations"
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates"
    val PARTICIPATION_UPDATES = "participation_updates"
    val FRIENDSHIP_UPDATES = "friendship_updates"
  }
}

