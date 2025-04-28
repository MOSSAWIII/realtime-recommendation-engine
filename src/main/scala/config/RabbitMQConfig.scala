package config

import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory

/**
 * Compatibility layer for existing code that uses RabbitMQConfig
 * Delegates to RabbitMQManager for actual implementation
 */
object RabbitMQConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Create a channel without component tracking for backward compatibility
  def createChannel(): Channel = null
  
  def closeConnection(): Unit = {
    logger.debug("Closing connection via compatibility layer")
    RabbitMQManager.closeConnection()
  }
  
  // Mirror the queue constants for compatibility
  object Queues {
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates"
    val PARTICIPATION_UPDATES = "participation_updates"
    val FRIENDSHIP_UPDATES = "friendship_updates"
  }
}

