package config

import com.rabbitmq.client.{Connection, Channel, ConnectionFactory}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap

/**
 * Centralized RabbitMQ connection manager.
 * Responsible for managing all RabbitMQ connections and channels.
 */
object RabbitMQManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private val config = ConfigFactory.load()
  
  private val host = config.getString("rabbitmq.host")
  private val port = config.getInt("rabbitmq.port")
  private val username = config.getString("rabbitmq.username")
  private val password = config.getString("rabbitmq.password")
  
  private lazy val factory = {
    val f = new ConnectionFactory()
    f.setHost(host)
    f.setPort(port)
    f.setUsername(username)
    f.setPassword(password)
    f
  }
  
  private lazy val connection: Connection = factory.newConnection()
  
  // Track all created channels to ensure proper cleanup
  private val channels = new ConcurrentHashMap[String, Channel]()
  
  // Register shutdown hook for proper cleanup
  sys.addShutdownHook {
    logger.info("Shutting down RabbitMQ connections...")
    closeAllChannels()
    closeConnection()
  }
  
  /**
   * Create a new channel with a component-specific identifier.
   * 
   * @param componentId A unique identifier for the component requesting the channel
   * @return A RabbitMQ channel
   */
  def createChannel(componentId: String): Channel = {
    val channel = connection.createChannel()
    channels.put(componentId, channel)
    logger.debug(s"Created channel for $componentId")
    channel
  }
  
  /**
   * Close a specific component's channel.
   * 
   * @param componentId The identifier of the component whose channel should be closed
   */
  def closeChannel(componentId: String): Unit = {
    Option(channels.get(componentId)).foreach { channel =>
      if (channel.isOpen) {
        try {
          channel.close()
          logger.debug(s"Closed channel for $componentId")
        } catch {
          case e: Exception => 
            logger.warn(s"Error closing channel for $componentId: ${e.getMessage}", e)
        }
      }
      channels.remove(componentId)
    }
  }
  
  /**
   * Close all open channels.
   */
  def closeAllChannels(): Unit = {
    import scala.jdk.CollectionConverters._
    channels.asScala.foreach { case (componentId, channel) =>
      if (channel.isOpen) {
        try {
          channel.close()
          logger.debug(s"Closed channel for $componentId during cleanup")
        } catch {
          case e: Exception => 
            logger.warn(s"Error closing channel for $componentId: ${e.getMessage}", e)
        }
      }
    }
    channels.clear()
  }
  
  /**
   * Close the RabbitMQ connection.
   */
  def closeConnection(): Unit = {
    if (connection != null && connection.isOpen) {
      try {
        connection.close()
        logger.info("Closed RabbitMQ connection")
      } catch {
        case e: Exception => 
          logger.warn(s"Error closing RabbitMQ connection: ${e.getMessage}", e)
      }
    }
  }
  
  object Queues {
    val USER_EVENTS = "user_events"
    val RECOMMENDATIONS = "recommendations"
    val INTERNAL_RECOMMENDATION_REQUESTS = "internal_recommendation_requests"
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates"
    val PARTICIPATION_UPDATES = "participation_updates"
    val FRIENDSHIP_UPDATES = "friendship_updates"
  }
}

