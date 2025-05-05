package config

import com.rabbitmq.client.{Connection, Channel, ConnectionFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import scala.util.{Try, Success, Failure}

/**
 * Centralized RabbitMQ connection manager.
 * Responsible for managing all RabbitMQ connections and channels.
 */
object RabbitMQManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private var config = ConfigFactory.load()
  
  // Allow testing to override config
  def setConfig(newConfig: Config): Unit = synchronized {
    config = newConfig
    // Reset connection when config changes
    closeConnection()
    factory = null
  }
  
  private def getHost = config.getString("rabbitmq.host")
  private def getPort = config.getInt("rabbitmq.port")
  private def getUsername = config.getString("rabbitmq.username")
  private def getPassword = config.getString("rabbitmq.password")
  
  private var factory: ConnectionFactory = _
  private var connection: Connection = _
  
  // Track all created channels to ensure proper cleanup
  private val channels = new ConcurrentHashMap[String, Channel]()
  
  private def createFactory(): ConnectionFactory = {
    val f = new ConnectionFactory()
    f.setHost(getHost)
    f.setPort(getPort)
    f.setUsername(getUsername)
    f.setPassword(getPassword)
    
    // Optional configuration
    if (config.hasPath("rabbitmq.virtualHost")) {
      f.setVirtualHost(config.getString("rabbitmq.virtualHost"))
    }
    if (config.hasPath("rabbitmq.connectionTimeout")) {
      f.setConnectionTimeout(config.getInt("rabbitmq.connectionTimeout"))
    }
    
    f
  }
  
  // Thread-safe connection creation with retry
  def getConnection: Connection = synchronized {
    if (connection == null || !connection.isOpen) {
      logger.info(s"Creating new RabbitMQ connection to ${getHost}:${getPort}")
      
      val maxRetries = if (config.hasPath("rabbitmq.connectionRetries")) {
        config.getInt("rabbitmq.connectionRetries") 
      } else 3
      
      var retries = 0
      var connected = false
      var lastError: Throwable = null
      
      while (!connected && retries < maxRetries) {
        try {
          if (factory == null) factory = createFactory()
          connection = factory.newConnection()
          connected = true
          logger.info("Successfully connected to RabbitMQ")
        } catch {
          case e: Exception =>
            lastError = e
            retries += 1
            logger.warn(s"Failed to connect to RabbitMQ (attempt $retries/$maxRetries): ${e.getMessage}")
            Thread.sleep(1000) // Wait before retry
        }
      }
      
      if (!connected) {
        logger.error(s"Failed to connect to RabbitMQ after $maxRetries attempts")
        throw new RuntimeException("Failed to establish RabbitMQ connection", lastError)
      }
    }
    
    connection
  }
  
  // Get a channel with component tracking for cleanup
  def getChannel(componentName: String): Channel = {
    val channelKey = componentName
    
    if (channels.containsKey(channelKey)) {
      val existingChannel = channels.get(channelKey)
      if (existingChannel.isOpen) {
        return existingChannel
      }
      
      // Channel exists but is closed, remove it
      channels.remove(channelKey)
    }
    
    // Create a new channel
    try {
      val newChannel = getConnection.createChannel()
      channels.put(channelKey, newChannel)
      
      // Add shutdown listener
      newChannel.addShutdownListener(cause => {
        logger.info(s"Channel for component '$componentName' has been shut down: ${cause.getMessage}")
        channels.remove(channelKey)
      })
      
      newChannel
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create channel for component '$componentName': ${e.getMessage}")
        throw e
    }
  }
  
  // Method alias for backward compatibility
  def createChannel(componentName: String): Channel = {
    logger.debug(s"Creating channel for component '$componentName' via alias")
    getChannel(componentName)
  }
  
  // Close a specific channel by component name
  def closeChannel(componentName: String): Unit = synchronized {
    val channelKey = componentName
    if (channels.containsKey(channelKey)) {
      val channel = channels.get(channelKey)
      try {
        if (channel.isOpen) {
          channel.close()
          logger.debug(s"Closed channel for component '$componentName'")
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Error closing channel for component '$componentName': ${e.getMessage}")
      } finally {
        channels.remove(channelKey)
      }
    }
  }
  
  // Close all channels
  def closeAllChannels(): Unit = synchronized {
    logger.debug("Closing all channels")
    import scala.collection.JavaConverters._
    channels.keySet().asScala.foreach(closeChannel)
  }
  
  // Close all channels and connection
  def closeConnection(): Unit = synchronized {
    logger.info("Closing all RabbitMQ channels and connection")
    
    // Close all channels
    closeAllChannels()
    
    // Close connection
    if (connection != null && connection.isOpen) {
      Try(connection.close()) match {
        case Failure(e) => logger.warn(s"Error closing connection: ${e.getMessage}")
        case _ => // Success, do nothing
      }
    }
    connection = null
  }
  
  // Register shutdown hook for proper cleanup
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      closeConnection()
    }
  })
  
  // Queue constants
  object Queues {
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates"
    val PARTICIPATION_UPDATES = "participation_updates"
    val FRIENDSHIP_UPDATES = "friendship_updates"
    val INTERNAL_RECOMMENDATION_REQUESTS = "internal_recommendation_requests"
    val RECOMMENDATIONS = "recommendations"
    val USER_EVENTS = "user_events"  // Add this missing queue constant
  }
}

