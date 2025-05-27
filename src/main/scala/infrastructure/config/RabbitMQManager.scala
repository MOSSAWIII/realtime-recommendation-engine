package infrastructure.messaging.rabbitmq

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.typesafe.config.ConfigFactory
import infrastructure.config.DatabaseConfig
import infrastructure.config.RabbitMQConfigSection
import org.slf4j.LoggerFactory
import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Manager for RabbitMQ connections and channels
 */
object RabbitMQManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private val configSection = new RabbitMQConfigSection(ConfigFactory.load())
  
  private var connection: Connection = _
  private val channels = new ConcurrentHashMap[String, Channel]()
  
  // Define standard queue and exchange names based on simplified architecture diagram
  object Exchanges {
    val USER_ACTIONS = "user_actions_exchange"
  }
  
  object Queues {
    val PROCESSED_EVENTS = "processed_events_queue"  
    val RECOMMENDATIONS = "recommendations_queue"
    val HISTORICAL_DATA = "historical_data_queue"
  }
  
  object RoutingKeys {
    // Client to Transformer (user behaviors and interactions)
    val USER_ACTIONS = "user.action.#"
    
    // Transformer to Recommender (processed events)
    val PROCESSED_EVENTS = "event.processed.#"
    
    // Recommender to Client (recommendation results)
    val RECOMMENDATIONS = "recommendation.result.#"
    
    // Client to DataManager (historical data)
    val HISTORICAL_DATA = "historical.data.#"
  }
  
  // Message schema registry
  private val schemaRegistry = mutable.Map[String, String]()
  
  def registerSchema(messageType: String, schema: String): Unit = {
    logger.info(s"Registering schema for message type: $messageType")
    schemaRegistry.put(messageType, schema)
  }

  def getSchema(messageType: String): Option[String] = {
    schemaRegistry.get(messageType)
  }
  
  /**
   * Gets or creates a RabbitMQ connection
   */
  def getConnection: Connection = synchronized {
    if (connection == null || !connection.isOpen) {
      logger.info("Initializing RabbitMQ connection")
      initializeConnection()
    }
    connection
  }
  
  /**
   * Initialize RabbitMQ connection
   */
  private def initializeConnection(): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost(configSection.host)
    factory.setPort(configSection.port)
    factory.setUsername(configSection.username)
    factory.setPassword(configSection.password)
    
    connection = factory.newConnection("recommendation-engine")
    logger.info("RabbitMQ connection established")
  }
  
  /**
   * Create a channel with automatic reconnection
   */
  def createChannel(name: String): Channel = {
    synchronized {
      if (connection == null || !connection.isOpen) {
        logger.info("Initializing RabbitMQ connection")
        initializeConnection()
      }
      
      try {
        val channel = connection.createChannel()
        channels.put(name, channel)
        logger.info(s"Creating RabbitMQ channel: $name")
        
        // Configurer la confirmation de publication si nécessaire
        if (configSection.publisherConfirmEnabled) {
          channel.confirmSelect()
        }
        
        channel
      } catch {
        case e: Exception =>
          logger.error(s"Failed to create channel $name: ${e.getMessage}", e)
          throw e
      }
    }
  }
  
  /**
   * Gets or creates a channel with the given name
   */
  def getChannel(name: String): Channel = {
    Option(channels.get(name)) match {
      case Some(ch) if ch.isOpen => ch
      case _ => createChannel(name)
    }
  }
  
  /**
   * Closes a channel with the given name
   */
  def closeChannel(name: String): Unit = {
    Option(channels.get(name)).foreach { ch =>
      try {
        if (ch.isOpen) {
          logger.info(s"Closing RabbitMQ channel: $name")
          ch.close()
        }
        channels.remove(name)
      } catch {
        case e: Exception => 
          logger.warn(s"Error closing channel $name: ${e.getMessage}")
      }
    }
  }
  
  /**
   * Sets up RabbitMQ exchanges and queues according to the simplified architecture diagram
   */
  def setupExchangesAndQueues(): Unit = {
    val channel = createChannel("setup")
    try {
      // Declare exchange
      logger.info(s"Declaring exchange ${Exchanges.USER_ACTIONS}")
      channel.exchangeDeclare(Exchanges.USER_ACTIONS, "topic", true)
      
      // Declare queues
      logger.info("Declaring queues from simplified architecture")
      channel.queueDeclare(Queues.PROCESSED_EVENTS, true, false, false, null)
      channel.queueDeclare(Queues.RECOMMENDATIONS, true, false, false, null)
      channel.queueDeclare(Queues.HISTORICAL_DATA, true, false, false, null)
      
      // Bind queues to exchange
      logger.info("Binding queues to exchange")
      channel.queueBind(Queues.PROCESSED_EVENTS, Exchanges.USER_ACTIONS, RoutingKeys.PROCESSED_EVENTS)
      channel.queueBind(Queues.RECOMMENDATIONS, Exchanges.USER_ACTIONS, RoutingKeys.RECOMMENDATIONS)
      channel.queueBind(Queues.HISTORICAL_DATA, Exchanges.USER_ACTIONS, RoutingKeys.HISTORICAL_DATA)
      
      logger.info("RabbitMQ exchanges and queues set up successfully")
    } finally {
      closeChannel("setup")
    }
  }
  
  /**
   * Closes all channels and the connection
   */
  def shutdown(): Unit = {
    logger.info("Shutting down RabbitMQ connections")
    
    // Close all channels
    import scala.jdk.CollectionConverters._
    channels.keySet().asScala.foreach(closeChannel)
    channels.clear()
    
    // Close connection
    if (connection != null && connection.isOpen) {
      try {
        connection.close()
        logger.info("RabbitMQ connection closed")
      } catch {
        case e: Exception => 
          logger.warn(s"Error closing RabbitMQ connection: ${e.getMessage}")
      }
    }
  }
}

