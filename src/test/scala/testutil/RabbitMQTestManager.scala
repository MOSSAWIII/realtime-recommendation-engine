package testutil

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import config.{RabbitMQConfig, RabbitMQManager}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Manages RabbitMQ connections for testing.
 */
object RabbitMQTestManager {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private var connection: Option[Connection] = None
  private val channels = scala.collection.mutable.Map.empty[String, Channel]
  
  // Define correct queue names directly
  private val QUEUE_USER_EVENTS = RabbitMQConfig.Queues.USER_EVENTS
  private val QUEUE_PROCESSED_EVENTS = "processed_events" // Use string literal instead of non-existent constant
  private val QUEUE_RECOMMENDATION_REQUESTS = RabbitMQConfig.Queues.INTERNAL_RECOMMENDATION_REQUESTS
  private val QUEUE_RECOMMENDATIONS = RabbitMQConfig.Queues.RECOMMENDATIONS
  private val QUEUE_USER_INTERACTIONS = RabbitMQConfig.Queues.USER_INTERACTIONS
  
  def createChannel(name: String): Channel = {
    if (connection.isEmpty) {
      val factory = new ConnectionFactory()
      factory.setHost(System.getenv().getOrDefault("RABBITMQ_HOST", "localhost"))
      factory.setPort(System.getenv().getOrDefault("RABBITMQ_PORT", "5672").toInt)
      factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest"))
      factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest"))
      
      connection = Some(factory.newConnection(s"test-connection-${java.util.UUID.randomUUID()}"))
      logger.info("Created new RabbitMQ connection for testing")
    }
    
    val channel = connection.get.createChannel()
    channels.put(name, channel)
    channel
  }
  
  def closeChannel(name: String): Unit = {
    channels.get(name).foreach { channel =>
      try {
        if (channel.isOpen) {
          channel.close()
        }
      } catch {
        case e: Exception => logger.warn(s"Error closing channel $name: ${e.getMessage}")
      }
      channels.remove(name)
    }
  }
  
  // Setup required queues for testing
  def setupQueues(channel: Channel): Unit = {
    logger.info("Setting up RabbitMQ test queues")
    // Declare all the necessary queues for testing
    channel.queueDeclare(QUEUE_USER_EVENTS, true, false, false, null)
    channel.queueDeclare(QUEUE_PROCESSED_EVENTS, true, false, false, null)
    channel.queueDeclare(QUEUE_RECOMMENDATION_REQUESTS, true, false, false, null)
    channel.queueDeclare(QUEUE_RECOMMENDATIONS, true, false, false, null)
    channel.queueDeclare(QUEUE_USER_INTERACTIONS, true, false, false, null)
    logger.info("RabbitMQ test queues setup complete")
  }
  
  // Purge all test queues
  def purgeAllQueues(): Unit = {
    logger.info("Purging all RabbitMQ test queues")
    val purgeChannel = createChannel("purge-channel")
    try {
      purgeChannel.queuePurge(QUEUE_USER_EVENTS)
      purgeChannel.queuePurge(QUEUE_PROCESSED_EVENTS)
      purgeChannel.queuePurge(QUEUE_RECOMMENDATION_REQUESTS)
      purgeChannel.queuePurge(QUEUE_RECOMMENDATIONS)
      purgeChannel.queuePurge(QUEUE_USER_INTERACTIONS)
      logger.info("Finished purging queues")
    } catch {
      case e: Exception => logger.warn(s"Error purging queues: ${e.getMessage}")
    } finally {
      closeChannel("purge-channel")
    }
  }
  
  // Close resources in tests
  def close(): Unit = {
    // Close all open channels
    channels.foreach { case (name, channel) =>
      try {
        if (channel.isOpen) {
          channel.close()
        }
      } catch {
        case e: Exception => logger.warn(s"Error closing channel $name: ${e.getMessage}")
      }
    }
    channels.clear()
    
    // Close connection
    connection.foreach { conn =>
      try {
        if (conn.isOpen) {
          conn.close()
        }
      } catch {
        case e: Exception => logger.warn(s"Error closing connection: ${e.getMessage}")
      }
    }
    connection = None
    logger.info("RabbitMQ test resources cleaned up")
  }
}

// Trait for easy mixing into test classes
trait RabbitMQTestManagerTrait extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>
  // Store channels created during test
  private val mockChannels = mutable.Map[String, Channel]()
  
  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  
  override def afterAll(): Unit = {
    closeAllMockChannels()
    super.afterAll()
  }
  
  def createMockChannel(name: String): Channel = {
    val channel = PowerMockito.mock(classOf[Channel])
    when(channel.isOpen).thenReturn(true)
    mockChannels.put(name, channel)
    channel
  }
  
  def closeAllMockChannels(): Unit = {
    mockChannels.clear()
  }
}

// Concrete implementation for direct use
trait RabbitMQTestManager extends RabbitMQTestManagerTrait { self: Suite => }

