package common.utils

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import org.mockito.Mockito._
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * Utilitaire pour faciliter les tests avec RabbitMQ
 */
object RabbitMQTestHelper {
  private val logger = LoggerFactory.getLogger(getClass)
  private var connection: Option[Connection] = None
  private val channels = scala.collection.mutable.Map[String, Channel]()
  private var mockChannel: Option[Channel] = None
  
  def setMockChannel(channel: Channel): Unit = {
    mockChannel = Some(channel)
    logger.debug(s"Mock channel set: $channel")
  }
  
  def getMockOrRealChannel(name: String): Channel = {
    mockChannel.getOrElse {
      logger.warn("Creating mock channel as no connection exists")
      val channel = mock(classOf[Channel])
      setMockChannel(channel)
      channel
    }
  }
  
  def clearMockChannel(): Unit = {
    mockChannel = None
  }
  
  /**
   * Initialise une connexion RabbitMQ pour les tests
   */
  def initConnection(host: String = "localhost", port: Int = 5673): Boolean = {
    try {
      val factory = new ConnectionFactory()
      factory.setHost(host)
      factory.setPort(port)
      factory.setUsername("guest")
      factory.setPassword("guest")
      connection = Some(factory.newConnection())
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to initialize RabbitMQ connection: ${e.getMessage}")
        false
    }
  }
  
  /**
   * Crée un canal avec un nom unique pour les tests
   */
  def createChannel(name: String): Channel = {
    if (channels.contains(name)) {
      return channels(name)
    }
    
    val channel = connection.fold {
      getMockOrRealChannel(name)
    } (_.createChannel())
    
    channels.put(name, channel)
    channel
  }
  
  /**
   * Nettoie toutes les ressources RabbitMQ
   */
  def cleanup(): Unit = {
    channels.foreach { case (name, channel) => 
      Try(channel.close()).recover { case e => 
        logger.warn(s"Error closing channel $name: ${e.getMessage}") 
      }
    }
    channels.clear()
    
    connection.foreach { conn => 
      Try(conn.close()).recover { case e => 
        logger.warn(s"Error closing connection: ${e.getMessage}") 
      }
    }
    connection = None
    clearMockChannel()
  }
  
  /**
   * Configure une queue de test avec des paramètres spécifiés
   */
  def setupTestQueue(queueName: String, durable: Boolean = true): Try[Unit] = {
    Try {
      val channel = createChannel(s"setup-$queueName")
      channel.queueDeclare(queueName, durable, false, false, null)
    }.map(_ => ())
  }
  
  /**
   * Crée une connexion RabbitMQ de test
   */
  def createTestConnection(): Connection = {
    try {
      val factory = new ConnectionFactory()
      factory.setHost(DockerEnvironment.rmqHost)
      factory.setPort(DockerEnvironment.rmqPort.toInt)
      factory.setUsername(DockerEnvironment.rmqUser)
      factory.setPassword(DockerEnvironment.rmqPassword)
      factory.newConnection()
    } catch {
      case e: Exception =>
        logger.warn(s"Could not create real RabbitMQ connection: ${e.getMessage}")
        mock(classOf[Connection])
    }
  }
}

