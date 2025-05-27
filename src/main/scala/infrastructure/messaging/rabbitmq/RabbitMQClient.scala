package infrastructure.messaging.rabbitmq

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.typesafe.config.Config
import infrastructure.config.ConfigurationServiceInterface
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory

class RabbitMQClient(config: ConfigurationServiceInterface, channelName: String = "default") {
  private val logger = LoggerFactory.getLogger(getClass)
  private var connection: Option[Connection] = None
  private var channel: Option[Channel] = None
  
  private val host = config.getConfig.getString("rabbitmq.host")
  private val port = config.getConfig.getInt("rabbitmq.port")
  private val username = config.getConfig.getString("rabbitmq.username")
  private val password = config.getConfig.getString("rabbitmq.password")
  
  def start(): Try[Channel] = {
    Try {
      if (channel.isDefined) {
        channel.get
      } else {
        val factory = new ConnectionFactory()
        factory.setHost(host)
        factory.setPort(port)
        factory.setUsername(username)
        factory.setPassword(password)
        
        connection = Some(factory.newConnection(s"app-connection-$channelName"))
        channel = Some(connection.get.createChannel())
        logger.info(s"RabbitMQ connection established for channel: $channelName")
        channel.get
      }
    }
  }
  
  def getChannel: Channel = channel.getOrElse(throw new IllegalStateException("RabbitMQ client not started"))
  
  def close(): Unit = {
    Try {
      channel.foreach(_.close())
      connection.foreach(_.close())
      channel = None
      connection = None
      logger.info(s"RabbitMQ connection closed for channel: $channelName")
    } match {
      case Success(_) => // Fermé avec succès
      case Failure(e) => logger.error(s"Error closing RabbitMQ connection: ${e.getMessage}")
    }
  }
}

