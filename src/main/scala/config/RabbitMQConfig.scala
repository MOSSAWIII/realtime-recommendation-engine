package config

import com.rabbitmq.client.{Connection, Channel, ConnectionFactory}
import com.typesafe.config.ConfigFactory

object RabbitMQConfig {
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
  
  def createChannel(): Channel = connection.createChannel()
  
  def closeConnection(): Unit = {
    if (connection != null && connection.isOpen) {
      connection.close()
    }
  }
  
  // Queues définies
  object Queues {
    val USER_EVENTS = "user_events"
    val RECOMMENDATIONS = "recommendations"
  }
}