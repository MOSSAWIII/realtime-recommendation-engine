package config

import com.rabbitmq.client.{ConnectionFactory, Connection, Channel}
import com.typesafe.config.ConfigFactory
import scala.util.Try

object RabbitMQConfig {
  private val config = ConfigFactory.load()
  private val factory = new ConnectionFactory()
  
  // Configure RabbitMQ connection
  factory.setHost(config.getString("rabbitmq.host"))
  factory.setPort(config.getInt("rabbitmq.port"))
  factory.setUsername(config.getString("rabbitmq.username"))
  factory.setPassword(config.getString("rabbitmq.password"))
  factory.setVirtualHost(config.getString("rabbitmq.virtualHost"))
  
  // Connection pool settings
  factory.setAutomaticRecoveryEnabled(true)
  factory.setNetworkRecoveryInterval(5000)
  
  // Create connection
  private var connection: Connection = _
  
  def getConnection(): Connection = {
    if (connection == null || !connection.isOpen) {
      connection = factory.newConnection()
    }
    connection
  }
  
  def createChannel(): Channel = {
    getConnection().createChannel()
  }
  
  def close(): Unit = {
    if (connection != null && connection.isOpen) {
      connection.close()
    }
  }
  
  // Define exchange and queue names as constants
  object Exchanges {
    val USER_INTERACTIONS = "user_interactions"
    val EVENT_UPDATES = "event_updates" 
    val RECOMMENDATIONS = "recommendations"
  }
  
  object Queues {
    val USER_INTERACTIONS = "user_interactions_queue"
    val EVENT_UPDATES = "event_updates_queue"
    val RECOMMENDATION_REQUESTS = "recommendation_requests_queue"
    val RECOMMENDATION_RESULTS = "recommendation_results_queue"
  }
  
  def setupExchangesAndQueues(): Try[Unit] = Try {
    val channel = createChannel()
    
    // Declare exchanges
    channel.exchangeDeclare(Exchanges.USER_INTERACTIONS, "topic", true)
    channel.exchangeDeclare(Exchanges.EVENT_UPDATES, "topic", true)
    channel.exchangeDeclare(Exchanges.RECOMMENDATIONS, "direct", true)
    
    // Declare queues
    channel.queueDeclare(Queues.USER_INTERACTIONS, true, false, false, null)
    channel.queueDeclare(Queues.EVENT_UPDATES, true, false, false, null)
    channel.queueDeclare(Queues.RECOMMENDATION_REQUESTS, true, false, false, null)
    channel.queueDeclare(Queues.RECOMMENDATION_RESULTS, true, false, false, null)
    
    // Bind queues to exchanges
    channel.queueBind(Queues.USER_INTERACTIONS, Exchanges.USER_INTERACTIONS, "#")
    channel.queueBind(Queues.EVENT_UPDATES, Exchanges.EVENT_UPDATES, "#")
    channel.queueBind(Queues.RECOMMENDATION_REQUESTS, Exchanges.RECOMMENDATIONS, "request")
    channel.queueBind(Queues.RECOMMENDATION_RESULTS, Exchanges.RECOMMENDATIONS, "result")
    
    channel.close()
  }
  
  def healthCheck(): Boolean = {
    Try {
      val channel = createChannel()
      val result = channel.isOpen
      channel.close()
      result
    }.getOrElse(false)
  }
}

