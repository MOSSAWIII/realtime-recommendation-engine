package infrastructure.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try
import scala.collection.JavaConverters._
import infrastructure.messaging.rabbitmq.RabbitMQManager

/**
 * Typed RabbitMQ configuration section
 */
class RabbitMQConfigSection(config: Config) extends ConfigSection {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  
  private val rmqConfig = config.getConfig("rabbitmq")
  
  val host: String = rmqConfig.getString("host")
  val port: Int = rmqConfig.getInt("port")
  val username: String = rmqConfig.getString("username")
  val password: String = rmqConfig.getString("password")
  val virtualHost: String = 
    if (rmqConfig.hasPath("virtualHost")) rmqConfig.getString("virtualHost") else "/"
  
  val connectionTimeout: FiniteDuration = 
    if (rmqConfig.hasPath("connectionTimeout")) 
      Duration(rmqConfig.getString("connectionTimeout")).asInstanceOf[FiniteDuration]
    else
      Duration("30s").asInstanceOf[FiniteDuration]
  
  val networkRecoveryInterval: FiniteDuration = 
    if (rmqConfig.hasPath("networkRecoveryInterval")) 
      Duration(rmqConfig.getString("networkRecoveryInterval")).asInstanceOf[FiniteDuration]
    else
      Duration("10s").asInstanceOf[FiniteDuration]
  
  val automaticRecovery: Boolean = 
    if (rmqConfig.hasPath("automaticRecovery")) rmqConfig.getBoolean("automaticRecovery") else true
  
  val connectionRetries: Int = 
    if (rmqConfig.hasPath("connectionRetries")) rmqConfig.getInt("connectionRetries") else 3
  
  // Exchange configuration
  val exchanges: Map[String, ExchangeConfig] = {
    if (!rmqConfig.hasPath("exchanges")) {
      logger.warn("No exchanges configuration found, using minimal defaults")
    }
    
    val exchangesConfig = if (rmqConfig.hasPath("exchanges")) rmqConfig.getConfig("exchanges") else ConfigFactory.empty()
    
    // Ensure user_actions_exchange always exists
    val result = scala.collection.mutable.Map[String, ExchangeConfig]()
    
    // Add the critical exchange if not in config
    if (!exchangesConfig.hasPath("user_actions_exchange")) {
      result += ("user_actions_exchange" -> ExchangeConfig("user_actions_exchange", "topic", true, false, false))
    }
    
    // Add exchanges from config
    if (!exchangesConfig.isEmpty()) {
      exchangesConfig.root().entrySet().asScala.foreach { entry =>
        val name = entry.getKey
        val exchangeConfig = exchangesConfig.getConfig(name)
        result += (name -> ExchangeConfig(
          name = name,
          type_ = exchangeConfig.getString("type"),
          durable = exchangeConfig.getBoolean("durable"),
          autoDelete = if (exchangeConfig.hasPath("autoDelete")) exchangeConfig.getBoolean("autoDelete") else false,
          internal = if (exchangeConfig.hasPath("internal")) exchangeConfig.getBoolean("internal") else false
        ))
      }
    }
    
    result.toMap
  }
  
  // Helper method to parse duration strings
  private def parseDuration(durationStr: String): Long = {
    val duration = Duration(durationStr)
    duration.toMillis
  }

  // Queue configuration
  val queues: Map[String, QueueConfig] = {
    val result = scala.collection.mutable.Map[String, QueueConfig]()
    
    if (rmqConfig.hasPath("queues")) {
      val queuesConfig = rmqConfig.getConfig("queues")
      queuesConfig.root().entrySet().asScala.foreach { entry =>
        val name = entry.getKey
        val queueConfig = queuesConfig.getConfig(name)
        result += (name -> QueueConfig(
          name = name,
          durable = queueConfig.getBoolean("durable"),
          exclusive = if (queueConfig.hasPath("exclusive")) queueConfig.getBoolean("exclusive") else false,
          autoDelete = if (queueConfig.hasPath("autoDelete")) queueConfig.getBoolean("autoDelete") else false,
          ttl = if (queueConfig.hasPath("ttl")) Some(parseDuration(queueConfig.getString("ttl"))) else None,
          deadLetterEnabled = if (queueConfig.hasPath("deadLetterEnabled")) queueConfig.getBoolean("deadLetterEnabled") else true,
          maxPriority = if (queueConfig.hasPath("maxPriority")) Some(queueConfig.getInt("maxPriority")) else None
        ))
      }
    }
    
    // Create default configurations only when not present in config file
    val criticalQueues = List(
      (RabbitMQManager.Queues.PROCESSED_EVENTS, "24h"),
      (RabbitMQManager.Queues.RECOMMENDATIONS, "30m"),
      (RabbitMQManager.Queues.HISTORICAL_DATA, "7d")
    )
    
    criticalQueues.foreach { case (queueName, ttl) =>
      if (!result.contains(queueName)) {
        result += (queueName -> QueueConfig(
          name = queueName, 
          durable = true, 
          exclusive = false, 
          autoDelete = false, 
          ttl = Some(parseDuration(ttl)), 
          deadLetterEnabled = queueName != RabbitMQManager.Queues.HISTORICAL_DATA,
          maxPriority = None
        ))
      }
    }
    
    result.toMap
  }
  
  // Bindings configuration
  val bindings: List[BindingConfig] = 
    if (rmqConfig.hasPath("bindings")) {
      rmqConfig.getConfigList("bindings").asScala.map { bindingConfig =>
        BindingConfig(
          exchange = bindingConfig.getString("exchange"),
          queue = bindingConfig.getString("queue"),
          routingKey = bindingConfig.getString("routingKey")
        )
      }.toList
    } else {
      // Default bindings matching the simplified technical architecture
      List(
        // Client comportements et interactions -> Transformer
        BindingConfig("user_actions_exchange", "processed_events_queue", "event.processed.#"),
        
        // Recommender -> Client (recommendations)
        BindingConfig("user_actions_exchange", "recommendations_queue", "recommendation.result.#"),
        
        // Client -> DataManager (données historiques)
        BindingConfig("user_actions_exchange", "historical_data_queue", "historical.data.#")
      )
    }
  
  override def validate(): Map[String, String] = {
    val errors = scala.collection.mutable.Map[String, String]()
    
    if (host.isEmpty) {
      errors += ("rabbitmq.host" -> "RabbitMQ host cannot be empty")
    }
    
    if (port <= 0 || port > 65535) {
      errors += ("rabbitmq.port" -> s"Invalid port number: $port")
    }
    
    if (username.isEmpty) {
      errors += ("rabbitmq.username" -> "RabbitMQ username cannot be empty")
    }
    
    // Validate exchanges
    exchanges.foreach { case (name, exchange) =>
      val validTypes = Set("direct", "topic", "fanout", "headers")
      if (!validTypes.contains(exchange.type_)) {
        errors += (s"rabbitmq.exchanges.$name.type" -> 
                  s"Invalid exchange type: ${exchange.type_}. Must be one of: ${validTypes.mkString(", ")}")
      }
    }
    
    // Validate queues have unique names
    val queueNames = queues.values.map(_.name).toList
    queueNames.groupBy(identity).collect {
      case (name, occurrences) if occurrences.size > 1 =>
        errors += (s"rabbitmq.queues.$name" -> s"Duplicate queue name: $name")
    }
    
    // Validate bindings reference existing exchanges and queues
    bindings.foreach { binding =>
      if (!exchanges.contains(binding.exchange)) {
        errors += (s"rabbitmq.bindings.${binding.exchange}_${binding.queue}" -> 
                  s"Binding references non-existent exchange: ${binding.exchange}")
      }
      
      if (!queues.contains(binding.queue)) {
        errors += (s"rabbitmq.bindings.${binding.exchange}_${binding.queue}" -> 
                  s"Binding references non-existent queue: ${binding.queue}")
      }
    }
    
    errors.toMap
  }
}

/**
 * Exchange configuration
 */
case class ExchangeConfig(
  name: String,
  type_ : String,
  durable: Boolean,
  autoDelete: Boolean,
  internal: Boolean
)

/**
 * Queue configuration
 */
case class QueueConfig(
  name: String,
  durable: Boolean,
  exclusive: Boolean,
  autoDelete: Boolean,
  ttl: Option[Long],
  deadLetterEnabled: Boolean,
  maxPriority: Option[Int]
)

/**
 * Binding configuration
 */
case class BindingConfig(
  exchange: String,
  queue: String,
  routingKey: String
)

