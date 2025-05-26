package application.core

import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.ConfigurationService
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback, ConfirmCallback, AMQP}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import utils.metrics.{MetricsRegistry, HealthCheckRegistry, SystemStatus, HealthStatus, HealthCheckResult}
import java.time.Instant
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Transformer component that processes user events and publishes them to appropriate queues.
 * Handles both consuming events from user_actions_exchange and publishing to processed_events_queue.
 */
class Transformer(dataManager: DataManager)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "transformer"
  
  // Metrics tracking
  private var totalProcessed: Long = 0
  private var totalProcessingTime: Long = 0
  private var minProcessingTime: Long = Long.MaxValue
  private var maxProcessingTime: Long = 0
  
  // Define metrics
  private val messageProcessingTimer = MetricsRegistry.buildTimer(
    MetricsRegistry.MetricMetadata(componentName, "processing", "duration"),
    "Duration of message processing",
    "milliseconds"
  )
  
  private val messageProcessedCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "messages", "processed"),
    "Total number of messages processed"
  )
  
  private val errorCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "errors", "count"),
    "Total number of processing errors"
  )
  
  // Consumer metrics
  private val messagesReceivedCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "messages", "received"),
    "Total number of messages received"
  )
  
  // Register health checks
  registerHealthChecks()
  
  // Create RabbitMQ connections
  private val publishChannel = setupPublishChannel()
  private var consumeChannel: Channel = _
  private var consumerTag: String = _
  
  // Maps for tracking published messages for publisher confirms
  private val outstandingConfirms = new ConcurrentSkipListMap[Long, ProcessedEvent]()

  /**
   * Register health check function for the transformer component
   */
  private def registerHealthChecks(): Unit = {
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck(componentName, "processing") {
        val recentErrors = errorCounter.get()
        val processed = messageProcessedCounter.get()
        val errorRate = if (processed > 0) (recentErrors * 100.0) / processed else 0.0
        
        val status = errorRate match {
          case rate if rate > 10.0 => HealthStatus.Unhealthy
          case rate if rate > 5.0 => HealthStatus.Degraded
          case _ => HealthStatus.Healthy
        }
        
        HealthCheckResult(
          component = componentName,
          check = "processing",
          status = status,
          message = s"Transformer error rate: $errorRate%",
          details = Map(
            "processedMessages" -> processed.toString,
            "errors" -> recentErrors.toString,
            "avgProcessingTime" -> messageProcessingTimer.getAvg().toString
          )
        )
      }
    )
  }

  /**
   * Set up RabbitMQ channel for publishing processed events
   */
  private def setupPublishChannel(): Channel = {
    val channel = RabbitMQManager.createChannel("transformer-publisher")
    
    // Enable publisher confirms for reliability
    channel.confirmSelect()
    
    // Set up topic exchange
    val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
    channel.exchangeDeclare(exchange, "topic", true)
    
    // Declare the processed events queue
    channel.queueDeclare(
      RabbitMQManager.Queues.PROCESSED_EVENTS, 
      true,  // durable
      false, // exclusive
      false, // autoDelete
      null   // arguments
    )
    
    // Bind queue to the exchange with appropriate routing key
    channel.queueBind(
      RabbitMQManager.Queues.PROCESSED_EVENTS, 
      exchange, 
      RabbitMQManager.RoutingKeys.PROCESSED_EVENTS
    )
    
    // Set up publisher confirms callbacks
    val confirmCallback = new ConfirmCallback() {
      override def handle(sequenceNumber: Long, multiple: Boolean): Unit = {
        if (multiple) {
          val confirmed = outstandingConfirms.headMap(sequenceNumber + 1)
          logger.debug(s"Batch confirmed up to message: $sequenceNumber (${confirmed.size()} messages)")
          confirmed.clear()
        } else {
          outstandingConfirms.remove(sequenceNumber)
        }
      }
    }
    
    val nackCallback = new ConfirmCallback() {
      override def handle(sequenceNumber: Long, multiple: Boolean): Unit = {
        if (multiple) {
          val nacked = outstandingConfirms.headMap(sequenceNumber + 1)
          logger.error(s"Batch nacked up to message: $sequenceNumber (${nacked.size()} messages)")
          nacked.clear()
        } else {
          outstandingConfirms.remove(sequenceNumber)
        }
        errorCounter.inc()
      }
    }
    
    channel.addConfirmListener(confirmCallback, nackCallback)
    channel
  }

  /**
   * Start the transformer consumer to listen for user events
   */
  def start(): Unit = {
    logger.info("Starting Transformer consumer...")
    
    // Create a consumer channel
    consumeChannel = RabbitMQManager.createChannel("transformer-consumer")
    
    // Set up topic exchange
    val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
    consumeChannel.exchangeDeclare(exchange, "topic", true)
    
    // Create a queue for consuming user actions
    val queueName = consumeChannel.queueDeclare().getQueue()
    
    // Bind the queue to the exchange
    consumeChannel.queueBind(queueName, exchange, RabbitMQManager.RoutingKeys.USER_ACTIONS)
    
    // Create a consumer
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        messagesReceivedCounter.inc()
        
        try {
          // Parse the message as a UserEvent
          decode[UserEvent](message) match {
            case Right(userEvent) =>
              processEvent(userEvent).onComplete {
                case Success(_) => 
                  consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                  messageProcessedCounter.inc()
                case Failure(e) => 
                  logger.error(s"Failed to process message: ${e.getMessage}", e)
                  consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                  errorCounter.inc()
              }
            case Left(error) =>
              logger.error(s"Failed to parse message as UserEvent: ${error.getMessage}")
              consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
              errorCounter.inc()
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error processing received message: ${e.getMessage}", e)
            consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
            errorCounter.inc()
        }
      }
    }
    
    // Cancel callback is a no-op
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer was cancelled: $consumerTag")
      }
    }
    
    // Start consuming
    consumerTag = consumeChannel.basicConsume(queueName, false, deliverCallback, cancelCallback)
    logger.info(s"Transformer consumer started with tag: $consumerTag")
  }

  /**
   * Process a user event by transforming it to a processed event
   * and publishing it to the processed_events_queue
   *
   * @param userEvent The raw user event to process
   * @return A future containing the processed event
   */
  def processEvent(userEvent: UserEvent): Future[ProcessedEvent] = {
    val startTime = System.nanoTime()
    
    try {
      // Transform the event
      val processedEvent = transformEvent(userEvent)
      
      // Publish to the queue
      publishProcessedEvent(processedEvent)
      
      // If this is an event with an item, trigger recommendation
      if (userEvent.itemId.isDefined) {
        publishRecommendationRequest(RecommendationRequest(userEvent.userId, userEvent.itemId.get))
      }
      
      val processingTime = (System.nanoTime() - startTime) / 1000000 // Convert to milliseconds
      messageProcessingTimer.record(processingTime)
      updateMetrics(processingTime)
      
      Future.successful(processedEvent)
    } catch {
      case e: Exception =>
        logger.error(s"Error processing event: ${e.getMessage}", e)
        errorCounter.inc()
        Future.failed(e)
    }
  }

  /**
   * Transform a user event into a processed event
   */
  def transformEvent(userEvent: UserEvent): ProcessedEvent = {
    ProcessedEvent(
      userId = userEvent.userId,
      eventType = userEvent.eventType,
      itemId = userEvent.itemId,
      timestamp = Instant.now(),
      properties = userEvent.properties.getOrElse(Map.empty)
    )
  }
  
  /**
   * Publish a processed event to the processed_events_queue
   */
  def publishProcessedEvent(event: ProcessedEvent): Unit = {
    try {
      val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
      val routingKey = s"event.processed.${event.eventType}"
      
      val props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .build()
      
      val message = event.asJson.noSpaces
      
      // Publish the message
      val seq = publishChannel.getNextPublishSeqNo()
      outstandingConfirms.put(seq, event)
      
      publishChannel.basicPublish(
        exchange,
        routingKey,
        props,
        message.getBytes(StandardCharsets.UTF_8)
      )
      
      logger.debug(s"Published processed event for user ${event.userId}")
    } catch {
      case e: Exception => 
        logger.error(s"Failed to publish processed event: ${e.getMessage}", e)
        errorCounter.inc()
    }
  }
  
  /**
   * Publish a recommendation request
   */
  def publishRecommendationRequest(request: RecommendationRequest): Unit = {
    try {
      val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
      val routingKey = s"recommendation.request.${request.userId}"
      
      val props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .build()
      
      val message = request.asJson.noSpaces
      
      publishChannel.basicPublish(
        exchange,
        routingKey,
        props,
        message.getBytes(StandardCharsets.UTF_8)
      )
      
      logger.debug(s"Published recommendation request for user ${request.userId}")
    } catch {
      case e: Exception => 
        logger.error(s"Failed to publish recommendation request: ${e.getMessage}", e)
        errorCounter.inc()
    }
  }

  /**
   * Update metrics with processing time
   */
  private def updateMetrics(processingTime: Long): Unit = {
    this.synchronized {
      totalProcessed += 1
      totalProcessingTime += processingTime
      minProcessingTime = Math.min(minProcessingTime, processingTime)
      maxProcessingTime = Math.max(maxProcessingTime, processingTime)
    }
  }
  
  /**
   * Get current metrics as a formatted string
   */
  def getMetrics: String = {
    val avgTime = if (totalProcessed > 0) totalProcessingTime / totalProcessed else 0
    s"""Transformer Metrics:
       |  Total events processed: $totalProcessed
       |  Average processing time: ${avgTime}ms
       |  Min processing time: ${if (minProcessingTime == Long.MaxValue) 0 else minProcessingTime}ms
       |  Max processing time: ${maxProcessingTime}ms
       |""".stripMargin
  }
  
  /**
   * Reset metrics counters
   */
  def resetMetrics(): Unit = {
    this.synchronized {
      totalProcessed = 0
      totalProcessingTime = 0
      minProcessingTime = Long.MaxValue
      maxProcessingTime = 0
    }
  }
  
  /**
   * Close resources when shutting down
   */
  def close(): Unit = {
    logger.info("Closing Transformer resources...")
    
    // Close consumer channel
    if (consumeChannel != null && consumeChannel.isOpen) {
      try {
        if (consumerTag != null) {
          consumeChannel.basicCancel(consumerTag)
        }
        RabbitMQManager.closeChannel("transformer-consumer")
      } catch {
        case e: Exception => logger.warn(s"Error closing consumer channel: ${e.getMessage}")
      }
    }
    
    // Close publisher channel
    if (publishChannel != null && publishChannel.isOpen) {
      try {
        if (!outstandingConfirms.isEmpty) {
          logger.info(s"Waiting for ${outstandingConfirms.size()} outstanding confirms before closing")
          publishChannel.waitForConfirmsOrDie(5000)
        }
      } catch {
        case e: Exception => logger.warn(s"Error waiting for confirms: ${e.getMessage}")
      }
      
      RabbitMQManager.closeChannel("transformer-publisher")
    }
    
    logger.info("Transformer closed")
  }
}

/**
 * Companion object to create transformer instances
 */
object Transformer {
  def apply(dataManager: DataManager)(implicit ec: ExecutionContext): Transformer = {
    val transformer = new Transformer(dataManager)
    transformer.start()
    transformer
  }
}
