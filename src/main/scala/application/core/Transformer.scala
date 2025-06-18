package application.core

import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.ConfigurationService
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback, ConfirmListener, AMQP}
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
class Transformer(dataManager: DataManager)(implicit ec: ExecutionContext) extends AutoCloseable {
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
  
  // Additional metrics from StreamingConsumer
  private val streamingMessagesProcessedCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "streaming", "processed"),
    "Total number of streaming messages successfully processed"
  )
  
  private val streamingMessageErrorsCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "streaming", "errors"),
    "Total number of streaming message processing errors"
  )
  
  // Register health checks
  registerHealthChecks()
  
  // Create RabbitMQ connections
  private var publishChannel: Channel = _
  private var consumeChannel: Channel = _
  private var consumerTag: String = _
  
  // Additional streaming consumer channel
  private var streamingConsumerChannel: Channel = _
  private var streamingConsumerTag: String = _
  
  // Initialize channels immediately
  private def initializeChannels(): Unit = {
    try {
      publishChannel = RabbitMQManager.createChannel("transformer-publisher")
      publishChannel.confirmSelect() // Enable publisher confirms
      publishChannel.addConfirmListener(confirmListener)
      
      consumeChannel = RabbitMQManager.createChannel("transformer-consumer")
      streamingConsumerChannel = RabbitMQManager.createChannel("streaming-consumer")
      
      logger.info("Transformer channels initialized successfully")
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize Transformer channels", e)
        throw e
    }
  }
  
  // Call initialization in constructor
  initializeChannels()
  
  // Maps for tracking published messages for publisher confirms
  private val outstandingConfirms = new ConcurrentSkipListMap[Long, ProcessedEvent]()
  
  // Confirm listener for publisher confirms
  private val confirmListener = new ConfirmListener {
    override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
      if (multiple) {
        // Remove all outstanding confirms up to and including this delivery tag
        val confirmedMessages = outstandingConfirms.headMap(deliveryTag + 1)
        confirmedMessages.clear()
      } else {
        // Remove only this specific delivery tag
        outstandingConfirms.remove(deliveryTag)
      }
      logger.debug(s"Message confirmed: deliveryTag=$deliveryTag, multiple=$multiple")
    }
    
    override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
      if (multiple) {
        val nackMessages = outstandingConfirms.headMap(deliveryTag + 1)
        nackMessages.values().forEach { event =>
          logger.warn(s"Message nacked for user ${event.userId}")
        }
        nackMessages.clear()
      } else {
        outstandingConfirms.get(deliveryTag) match {
          case event if event != null =>
            logger.warn(s"Message nacked for user ${event.userId}")
            outstandingConfirms.remove(deliveryTag)
          case _ =>
            logger.warn(s"Unknown message nacked: deliveryTag=$deliveryTag")
        }
      }
    }
  }

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
   * Check if publish channel is available and create if needed
   */
  private def ensurePublishChannelAvailable(): Unit = {
    if (publishChannel == null || !publishChannel.isOpen) {
      logger.warn("Publish channel is not available, recreating...")
      try {
        publishChannel = RabbitMQManager.createChannel("transformer-publisher")
        publishChannel.confirmSelect()
        publishChannel.addConfirmListener(confirmListener)
        logger.info("Publish channel recreated successfully")
      } catch {
        case e: Exception =>
          logger.error("Failed to recreate publish channel", e)
          throw e
      }
    }
  }

  /**
   * Set up RabbitMQ channel for publishing processed events
   */
  private def setupPublishChannel(): Unit = {
    publishChannel = RabbitMQManager.getChannel("transformer-publisher")
    
    // MODIFIER: Simplifier en utilisant seulement queueDeclarePassive
    try {
      publishChannel.queueDeclarePassive(RabbitMQManager.Queues.PROCESSED_EVENTS)
      logger.debug(s"Queue ${RabbitMQManager.Queues.PROCESSED_EVENTS} exists and is accessible")
    } catch {
      case e: Exception =>
        logger.error(s"Queue ${RabbitMQManager.Queues.PROCESSED_EVENTS} not found or not accessible", e)
        throw new RuntimeException(s"Required queue not available: ${RabbitMQManager.Queues.PROCESSED_EVENTS}", e)
    }
    
    // Configurer les propriétés du publisher
    publishChannel.addConfirmListener(confirmListener)
    publishChannel.confirmSelect()
  }

  /**
   * Start the transformer consumer to listen for user events.
   * This method now incorporates both the original Transformer consumer
   * and the StreamingConsumer functionality.
   */
  def start(): Unit = {
    logger.info("Starting Transformer consumer...")
    
    // Start the original transformer consumer
    startOriginalConsumer()
    
    // Start the streaming consumer (previously in StreamingConsumer)
    startStreamingConsumer()
    
    logger.info("Transformer started with both consumers")
  }
  
  /**
   * Start the original transformer consumer
   */
  private def startOriginalConsumer(): Unit = {
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
    logger.info(s"Original transformer consumer started with tag: $consumerTag")
  }
  
  /**
   * Start the streaming consumer (formerly in StreamingConsumer class)
   */
  private def startStreamingConsumer(): Unit = {
    try {
      logger.info("Starting streaming consumer...")
      
      // Create a dedicated channel for this consumer
      streamingConsumerChannel = RabbitMQManager.createChannel("streaming-consumer")
      
      // Declare the topic exchange
      val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
      streamingConsumerChannel.exchangeDeclare(exchange, "topic", true)
      
      // Declare a temporary queue for the consumer
      val queueName = streamingConsumerChannel.queueDeclare().getQueue() // Anonymous temporary queue
      
      logger.info(s"Using streaming consumer queue: $queueName")
      
      // Bind queue to exchange with routing patterns for user events
      streamingConsumerChannel.queueBind(queueName, exchange, RabbitMQManager.RoutingKeys.USER_ACTIONS)
      
      // Set QoS for backpressure control - only process a limited number at a time
      streamingConsumerChannel.basicQos(10) 
      
      val deliverCallback = new DeliverCallback {
        override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
          val message = new String(delivery.getBody, StandardCharsets.UTF_8)
          val routingKey = delivery.getEnvelope.getRoutingKey
          
          logger.debug(s"Received streaming message with routing key [$routingKey]: ${message.take(100)}...")
          messagesReceivedCounter.inc()
          
          try {
            decode[UserEvent](message) match {
              case Right(event) =>
                logger.info(s"Processing streaming event [${event.eventType}] for user ${event.userId}")
                
                // Process the event using the transformer
                processEvent(event).onComplete {
                  case scala.util.Success(_) => 
                    logger.debug(s"Successfully processed streaming event for user ${event.userId}")
                    streamingConsumerChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                    streamingMessagesProcessedCounter.inc()
                    
                  case scala.util.Failure(exception) =>
                    logger.error(s"Failed to process streaming event for user ${event.userId}: ${exception.getMessage}", exception)
                    // Detect permanent failures vs. retriable failures
                    val isPermanentFailure = exception match {
                      case _: IllegalArgumentException => true
                      case _: java.lang.IllegalStateException => true
                      case _ => false
                    }
                    
                    // Don't requeue permanent failures
                    streamingConsumerChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, !isPermanentFailure)
                    streamingMessageErrorsCounter.inc()
                }
              
              case Left(error) =>
                logger.error(s"Failed to parse streaming message as UserEvent: ${error.getMessage}")
                // Don't requeue parsing errors - they won't succeed on retry
                streamingConsumerChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                streamingMessageErrorsCounter.inc()
            }
          } catch {
            case e: Exception =>
              logger.error(s"Unexpected error processing streaming message: ${e.getMessage}", e)
              // Requeue on unexpected errors as they might be temporary
              streamingConsumerChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
              streamingMessageErrorsCounter.inc()
          }
        }
      }
      
      val cancelCallback = new CancelCallback {
        override def handle(consumerTag: String): Unit = {
          logger.warn(s"Streaming consumer canceled: $consumerTag")
          
          // Attempt to restart consumer after delay if service is still running
          new Thread(() => {
            try {
              logger.info("Attempting to restart streaming consumer in 5 seconds...")
              Thread.sleep(5000)
              startStreamingConsumer()
            } catch {
              case e: Exception =>
                logger.error(s"Failed to restart streaming consumer: ${e.getMessage}", e)
            }
          }).start()
        }
      }
      
      // Start consuming with manual ack mode
      streamingConsumerTag = streamingConsumerChannel.basicConsume(
        queueName,  // Use the temporary queue created
        false, // manual ack
        deliverCallback,
        cancelCallback
      )
      logger.info(s"Streaming consumer started with tag $streamingConsumerTag")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to start streaming consumer: ${e.getMessage}", e)
        throw e
    }
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
        publishRecommendationRequest(RecommendationRequest(userEvent.userId, Some(userEvent.itemId.get)))
      }
      
      val processingTime = (System.nanoTime() - startTime) / 1000000 // Convert to milliseconds
      messageProcessingTimer.recordMillis(processingTime)
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
      timestamp = Instant.now().toEpochMilli(),
      properties = userEvent.properties.getOrElse(Map.empty)
    )
  }
  
  /**
   * Publish a processed event to the processed_events_queue
   */
  private def publishProcessedEvent(processedEvent: ProcessedEvent): Unit = {
    ensurePublishChannelAvailable()
    
    try {
      val message = processedEvent.asJson.noSpaces
      val props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(2) // persistent
        .build()
      
      val deliveryTag = publishChannel.getNextPublishSeqNo()
      outstandingConfirms.put(deliveryTag, processedEvent)
      
      publishChannel.basicPublish(
        RabbitMQManager.Exchanges.USER_ACTIONS,
        "event.processed.user",
        props,
        message.getBytes(StandardCharsets.UTF_8)
      )
      
      logger.debug(s"Published processed event for user ${processedEvent.userId}")
    } catch {
      case e: Exception =>
        logger.error("Failed to publish processed event", e)
        throw e
    }
  }
  
  /**
   * Publish a recommendation request
   */
  def publishRecommendationRequest(request: RecommendationRequest): Unit = {
    try {
      ensurePublishChannelAvailable()
      
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
    val received = messagesReceivedCounter.get()
    val processed = messageProcessedCounter.get() + streamingMessagesProcessedCounter.get()
    val errors = errorCounter.get() + streamingMessageErrorsCounter.get()
    val successRate = if (received > 0) (processed.toDouble / received.toDouble) * 100.0 else 0.0
    
    s"""Transformer Metrics:
       |  Total events processed: $totalProcessed
       |  Average processing time: ${avgTime}ms
       |  Min processing time: ${if (minProcessingTime == Long.MaxValue) 0 else minProcessingTime}ms
       |  Max processing time: ${maxProcessingTime}ms
       |  
       |  Messages received: $received
       |  Messages processed: $processed
       |  Processing errors: $errors
       |  Success rate: $successRate%.2f%%
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
  override def close(): Unit = {
    logger.info("Closing Transformer resources...")
    
    // Close original consumer channel
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
    
    // Close streaming consumer channel
    if (streamingConsumerChannel != null && streamingConsumerChannel.isOpen) {
      try {
        if (streamingConsumerTag != null) {
          logger.info(s"Canceling streaming consumer with tag $streamingConsumerTag")
          streamingConsumerChannel.basicCancel(streamingConsumerTag)
        }
        
        // Wait a moment for in-flight processing to complete
        Thread.sleep(200)
        
        // Close the channel
        if (streamingConsumerChannel.isOpen) {
          logger.info("Closing streaming consumer channel")
          RabbitMQManager.closeChannel("streaming-consumer")
        }
      } catch {
        case e: Exception => 
          logger.error(s"Error closing streaming consumer: ${e.getMessage}", e)
      } finally {
        streamingConsumerTag = null
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

  /**
   * Initialize the transformer - useful for tests
   */
  def initialize(): Unit = {
    try {
      logger.info("Initializing transformer")
      // Si les canaux ne sont pas initialisés, les initialiser
      if (publishChannel == null || !publishChannel.isOpen) {
        // Logique d'initialisation des canaux si nécessaire
        logger.info("Transformer channels initialized")
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize transformer", e)
        throw e
    }
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
