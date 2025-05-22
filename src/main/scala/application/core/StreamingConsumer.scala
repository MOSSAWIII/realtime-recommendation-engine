package application.core

import domain.models.UserEvent
import infrastructure.messaging.rabbitmq.RabbitMQManager
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import io.circe.parser.decode
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory
import utils.metrics.MetricsRegistry

/**
 * Consumes user events directly from the user_actions_exchange and forwards them to the Transformer
 * for processing. Implements a direct flow pattern according to the architectural diagram.
 */
class StreamingConsumer(val transformer: Transformer)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "streaming-consumer"
  private var consumerTag: String = _
  private var channel: Channel = _
  
  // Define metrics
  private val messagesReceivedCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "messages", "received"),
    "Total number of messages received"
  )
  
  private val messagesProcessedCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "messages", "processed"),
    "Total number of messages successfully processed"
  )
  
  private val messageErrorsCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "messages", "errors"),
    "Total number of message processing errors"
  )
  
  /**
   * Starts the consumer to listen for events from the user_actions_exchange
   */
  def start(): Unit = {
    try {
      logger.info("Starting StreamingConsumer...")
      
      // Create a dedicated channel for this consumer
      channel = RabbitMQManager.createChannel("streaming-consumer")
      
      // Declare the topic exchange
      val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
      channel.exchangeDeclare(exchange, "topic", true)
      
      // Declare a temporary queue for the consumer
      val queueName = channel.queueDeclare().getQueue() // Anonymous temporary queue
      
      logger.info(s"Using consumer queue: $queueName")
      
      // Bind queue to exchange with routing patterns for user events
      channel.queueBind(queueName, exchange, RabbitMQManager.RoutingKeys.USER_ACTIONS)
      
      // Set QoS for backpressure control - only process a limited number at a time
      channel.basicQos(10) 
      
      val deliverCallback = new DeliverCallback {
        override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
          val message = new String(delivery.getBody, StandardCharsets.UTF_8)
          val routingKey = delivery.getEnvelope.getRoutingKey
          
          logger.debug(s"Received message with routing key [$routingKey]: ${message.take(100)}...")
          messagesReceivedCounter.inc()
          
          try {
            decode[UserEvent](message) match {
              case Right(event) =>
                logger.info(s"Processing event [${event.eventType}] for user ${event.userId}")
                
                // Process the event using the transformer
                transformer.processEvent(event).onComplete {
                  case scala.util.Success(_) => 
                    logger.debug(s"Successfully processed event for user ${event.userId}")
                    channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                    messagesProcessedCounter.inc()
                    
                  case scala.util.Failure(exception) =>
                    logger.error(s"Failed to process event for user ${event.userId}: ${exception.getMessage}", exception)
                    // Detect permanent failures vs. retriable failures
                    val isPermanentFailure = exception match {
                      case _: IllegalArgumentException => true
                      case _: java.lang.IllegalStateException => true
                      case _ => false
                    }
                    
                    // Don't requeue permanent failures
                    channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, !isPermanentFailure)
                    messageErrorsCounter.inc()
                }
              
              case Left(error) =>
                logger.error(s"Failed to parse message as UserEvent: ${error.getMessage}")
                // Don't requeue parsing errors - they won't succeed on retry
                channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                messageErrorsCounter.inc()
            }
          } catch {
            case e: Exception =>
              logger.error(s"Unexpected error processing message: ${e.getMessage}", e)
              // Requeue on unexpected errors as they might be temporary
              channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
              messageErrorsCounter.inc()
          }
        }
      }
      
      val cancelCallback = new CancelCallback {
        override def handle(consumerTag: String): Unit = {
          logger.warn(s"Consumer canceled: $consumerTag")
          
          // Attempt to restart consumer after delay if service is still running
          new Thread(() => {
            try {
              logger.info("Attempting to restart consumer in 5 seconds...")
              Thread.sleep(5000)
              start()
            } catch {
              case e: Exception =>
                logger.error(s"Failed to restart consumer: ${e.getMessage}", e)
            }
          }).start()
        }
      }
      
      // Start consuming with manual ack mode
      consumerTag = channel.basicConsume(
        queueName,  // Use the temporary queue created
        false, // manual ack
        deliverCallback,
        cancelCallback
      )
      logger.info(s"StreamingConsumer started with tag $consumerTag")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to start consumer: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Process a single event (mainly used for testing)
   */
  def processEvent(event: UserEvent): Future[Unit] = {
    transformer.processEvent(event).map(_ => ())
  }
  
  /**
   * Gracefully closes the consumer
   */
  def close(): Unit = {
    if (channel != null && channel.isOpen && consumerTag != null) {
      try {
        logger.info(s"Canceling consumer with tag $consumerTag")
        channel.basicCancel(consumerTag)
        
        // Wait a moment for in-flight processing to complete
        Thread.sleep(200)
        
        // Close the channel
        RabbitMQManager.closeChannel("streaming-consumer")
        logger.info("StreamingConsumer closed")
      } catch {
        case e: Exception => 
          logger.error(s"Error closing consumer: ${e.getMessage}", e)
      }
    }
  }
  
  /**
   * Get metrics information 
   */
  def getMetrics: String = {
    val received = messagesReceivedCounter.get()
    val processed = messagesProcessedCounter.get()
    val errors = messageErrorsCounter.get()
    val successRate = if (received > 0) (processed.toDouble / received.toDouble) * 100.0 else 0.0
    
    f"""StreamingConsumer Metrics:
       |  Messages received: $received
       |  Messages processed: $processed
       |  Processing errors: $errors
       |  Success rate: $successRate%.2f%%
       |""".stripMargin
  }
}
