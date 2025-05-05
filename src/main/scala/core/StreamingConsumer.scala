package core

import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import config.RabbitMQManager
import io.circe.parser._
import io.circe.generic.auto._  // Add this import for automatic decoder derivation
import models.{UserEvent, ProcessedEvent}
import java.nio.charset.StandardCharsets
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}

/**
 * Consumer for user interaction events
 * Connects to RabbitMQ and processes incoming events
 */
class StreamingConsumer(transformer: Transformer)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val channel = RabbitMQManager.createChannel("streaming-consumer")
  
  // Configure queue
  channel.queueDeclare(RabbitMQManager.Queues.USER_EVENTS, true, false, false, null)
  
  def start(): Unit = {
    logger.info("Starting streaming consumer...")
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received message: $message")
        
        decode[UserEvent](message) match {
          case Right(event) =>
            processEvent(event)
            logger.debug(s"Successfully processed event for user ${event.userId}")
          case Left(error) =>
            logger.error(s"Failed to decode message: ${error.getMessage}", error)
        }
        
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer cancelled: $consumerTag")
      }
    }
    
    channel.basicConsume(
      RabbitMQManager.Queues.USER_EVENTS, 
      false, 
      deliverCallback, 
      cancelCallback
    )
    
    logger.info("Streaming consumer started")
  }
  
  def processEvent(event: UserEvent): Unit = {
    transformer.processEvent(event).onComplete {
      case Success(result) => 
        logger.debug(s"Event processed successfully: $result")
      case Failure(ex) =>
        logger.error(s"Failed to process event: ${ex.getMessage}", ex)
    }
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("streaming-consumer")
    logger.info("StreamingConsumer closed")
  }
}
