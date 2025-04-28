package core

import models._
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import config.RabbitMQManager
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext
import org.slf4j.LoggerFactory

class StreamingConsumer(val transformer: Transformer)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val channel = RabbitMQManager.createChannel("streaming-consumer")
  
  def start(): Unit = {
    logger.info("Starting user event consumer...")
    
    channel.queueDeclare(RabbitMQManager.Queues.USER_EVENTS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        decode[UserEvent](message) match {
          case Right(event) =>
            logger.debug(s"Received user event: $event")
            processEvent(event)
          case Left(error) =>
            logger.error(s"Failed to decode user event: ${error.getMessage}")
        }
        
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        logger.warn(s"User event consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume(
      RabbitMQManager.Queues.USER_EVENTS, 
      false,  // manual ack
      deliverCallback, 
      cancelCallback
    )
    
    logger.info("User event consumer started successfully")
  }
  
  def processEvent(event: UserEvent): Unit = {
    try {
      transformer.processEvent(event).foreach { processedEvent =>
        logger.info(s"Processed event for user ${event.userId}, item ${event.itemId.getOrElse("N/A")}")
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error processing event for user ${event.userId}: ${ex.getMessage}", ex)
    }
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("streaming-consumer")
    logger.info("StreamingConsumer closed")
  }
}
