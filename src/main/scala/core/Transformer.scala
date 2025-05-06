package core

import models._
import config.RabbitMQManager
import repositories.Repository
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory

class Transformer(val repository: Repository)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val channel = RabbitMQManager.createChannel("transformer-publisher")
  
  // Ensure queues exist
  channel.queueDeclare(
    RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, 
    true, false, false, null
  )
  
  // Add declaration for processed_events queue
  channel.queueDeclare(
    "processed_events", 
    true, false, false, null
  )
  
  def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    logger.info(s"Processing event for user ${event.userId}")
    
    // Convert to ProcessedEvent
    val processedEvent = ProcessedEvent(
      id = None,
      userId = event.userId,
      eventType = event.eventType,
      itemId = event.itemId,
      properties = event.properties.getOrElse(Map.empty),
      timestamp = event.timestamp,
      processingTimestamp = System.currentTimeMillis()
    )
    
    // Store in repository
    val future = repository.storeProcessedEvent(processedEvent)
    
    // Publish the processed event to processed_events queue for testing/monitoring
    future.foreach { storedEvent =>
      try {
        val message = storedEvent.asJson.noSpaces
        channel.basicPublish(
          "",  // Default exchange
          "processed_events",
          null, // No properties
          message.getBytes(StandardCharsets.UTF_8)
        )
        logger.debug(s"Published processed event for user ${storedEvent.userId} to processed_events queue")
      } catch {
        case e: Exception => 
          logger.error(s"Failed to publish processed event: ${e.getMessage}", e)
      }
      
      // If there's an item ID, trigger a recommendation request
      if (storedEvent.itemId.isDefined) {
        val recommendationRequest = RecommendationRequest(
          userId = storedEvent.userId,
          timestamp = System.currentTimeMillis()
        )
        publishRecommendationRequest(recommendationRequest)
      }
    }
    
    future
  }

  protected def publishRecommendationRequest(request: RecommendationRequest): Unit = {
    try {
      val message = request.asJson.noSpaces
      channel.basicPublish(
        "",  // Default exchange
        RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
        null, // No properties
        message.getBytes(StandardCharsets.UTF_8)
      )
      logger.debug(s"Published recommendation request for user ${request.userId}")
    } catch {
      case e: Exception => 
        logger.error(s"Failed to publish recommendation request: ${e.getMessage}", e)
    }
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("transformer-publisher")
    logger.info("Transformer closed")
  }
}