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
  
  // Ensure queue exists
  channel.queueDeclare(
    RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, 
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
    
    future.foreach { savedEvent =>
      // Only trigger recommendations for events with item IDs
      if (savedEvent.itemId.isDefined) {
        // Send to recommendation service
        val request = RecommendationRequest(
          userId = savedEvent.userId,
          timestamp = System.currentTimeMillis()
        )
        publishRecommendationRequest(request)
      }
    }
    
    future
  }
  
  protected def publishRecommendationRequest(request: RecommendationRequest): Unit = {
    try {
      val jsonPayload = request.asJson.noSpaces
      channel.basicPublish(
        "",  // Default exchange
        RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
        null, // Default properties
        jsonPayload.getBytes(StandardCharsets.UTF_8)
      )
      logger.debug(s"Published recommendation request for user ${request.userId}")
    } catch {
      case ex: Exception => 
        logger.error(s"Failed to publish recommendation request: ${ex.getMessage}", ex)
    }
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("transformer-publisher")
    logger.info("Transformer closed")
  }
}