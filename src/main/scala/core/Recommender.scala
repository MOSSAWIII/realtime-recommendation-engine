package core

import models._
import config.RabbitMQManager
import repositories.Repository
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

class Recommender(val repository: Repository)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val consumeChannel = RabbitMQManager.createChannel("recommender-consumer")
  private val publishChannel = RabbitMQManager.createChannel("recommender-publisher")
  
  // Configure queues
  consumeChannel.queueDeclare(
    RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, 
    true, false, false, null
  )
  publishChannel.queueDeclare(
    RabbitMQManager.Queues.RECOMMENDATIONS, 
    true, false, false, null
  )
  
  def start(): Unit = {
    logger.info("Starting recommendation service...")
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        decode[RecommendationRequest](message) match {
          case Right(request) =>
            logger.debug(s"Received recommendation request for user ${request.userId}")
            
            try {
              // Generate recommendations
              val recommendationsFuture = generateRecommendations(request)
              
              // Use Await since we're inside a callback
              val recommendations = Await.result(recommendationsFuture, 10.seconds)
              
              // Publish recommendations to the output queue
              val json = recommendations.asJson.noSpaces
              publishChannel.basicPublish(
                "",  // Default exchange
                RabbitMQManager.Queues.RECOMMENDATIONS,
                null, // Default properties
                json.getBytes(StandardCharsets.UTF_8)
              )
              logger.info(s"Published recommendations for user ${request.userId}")
            } catch {
              case ex: Exception =>
                logger.error(s"Failed to generate recommendations for user ${request.userId}: ${ex.getMessage}", ex)
            }
            
          case Left(error) =>
            logger.error(s"Failed to decode recommendation request: ${error.getMessage}")
        }
        
        // Acknowledge the message
        consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Recommendation consumer cancelled: $consumerTag")
      }
    }
    
    // Start consuming
    consumeChannel.basicConsume(
      RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
      false,  // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info("Recommendation service started")
  }
  
  def generateRecommendations(request: RecommendationRequest): Future[Recommendation] = {
    // Get user's interaction history
    repository.getProcessedEventsForUser(request.userId).map { events =>
      // Simple algorithm: recommend items the user has interacted with recently
      // In a real system, you'd use a more sophisticated algorithm
      val recommendedItems = events
        .filter(_.itemId.isDefined)
        .groupBy(_.itemId.get)
        .map { case (itemId, itemEvents) =>
          // Calculate a score based on recency and frequency
          val score = itemEvents.size * 10.0 + 
                     (if (itemEvents.exists(_.timestamp > System.currentTimeMillis() - 86400000)) 50.0 else 0.0)
          
          RecommendedItem(
            itemId = itemId, 
            score = score,
            reason = "Based on your recent activity"
          )
        }
        .toList
        .sortBy(-_.score)
        .take(5)  // Limit to 5 recommendations
      
      // Create recommendation object
      val recommendation = Recommendation(
        userId = request.userId,
        recommendedItems = recommendedItems,
        score = recommendedItems.map(_.score).sum / math.max(1, recommendedItems.size),
        timestamp = System.currentTimeMillis()
      )
      
      // Store recommendation in repository
      repository.storeRecommendation(recommendation)
        .foreach(_ => logger.info(s"Stored recommendation for user ${request.userId}"))
      
      recommendation
    }
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("recommender-consumer")
    RabbitMQManager.closeChannel("recommender-publisher")
    logger.info("Recommender closed")
  }
}