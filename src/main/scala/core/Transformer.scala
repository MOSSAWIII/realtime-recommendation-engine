package core

import models._
import repositories.RepositoryInterface
import config.RabbitMQManager
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import org.slf4j.LoggerFactory
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.Channel  // Add this import

// Modifier la signature du constructeur
class Transformer(repository: RepositoryInterface, channel: Channel = RabbitMQManager.createChannel("transformer"))(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Ensure queue exists
  channel.queueDeclare(
    RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, 
    true, false, false, null
  )
  
  def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    logger.debug(s"Traitement de l'événement: $event")
    
    // 1. Transformation de l'événement
    val processedEvent = transformEvent(event)
    
    // 2. Stockage dans la base de données
    repository.storeProcessedEvent(processedEvent)
      .map { storedEvent =>
        // 3. Publier une demande de recommandation asynchrone via RabbitMQ
        // au lieu d'appeler directement le Recommender
        val recommendationRequest = RecommendationRequest(
          userId = event.userId,
          timestamp = System.currentTimeMillis()
        )
        
        val requestJson = recommendationRequest.asJson.noSpaces
        
        channel.basicPublish(
          "",  // échange par défaut
          RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
          null,
          requestJson.getBytes(StandardCharsets.UTF_8)
        )
        
        logger.info(s"Demande de recommandation publiée pour l'utilisateur ${event.userId}")
        storedEvent
      }
      .recoverWith { case e: Exception =>
        logger.error(s"Erreur lors du traitement de l'événement: ${e.getMessage}", e)
        Future.failed(e)
      }
  }
  
  private def transformEvent(event: UserEvent): ProcessedEvent = {
    // Transformation simple: nettoyage et enrichissement des données
    val cleanedProperties = event.properties.map {
      case (key, value) => (key.toLowerCase.trim, value.trim)
    }.filter {
      case (key, value) => key.nonEmpty && value.nonEmpty
    }
    
    // Ajout de propriétés supplémentaires si nécessaire
    val enrichedProperties = cleanedProperties + ("processed_at" -> System.currentTimeMillis().toString)
    
    ProcessedEvent(
      userId = event.userId,
      eventType = event.eventType,
      itemId = event.itemId,
      properties = enrichedProperties,
      timestamp = event.timestamp,
      processingTimestamp = System.currentTimeMillis()
    )
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("transformer")
  }
}