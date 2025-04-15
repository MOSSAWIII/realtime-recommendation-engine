package core

import models._
import repositories.Repository
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import org.slf4j.LoggerFactory

class Transformer(repository: Repository, recommender: Recommender)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    logger.debug(s"Traitement de l'événement: $event")
    
    // 1. Transformation de l'événement
    val processedEvent = transformEvent(event)
    
    // 2. Stockage dans la base de données
    repository.storeProcessedEvent(processedEvent)
      .flatMap { storedEvent =>
        // 3. Déclenchement du processus de recommandation
        val recommendationFuture = recommender.generateRecommendationsForUser(event.userId)
          .recover { case e: Exception =>
            logger.error(s"Échec de génération des recommandations: ${e.getMessage}", e)
            throw e
          }
        
        recommendationFuture.onComplete {
          case Success(recommendation) => 
            logger.info(s"Nouvelles recommandations générées pour l'utilisateur ${event.userId}")
          case Failure(exception) =>
            logger.error(s"Erreur lors de la génération des recommandations: ${exception.getMessage}", exception)
        }
        
        Future.successful(storedEvent)
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
}