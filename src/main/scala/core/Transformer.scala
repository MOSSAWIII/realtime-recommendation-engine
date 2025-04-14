package core

import models._
import repositories.Repository
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

class Transformer(repository: Repository, recommender: Recommender)(implicit ec: ExecutionContext) {
  
  def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    // 1. Transformation de l'événement
    val processedEvent = transformEvent(event)
    
    // 2. Stockage dans la base de données
    repository.storeProcessedEvent(processedEvent).flatMap { storedEvent =>
      // 3. Déclenchement du processus de recommandation
      recommender.generateRecommendationsForUser(event.userId).onComplete {
        case Success(recommendation) => 
          println(s"Nouvelles recommandations générées pour l'utilisateur ${event.userId}")
        case Failure(exception) =>
          println(s"Erreur lors de la génération des recommandations: ${exception.getMessage}")
      }
      
      Future.successful(storedEvent)
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