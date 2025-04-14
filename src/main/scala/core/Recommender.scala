package core

import models._
import repositories.Repository
import config.RabbitMQConfig
import scala.concurrent.{ExecutionContext, Future}
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets

class Recommender(repository: Repository)(implicit ec: ExecutionContext) {
  
  private val publishChannel = RabbitMQConfig.createChannel()
  
  // Déclaration de la queue de sortie pour les recommandations
  publishChannel.queueDeclare(RabbitMQConfig.Queues.RECOMMENDATIONS, true, false, false, null)
  
  def generateRecommendationsForUser(userId: Long): Future[Recommendation] = {
    // 1. Récupération des événements récents de l'utilisateur
    repository.getProcessedEventsForUser(userId, 100).flatMap { events =>
      // 2. Application d'un algorithme de recommandation simple
      val recommendation = calculateRecommendations(userId, events)
      
      // 3. Stockage des recommandations
      repository.storeRecommendation(recommendation).map { storedRecommendation =>
        // 4. Publication des recommandations dans RabbitMQ
        publishRecommendation(storedRecommendation)
        storedRecommendation
      }
    }
  }
  
  def generateRecommendationsForAllUsers(): Future[List[Recommendation]] = {
    // Cette méthode pourrait être utilisée par une tâche planifiée
    // pour mettre à jour les recommandations de tous les utilisateurs
    
    // 1. Récupération des événements récents
    repository.getRecentProcessedEvents(1000).flatMap { events =>
      // 2. Regroupement par utilisateur
      val userEvents = events.groupBy(_.userId)
      
      // 3. Génération des recommandations pour chaque utilisateur
      val recommendationFutures = userEvents.map { case (userId, userEvents) =>
        val recommendation = calculateRecommendations(userId, userEvents)
        repository.storeRecommendation(recommendation).map { storedRecommendation =>
          publishRecommendation(storedRecommendation)
          storedRecommendation
        }
      }
      
      // 4. Combinaison des résultats
      Future.sequence(recommendationFutures.toList)
    }
  }
  
  private def calculateRecommendations(userId: Long, events: List[ProcessedEvent]): Recommendation = {
    // Implémentation simple d'un moteur de recommandation basé sur la fréquence
    
    // Extraire les IDs d'items et calculer les scores basés sur la fréquence et le récence
    val itemScores = events
      .filter(_.itemId.isDefined)
      .groupBy(_.itemId.get)
      .map { case (itemId, itemEvents) =>
        // Calculer un score basé sur la fréquence et la récence
        val frequency = itemEvents.size
        val recency = itemEvents.map(e => Math.max(0, (System.currentTimeMillis() - e.timestamp) / 3600000.0)).min
        val score = 0.7 * frequency + 0.3 * (1.0 / (1.0 + recency))
        
        (itemId, score)
      }
      .toList
      .sortBy(-_._2)
      .take(5)  // Top 5 items
    
    // Créer les objets RecommendedItem
    val recommendedItems = itemScores.map { case (itemId, score) =>
      RecommendedItem(
        itemId = itemId,
        score = score,
        reason = "Basé sur vos interactions passées"
      )
    }
    
    // Calculer un score global
    val overallScore = if (recommendedItems.isEmpty) 0.0 else recommendedItems.map(_.score).sum / recommendedItems.size
    
    Recommendation(
      userId = userId,
      recommendedItems = recommendedItems,
      score = overallScore
    )
  }
  
  private def publishRecommendation(recommendation: Recommendation): Unit = {
    // Convertir en JSON et publier dans la queue RabbitMQ
    val recommendationJson = recommendation.asJson.noSpaces
    publishChannel.basicPublish(
      "",  // échange par défaut
      RabbitMQConfig.Queues.RECOMMENDATIONS,
      null,
      recommendationJson.getBytes(StandardCharsets.UTF_8)
    )
    println(s"Recommandation publiée pour l'utilisateur ${recommendation.userId}")
  }
  
  def close(): Unit = {
    if (publishChannel != null && publishChannel.isOpen) {
      publishChannel.close()
    }
  }
}