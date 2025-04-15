package core

import models._
import repositories.Repository
import config.RabbitMQConfig
import scala.concurrent.{ExecutionContext, Future}
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import org.slf4j.LoggerFactory

class Recommender(repository: Repository)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val publishChannel = RabbitMQConfig.createChannel()
  
  // Déclaration de la queue de sortie pour les recommandations
  publishChannel.queueDeclare(RabbitMQConfig.Queues.RECOMMENDATIONS, true, false, false, null)
  
  // Configuration de l'algorithme de recommandation
  private object AlgorithmConfig {
    val RECENCY_WEIGHT = 0.3 // Poids de la récence (30%)
    val FREQUENCY_WEIGHT = 0.7 // Poids de la fréquence (70%)
    val MAX_RECOMMENDATIONS = 5 // Nombre max de recommandations
    val RECENCY_DECAY_DAYS = 30.0 // Facteur de décroissance temporelle (30 jours)
  }
  
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
    val currentTime = System.currentTimeMillis()
    
    // Extraire les items uniques des événements
    val itemCounts = events
      .filter(_.itemId.isDefined)
      .groupBy(_.itemId.get)
      .view
      .mapValues(_.size)
      .toMap
    
    // Calcul du score de récence
    val recencyScores = events
      .filter(_.itemId.isDefined)
      .groupBy(_.itemId.get)
      .view
      .mapValues { events =>
        val mostRecent = events.maxBy(_.timestamp).timestamp
        val daysAgo = (currentTime - mostRecent) / (1000.0 * 60 * 60 * 24)
        Math.exp(-daysAgo / AlgorithmConfig.RECENCY_DECAY_DAYS) // Score qui diminue avec le temps
      }
      .toMap
    
    // Normalisation des scores de fréquence
    val maxCount = if (itemCounts.isEmpty) 1.0 else itemCounts.values.max.toDouble
    val normalizedFrequency = itemCounts.view.mapValues(_ / maxCount).toMap
    
    // Calcul du score combiné (fréquence + récence)
    val itemScores = itemCounts.keys.map { itemId =>
      val frequencyScore = normalizedFrequency.getOrElse(itemId, 0.0) * AlgorithmConfig.FREQUENCY_WEIGHT
      val recencyScore = recencyScores.getOrElse(itemId, 0.0) * AlgorithmConfig.RECENCY_WEIGHT
      (itemId, frequencyScore + recencyScore)
    }.toList
      .sortBy(-_._2)
      .take(AlgorithmConfig.MAX_RECOMMENDATIONS)
    
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
    logger.info(s"Recommandation publiée pour l'utilisateur ${recommendation.userId}")
  }
  
  def close(): Unit = {
    if (publishChannel != null && publishChannel.isOpen) {
      publishChannel.close()
      logger.info("Recommender channel closed")
    }
  }
}