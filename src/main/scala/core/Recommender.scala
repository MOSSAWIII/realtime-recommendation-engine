package core

import models._
import repositories.Repository
import config.RabbitMQManager
import scala.concurrent.{ExecutionContext, Future}
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import org.slf4j.LoggerFactory
import com.rabbitmq.client.{DeliverCallback, CancelCallback}

class Recommender(repository: Repository)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Use the centralized RabbitMQ manager
  private val consumeChannel = RabbitMQManager.createChannel("recommender-consume")
  private val publishChannel = RabbitMQManager.createChannel("recommender-publish")
  
  // Déclaration des queues
  consumeChannel.queueDeclare(
    RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, 
    true, false, false, null
  )
  publishChannel.queueDeclare(
    RabbitMQManager.Queues.RECOMMENDATIONS, 
    true, false, false, null
  )
  
  // Configuration de l'algorithme de recommandation
  private object AlgorithmConfig {
    val RECENCY_WEIGHT = 0.3 // Poids de la récence (30%)
    val FREQUENCY_WEIGHT = 0.7 // Poids de la fréquence (70%)
    val MAX_RECOMMENDATIONS = 5 // Nombre max de recommandations
    val RECENCY_DECAY_DAYS = 30.0 // Facteur de décroissance temporelle (30 jours)
  }
  
  // Start consuming recommendation requests
  def start(): Unit = {
    logger.info("Starting recommendation request consumer...")
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        // Process recommendation request
        decode[RecommendationRequest](message) match {
          case Right(request) =>
            logger.debug(s"Processing recommendation request for user ${request.userId}")
            
            // Generate recommendations asynchronously
            generateRecommendationsForUser(request.userId).onComplete {
              case scala.util.Success(recommendation) =>
                logger.info(s"Successfully generated recommendations for user ${request.userId}")
              case scala.util.Failure(exception) =>
                logger.error(s"Failed to generate recommendations for user ${request.userId}: ${exception.getMessage}", exception)
            }
            
          case Left(error) =>
            logger.error(s"Error decoding recommendation request: ${error.getMessage}", error)
        }
        
        // Acknowledge the message
        consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        logger.warn(s"Recommendation request consumer cancelled: $consumerTag")
    }
    
    // Set prefetch to limit the number of unacknowledged messages (for backpressure)
    consumeChannel.basicQos(10)
    
    // Start consuming
    consumeChannel.basicConsume(
      RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS,
      false,
      deliverCallback,
      cancelCallback
    )
    
    logger.info("Recommendation request consumer started")
  }
  
  def generateRecommendationsForUser(userId: Long): Future[Recommendation] = {
    // Add throttling - check if we've recently generated recommendations for this user
    repository.getRecentRecommendationForUser(userId, 5 * 60 * 1000) // 5 minutes
      .flatMap { 
        case Some(recentRecommendation) =>
          // Recent recommendation exists, return it instead of regenerating
          logger.debug(s"Using recent recommendation for user $userId")
          Future.successful(recentRecommendation)
          
        case None =>
          // No recent recommendation, generate a new one
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
  }
  
  def generateRecommendationsForAllUsers(): Future[List[Recommendation]] = {
    repository.getRecentProcessedEvents(1000).flatMap { events =>
      val userEvents = events.groupBy(_.userId)
      
      val recommendationFutures = userEvents.map { case (userId, userEvents) =>
        val recommendation = calculateRecommendations(userId, userEvents)
        repository.storeRecommendation(recommendation).map { storedRecommendation =>
          publishRecommendation(storedRecommendation)
          storedRecommendation
        }
      }
      
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
    val recommendationJson = recommendation.asJson.noSpaces
    publishChannel.basicPublish(
      "",  // échange par défaut
      RabbitMQManager.Queues.RECOMMENDATIONS,
      null,
      recommendationJson.getBytes(StandardCharsets.UTF_8)
    )
    logger.info(s"Recommandation publiée pour l'utilisateur ${recommendation.userId}")
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("recommender-consume")
    RabbitMQManager.closeChannel("recommender-publish")
  }
}