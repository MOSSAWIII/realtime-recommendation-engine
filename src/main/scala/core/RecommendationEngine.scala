package core

import models._
import repositories._
import config.RabbitMQConfig
import scala.concurrent.{ExecutionContext, Future}
import java.time.LocalDateTime
import com.rabbitmq.client.AMQP.BasicProperties
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import java.util.UUID

class RecommendationEngine(
  userRepo: UserRepository,
  eventRepo: EventRepository,
  participationRepo: ParticipationRepository,
  friendsRepo: FriendsRepository,
  interactionRepo: InteractionRepository,
  cacheManager: CacheManager
)(implicit ec: ExecutionContext) {
  
  private val channel = RabbitMQConfig.createChannel()
  private val requestQueueName = RabbitMQConfig.Queues.RECOMMENDATION_REQUESTS
  private val resultExchange = RabbitMQConfig.Exchanges.RECOMMENDATIONS
  
  // Start listening for recommendation requests
  def startRequestConsumer(): Unit = {
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[RecommendationRequest](message) match {
          case Right(request) =>
            // Process recommendation request
            generateRecommendations(request.userId, request.count)
              .map(recommendations => {
                // Publish results back
                val result = RecommendationResult(
                  requestId = request.requestId,
                  userId = request.userId,
                  recommendations = recommendations
                )
                publishResult(result)
              })
          case Left(error) =>
            println(s"Error decoding recommendation request: ${error.getMessage}")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(requestQueueName, false, deliverCallback,
      consumerTag => println(s"Recommendation request consumer cancelled: $consumerTag"))
  }
  
  // Generate recommendations asynchronously
  def generateRecommendations(userId: Long, count: Int = 5): Future[List[Event]] = {
    // First check cache
    cacheManager.getRecommendation(userId).flatMap {
      case Some(cachedRecommendations) => 
        Future.successful(cachedRecommendations)
        
      case None =>
        // Get user data
        for {
          userOpt <- userRepo.findById(userId)
          interactions <- interactionRepo.findByUserId(userId)
          userFriends <- friendsRepo.findFriendsByUserId(userId)
          
          result <- userOpt match {
            case None =>
              Future.successful(List.empty[Event])
              
            case Some(user) =>
              // Core recommendation logic
              for {
                // Find events friends are attending
                friendEvents <- findFriendsEvents(userFriends)
                
                // Find events based on past interactions
                interactionEvents <- findEventsBasedOnInteractions(interactions)
                
                // Find events based on location
                locationEvents <- findEventsNearUser(user)
                
                // Combine all sources and apply weights
                allEvents <- combineEventSources(friendEvents, interactionEvents, locationEvents)
                
                // Get final list of top events
                futureEvents <- filterFutureEvents(allEvents, count)
              } yield {
                // Cache the results
                cacheManager.cacheRecommendation(userId, futureEvents.toList)
                futureEvents.toList
              }
          }
        } yield result
    }
  }
  
  // Request recommendations asynchronously
  def requestRecommendationsAsync(userId: Long, count: Int = 5): Future[String] = {
    val request = RecommendationRequest(
      requestId = UUID.randomUUID().toString,
      userId = userId,
      count = count,
      timestamp = LocalDateTime.now()
    )
    
    Future {
      try {
        val props = new BasicProperties.Builder()
          .contentType("application/json")
          .build()
        
        channel.basicPublish(
          resultExchange,
          "request",
          props,
          request.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
        )
        
        request.requestId
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to request recommendations: ${e.getMessage}", e)
      }
    }
  }
  
  private def publishResult(result: RecommendationResult): Unit = {
    try {
      val props = new BasicProperties.Builder()
        .contentType("application/json")
        .build()
      
      channel.basicPublish(
        resultExchange,
        "result",
        props,
        result.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
      )
      
      println(s"Published recommendation result for request ${result.requestId}")
    } catch {
      case e: Exception =>
        println(s"Error publishing recommendation result: ${e.getMessage}")
    }
  }
  
  // Helper methods for recommendation generation
  private def findFriendsEvents(userFriends: Seq[Long]): Future[Seq[Event]] = {
    if (userFriends.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      for {
        friendsParticipations <- Future.sequence(
          userFriends.map(friendId => participationRepo.findEventsByUserId(friendId))
        )
        eventIds = friendsParticipations.flatten.distinct
        events <- eventRepo.findByIds(eventIds)
      } yield events
    }
  }
  
  private def findEventsBasedOnInteractions(interactions: Seq[UserInteraction]): Future[Seq[Event]] = {
    if (interactions.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      val interactionEventIds = interactions
        .filter(i => i.status == "yes" || i.status == "maybe") // Only consider positive interactions
        .map(_.eventId)
        .distinct
        
      // Get these events to find similar ones
      eventRepo.findByIds(interactionEventIds).flatMap { previousEvents =>
        // Extract keywords (you'd need to implement this based on your data)
        val keywords = previousEvents.flatMap(event => 
          event.keywordFrequencies.keys.toSeq.sortBy(-event.keywordFrequencies(_)).take(5)
        ).distinct
        
        if (keywords.isEmpty) {
          Future.successful(Seq.empty)
        } else {
          eventRepo.findByKeywords(keywords)
        }
      }
    }
  }
  
  private def findEventsNearUser(user: User): Future[Seq[Event]] = {
    user.location match {
      case Some(location) =>
        // Simple location matching (would need to be enhanced for real-world use)
        eventRepo.findByLocation(location)
      case None =>
        Future.successful(Seq.empty)
    }
  }
  
  private def combineEventSources(
    friendEvents: Seq[Event], 
    interactionEvents: Seq[Event], 
    locationEvents: Seq[Event]
  ): Future[Seq[(Event, Double)]] = {
    // Combine and score events from different sources
    // This is a simplified implementation
    val allEvents = (friendEvents ++ interactionEvents ++ locationEvents).distinct
    
    Future.successful(
      allEvents.map { event =>
        val socialScore = if (friendEvents.contains(event)) 1.5 else 1.0
        val interactionScore = if (interactionEvents.contains(event)) 1.3 else 1.0
        val locationScore = if (locationEvents.contains(event)) 1.2 else 1.0
        
        val totalScore = socialScore * interactionScore * locationScore
        (event, totalScore)
      }
    )
  }
  
  private def filterFutureEvents(scoredEvents: Seq[(Event, Double)], count: Int): Future[Seq[Event]] = {
    val now = LocalDateTime.now()
    Future.successful(
      scoredEvents
        .filter(_._1.startTime.isAfter(now))
        .sortBy(-_._2)
        .take(count)
        .map(_._1)
    )
  }
  
  def close(): Unit = {
    if (channel.isOpen) {
      channel.close()
    }
  }
}

// Message types for recommendation requests and responses
case class RecommendationRequest(
  requestId: String,
  userId: Long,
  count: Int,
  timestamp: LocalDateTime
)

case class RecommendationResult(
  requestId: String,
  userId: Long,
  recommendations: List[Event]
)
