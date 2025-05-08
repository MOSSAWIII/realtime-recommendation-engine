package integration.recommendation

import models._
import repositories.Repository
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import org.slf4j.LoggerFactory

class TestRepository(implicit override val ec: ExecutionContext = scala.concurrent.ExecutionContext.global) extends Repository {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Mutable state for test data
  var mockUserProfile: Option[User] = None
  var mockUserFriends: List[UserFriend] = List.empty
  var mockProcessedEvents: List[ProcessedEvent] = List.empty
  var mockEvents: List[Event] = List.empty
  val mockRecommendations: MutableMap[Long, Recommendation] = MutableMap.empty
  
  override def getUserProfile(userId: Long): Future[Option[User]] = {
    logger.debug(s"Getting user profile for $userId")
    Future.successful(mockUserProfile)
  }
  
  override def getUserFriends(userId: Long): Future[List[UserFriend]] = {
    logger.debug(s"Getting user friends for $userId: ${mockUserFriends.size} friends")
    Future.successful(mockUserFriends)
  }
  
  override def getAllActiveEvents(): Future[List[Event]] = {
    logger.debug(s"Getting all active events: ${mockEvents.size} events")
    Future.successful(mockEvents)
  }
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    val events = mockProcessedEvents.filter(_.userId == userId).take(limit)
    logger.info(s"Getting processed events for user $userId. Found: ${events.size}")
    Future.successful(events)
  }
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    val eventWithId = event.copy(id = Some(System.currentTimeMillis()))
    mockProcessedEvents = eventWithId :: mockProcessedEvents
    logger.debug(s"Stored processed event for user ${event.userId}")
    Future.successful(eventWithId)
  }
  
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    logger.info(s"Storing recommendation for user ${recommendation.userId}")
    val recommendationWithId = recommendation.copy(id = Some(System.currentTimeMillis()))
    mockRecommendations(recommendation.userId) = recommendationWithId
    Future.successful(recommendationWithId)
  }
  
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    logger.debug(s"Getting recommendation for user $userId")
    Future.successful(mockRecommendations.get(userId))
  }
  
  override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = 
    Future.successful(mockRecommendations.get(userId))
  
  override def initTables(): Future[Unit] = Future.successful(())
}

