package common.mocks

import core.DataManager
import models._
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.jdk.CollectionConverters._
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import common.utils.MockDataManager

/**
 * @deprecated Utiliser MockDataManager du package common.mocks à la place
 */
@deprecated("Remplacer par testutil.MockDataManager", "Version 2.0")
class TestRepository(implicit override val ec: ExecutionContext = scala.concurrent.ExecutionContext.global) extends DataManager {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Délégation à MockDataManager pour rester compatible avec l'ancien code
  private val mockDataManager = new MockDataManager()
  
  // Support pour les anciennes données mockées
  var mockUserProfile: Option[User] = None
  var mockUserFriends: List[UserFriend] = List.empty
  var mockProcessedEvents: List[ProcessedEvent] = List.empty
  var mockEvents: List[Event] = List.empty
  val mockRecommendations: scala.collection.mutable.Map[Long, Recommendation] = scala.collection.mutable.Map.empty
  
  // Migrer les données existantes vers MockDataManager si nécessaire
  private def migrateData(): Unit = {
    if (mockUserProfile.isDefined) mockDataManager.addUser(mockUserProfile.get)
    mockProcessedEvents.foreach(e => mockDataManager.storeProcessedEvent(e))
    mockEvents.foreach(e => mockDataManager.addEvent(e))
    mockRecommendations.foreach { case (userId, rec) => mockDataManager.addRecommendation(rec) }
    
    // Regrouper les amitiés par userId
    mockUserFriends.groupBy(_.userId).foreach { case (userId, friends) => 
      mockDataManager.addUserFriends(userId, friends)
    }
  }
  
  // Méthodes DataManager déléguées à MockDataManager
  override def start(): Unit = {
    migrateData()
    mockDataManager.start()
  }
  
  override def close(): Unit = mockDataManager.close()
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    mockProcessedEvents = event :: mockProcessedEvents
    mockDataManager.storeProcessedEvent(event)
  }
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    mockDataManager.getProcessedEventsForUser(userId, limit)
  }
  
  override def getUserProfile(userId: Long): Future[Option[User]] = {
    Future.successful(mockUserProfile)
  }
  
  override def getUserFriends(userId: Long): Future[List[UserFriend]] = {
    Future.successful(mockUserFriends.filter(_.userId == userId))
  }
  
  override def getAllActiveEvents(): Future[List[Event]] = {
    Future.successful(mockEvents)
  }
  
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    val updatedRecommendation = recommendation.copy(id = Some(System.currentTimeMillis()))
    mockRecommendations(recommendation.userId) = updatedRecommendation
    mockDataManager.storeRecommendation(updatedRecommendation)
  }
  
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    Future.successful(mockRecommendations.get(userId))
  }
  
  override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = {
    Future.successful(mockRecommendations.get(userId))
  }
  
  override def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]] = {
    Future.successful(mockProcessedEvents.take(limit))
  }
}

