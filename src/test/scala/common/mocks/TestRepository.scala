package common.mocks

import application.core.DataManager
import domain.models._
import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}

class TestRepository(
  val configService: ConfigurationServiceInterface = new common.mocks.TestConfigurationService(),
  val dbProvider: DatabaseConfigProvider = new common.mocks.TestDatabaseProvider()
)(implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global) 
  extends DataManager(configService, dbProvider)(ec) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Délégation à MockDataManager avec ExecutionContext
  private val mockDataManager = new MockDataManager(configService, dbProvider)(ec)
  
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
    mockRecommendations.foreach { case (userId, rec) => 
      mockDataManager.storeRecommendation(rec)
    }
    
    // Regrouper les amitiés par userId
    mockUserFriends.groupBy(_.userId).foreach { case (userId, friends) => 
      mockDataManager.addUserFriend(friends.head)
      friends.tail.foreach(mockDataManager.addUserFriend)
    }
  }
  
  // Méthodes DataManager déléguées à MockDataManager
  def start(): Unit = {
    migrateData()
    mockDataManager.start()
  }
  
  override def close(): Unit = mockDataManager.close()
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    mockProcessedEvents = event :: mockProcessedEvents
    mockDataManager.storeProcessedEvent(event)
  }
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    mockDataManager.getProcessedEventsForUser(userId, limit).map(_.toList)
  }
  
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    val updatedRecommendation = recommendation.copy(id = Some(System.currentTimeMillis()))
    mockRecommendations(recommendation.userId) = updatedRecommendation
    mockDataManager.storeRecommendation(updatedRecommendation)
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
  
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    Future.successful(mockRecommendations.get(userId))
  }
}

