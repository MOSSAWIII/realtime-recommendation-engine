package common.mocks

import application.core.DataManager
import domain.models._
import scala.concurrent.{Future, Promise, ExecutionContext}
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import common.utils.TestResourceManager  // Ajout de l'import manquant
import common.fixtures.TestDataFactory   // Ajout de l'import manquant

/**
 * Version refactorisée du MockDataManager qui implémente l'interface DataManager
 */
class MockDataManager(
  val configService: infrastructure.config.ConfigurationServiceInterface = new TestConfigurationService(),
  val dbProvider: infrastructure.config.DatabaseConfigProvider = new TestDatabaseProvider(),
  val latch: CountDownLatch = null
)(implicit val ec: ExecutionContext) extends application.core.DataManager(configService, dbProvider) {
  
  // Maps pour stocker les données mockées
  private val mockEvents = MutableMap[Long, ListBuffer[ProcessedEvent]]()
  private val mockRecommendations = MutableMap[Long, Recommendation]()
  private val mockUsers = MutableMap[Long, User]()
  private val mockItems = MutableMap[Long, Event]()
  private val mockUserFriends = MutableMap[Long, Set[Long]]()
  
  // Méthodes publiques pour les tests
  def getMockUsers: Map[Long, User] = mockUsers.toMap
  def getUsersList: List[User] = mockUsers.values.toList
  
  // Méthodes pour accéder aux données de test
  def storedEvents: List[ProcessedEvent] = mockEvents.values.flatMap(_.toList).toList
  
  // Méthodes pour manipuler les données de test
  def addUser(user: User): Unit = mockUsers.put(user.userId, user)
  def addEvent(event: Event): Unit = event.id.foreach(id => mockItems.put(id, event))
  def addUserFriend(friend: UserFriend): Unit = {
    val current = mockUserFriends.getOrElse(friend.userId, Set.empty)
    mockUserFriends.put(friend.userId, current + friend.friendId)
  }
  
  // Configuration en une seule opération
  def setupMockData(
    users: List[User] = List.empty,
    events: List[Event] = List.empty,
    processedEvents: List[ProcessedEvent] = List.empty,
    friendships: List[UserFriend] = List.empty
  ): Unit = {
    users.foreach(addUser)
    events.foreach(addEvent)
    processedEvents.foreach(storeProcessedEvent)
    friendships.foreach(addUserFriend)
  }
  
  // Implémentations de méthodes DataManager
  def start(): Unit = {}
  override def close(): Unit = {
    try {
      // Pas besoin de fermer un manager puisque nous sommes dans un mock
    } catch {
      case e: Exception => println(s"Error closing manager: ${e.getMessage}")
    }
  }
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    if (!mockEvents.contains(event.userId)) {
      mockEvents(event.userId) = ListBuffer[ProcessedEvent]()
    }
    mockEvents(event.userId) += event
    
    if (latch != null) {
      latch.countDown()
    }
    
    Future.successful(event)
  }
  
  // Override pour utiliser des données mockées au lieu d'accéder à la BD
  override def getUserProfile(userId: Long): Future[Option[User]] = {
    Future.successful(mockUsers.get(userId))
  }
  
  // Correction de la méthode getAnyUserId
  def getAnyUserId(): Long = {
    mockUsers.headOption.map(_._2.userId).getOrElse {
      val newId = TestResourceManager.uniqueId()
      val newUser = TestDataFactory.createUser(newId)
      mockUsers(newId) = newUser
      newId
    }
  }
  
  // Autres overrides...
}

