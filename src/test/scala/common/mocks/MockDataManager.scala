package common.mocks

import application.core.DataManager
import domain.models._
import scala.concurrent.{Future, Promise, ExecutionContext}
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import common.utils.TestResourceManager
import common.fixtures.TestDataFactory
import org.slf4j.LoggerFactory // Ajouter cet import

/**
 * Version refactorisée du MockDataManager qui implémente l'interface DataManager
 */
class MockDataManager(
  configService: ConfigurationServiceInterface,
  dbProvider: DatabaseConfigProvider,
  latch: CountDownLatch = null
)(implicit ec: ExecutionContext = ExecutionContext.global) extends DataManager(configService, dbProvider)(ec) {
  
  // Ajouter la déclaration du logger
  private val logger = LoggerFactory.getLogger(getClass)
  
  // CORRIGER: Séparer les types de données
  private val mockProcessedEvents = MutableMap[Long, ListBuffer[ProcessedEvent]]()
  private val mockEvents = MutableMap[Long, Event]()  // Changer le type ici
  private val mockRecommendations = MutableMap[Long, Recommendation]()
  private val mockUsers = MutableMap[Long, User]()
  private val mockItems = MutableMap[Long, Event]()
  private val mockUserFriends = MutableMap[Long, Set[Long]]()
  
  // Liste séparée pour les ProcessedEvents stockés
  private val storedProcessedEvents = ListBuffer[ProcessedEvent]()
  
  // Ajouter une liste séparée pour les UserEvents
  private val mockUserEvents = scala.collection.mutable.ListBuffer.empty[UserEvent]
  
  // Méthodes publiques pour les tests
  def getMockUsers: Map[Long, User] = mockUsers.toMap
  def getUsersList: List[User] = mockUsers.values.toList
  
  // CORRIGER: Retourner la bonne liste d'événements
  def getMockEvents(): List[Event] = {
    mockEvents.values.toList  // Retourner les Events, pas les ProcessedEvents
  }
  
  // Ajouter une méthode pour stocker des UserEvents mockés
  def addMockUserEvent(userEvent: UserEvent): Unit = {
    mockUserEvents += userEvent
  }
  
  /**
   * Initialise des données mockées pour les tests
   */
  def initializeMockData(): Unit = {
    val baseItemId = TestResourceManager.uniqueId()
    println(s"Initializing mock data with test item ID: $baseItemId") // Remplacer logger.info
    
    // Initialiser les événements mock
    val mockEvent1 = Event(
      id = Some(baseItemId),
      name = s"Test Event $baseItemId",
      description = Some("Test event description"),
      startTime = System.currentTimeMillis() + 86400000,
      endTime = Some(System.currentTimeMillis() + 172800000),
      location = Some("Test Location"),
      createdBy = 1L,
      status = "active"
    )
    val mockEvent2 = Event(
      id = Some(baseItemId + 1),
      name = s"Test Event ${baseItemId + 1}",
      description = Some("Another test event"),
      startTime = System.currentTimeMillis() + 172800000,
      endTime = Some(System.currentTimeMillis() + 259200000),
      location = Some("Another Test Location"),
      createdBy = 2L,
      status = "active"
    )
    
    addEvent(mockEvent1)
    addEvent(mockEvent2)
    
    // Initialiser les utilisateurs mock
    val mockUser = User(
      userId = 12345L,
      name = Some("Test User"),
      emailAddress = Some("test@example.com"),
      locale = "en_US",
      birthYear = Some(1990),
      gender = Some("M"),
      location = Some("Test City"),
      timezone = Some(1),
      isGenerated = false
    )
    
    addUser(mockUser)
    
    println(s"Mock data initialized with ${mockEvents.size} events and ${mockUsers.size} users") // Remplacer logger.info
  }
  
  // Méthodes pour accéder aux données de test
  def storedEvents: List[ProcessedEvent] = {
    // CORRIGER: Utiliser mockProcessedEvents au lieu de mockEvents
    mockProcessedEvents.values.flatMap(_.toList).toList
  }
  
  // Méthodes pour manipuler les données de test
  def addUser(user: User): Unit = mockUsers.put(user.userId, user)

  // CORRIGER: Utiliser mockEvents au lieu de mockItems
  def addEvent(event: Event): Unit = {
    event.id.foreach(id => mockEvents.put(id, event))
  }

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
    val eventWithId = event.copy(id = Some(storedProcessedEvents.size + 1))
    storedProcessedEvents += eventWithId
    
    // CORRIGER: Utiliser mockProcessedEvents pour stocker par userId
    if (!mockProcessedEvents.contains(event.userId)) {
      mockProcessedEvents(event.userId) = ListBuffer[ProcessedEvent]()
    }
    mockProcessedEvents(event.userId) += eventWithId
    
    println(s"[MockDataManager] Stored event: ${eventWithId.userId}")
    Future.successful(eventWithId)
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
  
  // Ajouter une méthode pour récupérer les événements stockés
  def getStoredProcessedEvents(): List[ProcessedEvent] = {
    println(s"[MockDataManager] Retrieved ${storedProcessedEvents.size} events")
    storedProcessedEvents.toList
  }
  
  // Ajouter une méthode pour vérifier si un événement a été stocké
  def hasStoredEventForUser(userId: Long): Boolean = {
    storedEvents.exists(_.userId == userId)
  }
  
  // Ajouter une méthode pour compter les événements stockés
  def getStoredEventsCount(): Int = {
    storedEvents.size
  }

  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    // CORRIGER: Utiliser mockProcessedEvents
    Future.successful(mockProcessedEvents.getOrElse(userId, ListBuffer.empty).toList.take(limit))
  }

  override def getAllActiveEvents(): Future[List[Event]] = {
    // CORRIGER: Utiliser mockEvents au lieu de mockItems
    Future.successful(mockEvents.values.toList)
  }
}

