package common.mocks

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import java.sql.{Connection => SqlConnection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import com.typesafe.config.{Config, ConfigFactory}  // Ajout de Config
import domain.models._
import java.util.concurrent.CountDownLatch

/**
 * Unified mock manager for all test mocks
 */
object TestMockManager {
  // Test execution context
  val testExecutionContext: ExecutionContext = ExecutionContext.global
  
  // ======= Database Mocks =======
  def mockDatabaseConnection(): SqlConnection = {
    org.mockito.Mockito.mock(classOf[SqlConnection])
  }
  
  def mockPreparedStatement(): PreparedStatement = {
    org.mockito.Mockito.mock(classOf[PreparedStatement])
  }
  
  def mockResultSet(): ResultSet = {
    org.mockito.Mockito.mock(classOf[ResultSet])
  }
  
  // ======= RabbitMQ Mocks =======
  private val channels = MutableMap[String, Channel]()
  private var connection: Option[Connection] = None
  
  // Constants
  object Exchanges {
    val USER_ACTIONS = "user_actions_exchange"
  }
  
  object Queues {
    val RAW_EVENTS = "raw_events_queue"
    val PROCESSED_EVENTS = "processed_events_queue"
    val RECOMMENDATIONS = "recommendations_queue"
    val HISTORICAL_DATA = "historical_data_queue"
  }
  
  def mockRabbitMQChannel(): Channel = {
    val channel = mock(classOf[Channel])
    // Correction: utilisation de doNothing() au lieu de thenReturn(null)
    doNothing().when(channel).basicPublish(anyString(), anyString(), any(), any())
    channel
  }
  
  def mockRabbitMQConnection(): Connection = {
    org.mockito.Mockito.mock(classOf[Connection])
  }
  
  def initConnection(host: String = "localhost", port: Int = 5673): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    connection = Some(factory.newConnection())
  }
  
  def createChannel(name: String): Channel = {
    if (channels.contains(name)) {
      return channels(name)
    }
    
    val channel = mock(classOf[Channel])
    // Setup basic mocking
    when(channel.isOpen).thenReturn(true)
    doNothing().when(channel).basicPublish(anyString(), anyString(), any(), any())
    
    channels.put(name, channel)
    channel
  }
  
  def getChannel(name: String): Option[Channel] = {
    channels.get(name)
  }
  
  def closeChannel(name: String): Unit = {
    channels.get(name).foreach { channel =>
      try {
        if (channel.isOpen) channel.close()
      } catch {
        case _: Exception => // Ignore
      }
      channels.remove(name)
    }
  }
  
  def closeAllChannels(): Unit = {
    channels.foreach { case (_, channel) => 
      try { channel.close() } catch { case _: Exception => }
    }
    channels.clear()
  }
  
  def cleanup(): Unit = {
    closeAllChannels()
    connection.foreach(conn => try { conn.close() } catch { case _: Exception => })
    connection = None
  }
  
  // ======= Mock DataManager =======
  class MockDataManager(
    val configService: infrastructure.config.ConfigurationServiceInterface = new MockConfigService(),
    val dbProvider: infrastructure.config.DatabaseConfigProvider = new MockDatabaseProvider(),
    val latch: CountDownLatch = null
  )(implicit val ec: ExecutionContext) extends application.core.DataManager(configService, dbProvider) {
    
    // Maps pour stocker les données mockées
    private val mockEvents = MutableMap[Long, ListBuffer[ProcessedEvent]]()
    private val mockRecommendations = MutableMap[Long, Recommendation]()
    private val mockUsers = MutableMap[Long, User]()
    private val mockItems = MutableMap[Long, Event]()
    private val mockUserFriends = MutableMap[Long, Set[Long]]()
    
    // Access methods for tests
    def storedEvents: List[ProcessedEvent] = mockEvents.values.flatMap(_.toList).toList
    
    // Methods to manipulate mock data
    def addUser(user: User): Unit = mockUsers.put(user.userId, user)
    
    def addEvent(event: Event): Unit = event.id.foreach(id => mockItems.put(id, event))
    
    def addUserFriend(friend: UserFriend): Unit = {
      val current = mockUserFriends.getOrElse(friend.userId, Set.empty)
      mockUserFriends.put(friend.userId, current + friend.friendId)
    }
    
    def addRecommendation(recommendation: Recommendation): Unit = {
      mockRecommendations(recommendation.userId) = recommendation
    }
    
    // Setup test data in one call
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
    override def close(): Unit = {}
    
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
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
      Future.successful(mockEvents.getOrElse(userId, ListBuffer.empty).toList.take(limit))
    }
    
    override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
      mockRecommendations(recommendation.userId) = recommendation
      Future.successful(recommendation)
    }
    
    override def getUserProfile(userId: Long): Future[Option[User]] = {
      Future.successful(mockUsers.get(userId))
    }
    
    override def getUserFriends(userId: Long): Future[List[UserFriend]] = {
      Future.successful(
        mockUserFriends.getOrElse(userId, Set.empty).toList
          .map(friendId => UserFriend(userId, friendId, 1.0))
      )
    }
    
    override def getAllActiveEvents(): Future[List[Event]] = {
      Future.successful(mockItems.values.toList)
    }
    
    override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
      Future.successful(mockRecommendations.get(userId))
    }
    
    override def getRecentRecommendationForUser(userId: Long, maxAgeMs: Long): Future[Option[Recommendation]] = {
      Future.successful(mockRecommendations.get(userId))
    }
  }
  
  // Pour référencer la TestConfigurationService existante
  def createMockConfigService(): TestConfigurationService = {
    new TestConfigurationService()
  }
  
  // Pour référencer le TestDatabaseProvider existant
  def createMockDatabaseProvider(): TestDatabaseProvider = {
    new TestDatabaseProvider()
  }
  
  // Correction des types manquants
  def createTestComponents(implicit ec: ExecutionContext) = {
    val configService: infrastructure.config.ConfigurationServiceInterface = new MockConfigService()
    val dbProvider: infrastructure.config.DatabaseConfigProvider = new MockDatabaseProvider()
    
    // Autres composants...
    
    (configService, dbProvider)
  }
}

// Création des classes manquantes
class MockConfigService extends infrastructure.config.ConfigurationServiceInterface {
  // Implémentation minimale
  override def getConfig: Config = com.typesafe.config.ConfigFactory.load()
  
  override def get[T <: infrastructure.config.ConfigSection](implicit converter: com.typesafe.config.Config => T): T = {
    converter(getConfig)
  }
  
  override def reload(): Boolean = true
  
  override def updateConfigValue(path: String, value: String): Boolean = true
  
  override def updateConfigValueGeneric(path: String, value: Any): Boolean = true
}

class MockDatabaseProvider extends infrastructure.config.DatabaseConfigProvider {
  override def getConnection(): java.sql.Connection = mock(classOf[java.sql.Connection])
  override def close(): Unit = {}
}

