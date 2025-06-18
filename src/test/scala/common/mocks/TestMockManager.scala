package common.mocks

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import java.sql.{Connection => SqlConnection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import com.typesafe.config.{Config, ConfigFactory}
import domain.models._
import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._

/**
 * Unified mock manager for all test mocks
 */
object TestMockManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private var connection: Option[Connection] = None

  // Ajouter ExecutionContext implicite global
  implicit val testExecutionContext: ExecutionContext = ExecutionContext.global
  
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
  
  // Constants
  object Exchanges {
    val USER_ACTIONS = "user_actions_exchange"  // Same as production
  }
  
  object Queues {
    val RAW_EVENTS = "raw_events_queue"
    val PROCESSED_EVENTS = "processed_events_queue"  // Remove test_ prefix
    val RECOMMENDATIONS = "recommendations_queue"    // Remove test_ prefix
    val RECOMMENDATION_REQUESTS = "recommendation_requests_queue"
    val HISTORICAL_DATA = "historical_data_queue"   // Same as production
  }

  object RoutingKeys {
    val PROCESSED_EVENTS = "event.processed.#"
    val RECOMMENDATIONS = "recommendation.result.#"
    val HISTORICAL_DATA = "historical.data.#"
    val USER_ACTIONS = "user.actions.#"  // Add missing routing key
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
  
  def createConnection(): Connection = {
    val factory = new ConnectionFactory()
    factory.setHost(sys.env.getOrElse("RABBITMQ_HOST", "test-rabbitmq"))
    factory.setPort(sys.env.getOrElse("RABBITMQ_PORT", "5672").toInt)
    factory.setUsername("guest")
    factory.setPassword("guest")
    
    // Use test vhost from environment
    val testVHost = sys.env.getOrElse("TEST_VHOST", "/")
    factory.setVirtualHost(testVHost)
    
    logger.info(s"Connecting to RabbitMQ with vhost: $testVHost")
    
    factory.newConnection()
  }
  
  def initConnection(): Unit = {
    val maxRetries = 10
    val retryDelay = 2.seconds
    
    def attemptConnection(attempt: Int): Unit = {
      if (attempt > maxRetries) {
        throw new RuntimeException(s"Failed to connect to RabbitMQ after $maxRetries attempts")
      }
      
      Try {
        val factory = new ConnectionFactory()
        // Try different host configurations
        val hosts = List("test-rabbitmq", "localhost", "127.0.0.1")
        val ports = List(5673, 5672) // Test port first, then default
        
        var connected = false
        for (host <- hosts if !connected; port <- ports if !connected) {
          Try {
            factory.setHost(host)
            factory.setPort(port)
            factory.setUsername("guest")
            factory.setPassword("guest")
            factory.setConnectionTimeout(5000)
            factory.setRequestedHeartbeat(30)
            
            logger.info(s"Attempting to connect to RabbitMQ at $host:$port (attempt $attempt)")
            factory.newConnection()
          } match {
            case Success(conn) =>
              connection = Some(conn)
              connected = true
              logger.info(s"Successfully connected to RabbitMQ at $host:$port")
            case Failure(e) =>
              logger.warn(s"Failed to connect to $host:$port: ${e.getMessage}")
          }
        }
        
        if (!connected) {
          throw new RuntimeException("Could not connect to any RabbitMQ host")
        }
      } match {
        case Success(_) => // Connection successful
        case Failure(e) =>
          logger.warn(s"Connection attempt $attempt failed: ${e.getMessage}")
          Thread.sleep(retryDelay.toMillis)
          attemptConnection(attempt + 1)
      }
    }
    
    attemptConnection(1)
  }
  
  def createChannel(name: String): Channel = {
    connection match {
      case Some(conn) if conn.isOpen => 
        conn.createChannel()
      case _ => 
        initConnection()
        connection.get.createChannel()
    }
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
    connection.foreach { conn =>
      Try(conn.close())
      logger.info("TestMockManager connection closed")
    }
    connection = None
  }
  
  // ======= Mock DataManager =======
  class MockDataManager(
    val configService: infrastructure.config.ConfigurationServiceInterface = new MockConfigService(),
    val dbProvider: infrastructure.config.DatabaseConfigProvider = new MockDatabaseProvider(),
    val latch: CountDownLatch = null
  )(implicit val ec: ExecutionContext = testExecutionContext) extends application.core.DataManager(configService, dbProvider)(ec) {
    
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
  
  // Autres méthodes factory
  def createMockDataManager(withLatch: Boolean = false): MockDataManager = {
    val latch = if (withLatch) new CountDownLatch(1) else null
    new MockDataManager(
      new MockConfigService(),
      new MockDatabaseProvider(),
      latch
    )(testExecutionContext)
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

