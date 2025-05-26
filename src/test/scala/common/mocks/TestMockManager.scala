package common.mocks

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import java.sql.{Connection => SqlConnection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
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
    org.mockito.Mockito.mock(classOf[Channel])
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
    val channel = connection.fold(mockRabbitMQChannel())(_.createChannel())
    channels.put(name, channel)
    channel
  }
  
  def getChannel(name: String): Channel = {
    channels.getOrElse(name, createChannel(name))
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
    channels.keys.toList.foreach(closeChannel)
  }
  
  def cleanup(): Unit = {
    closeAllChannels()
    connection.foreach(conn => try { conn.close() } catch { case _: Exception => })
    connection = None
  }
  
  // ======= Mock DataManager =======
  class MockDataManager(val latch: CountDownLatch = null)(implicit val ec: ExecutionContext) extends application.core.DataManager {
    // Maps to store mock data
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
    
    // DataManager implementation
    override def start(): Unit = {}
    override def close(): Unit = {}
    
    override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
      val userId = event.userId
      if (!mockEvents.contains(userId)) {
        mockEvents(userId) = ListBuffer[ProcessedEvent]()
      }
      mockEvents[userId] += event
      
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
}

