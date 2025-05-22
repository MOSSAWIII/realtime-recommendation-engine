package application.core

import domain.models._
import infrastructure.config.{RabbitMQManager, DataManagerConfigSection}
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import io.circe.parser.decode
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import utils.metrics.{MetricsRegistry, HealthCheckRegistry, SystemStatus, HealthStatus, HealthCheckResult}

/**
 * DataManager component responsible for all data access operations.
 * Consolidates repository functionality and manages historical data.
 */
class DataManager(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "data-manager"
  
  // Transaction manager for database operations
  private val txManager = new DataManagerTransactions()
  txManager.startMonitoring()
  
  // RabbitMQ channels
  private var historicalDataChannel: Option[Channel] = None
  private var recommenderSyncChannel: Option[Channel] = None
  
  // Metrics
  private val metricsPrefix = "data-manager"
  
  private val dataLoadCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "data", "loads"),
    "Total number of data loads"
  )
  
  private val dataLoadErrorCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "data", "errors"),
    "Total number of data load errors"
  )
  
  private val dataProcessCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(metricsPrefix, "data", "processed"),
    "Number of historical data entries processed"
  )
  
  // In-memory caches with TTL support
  private val DefaultCacheTTL = 5.minutes
  private val ShortCacheTTL = 1.minute
  
  private case class CacheEntry[T](value: T, expiration: Long)
  private val cache = scala.collection.concurrent.TrieMap[String, CacheEntry[Any]]()
  
  /**
   * Start the DataManager services
   */
  def start(): Unit = {
    logger.info("Starting DataManager...")
    
    // Initialize historical data consumer
    initHistoricalDataConsumer()
    
    // Perform initial data load
    loadAll()
    
    logger.info("DataManager started")
  }
  
  /**
   * Initialize consumer for historical data
   */
  private def initHistoricalDataConsumer(): Unit = {
    val channel = RabbitMQManager.createChannel("historical-data-consumer")
    historicalDataChannel = Some(channel)
    
    channel.queueDeclare(RabbitMQManager.Queues.HISTORICAL_DATA, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        try {
          processHistoricalData(message)
          channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to process historical data message: ${ex.getMessage}", ex)
            // Negative acknowledgment, don't requeue as it might cause an infinite loop if message is malformed
            channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
        }
      }
    }
    
    // Cancel callback is a no-op
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer was cancelled: $consumerTag")
      }
    }
    
    channel.basicConsume(
      RabbitMQManager.Queues.HISTORICAL_DATA,
      false, // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info("Historical data consumer initialized")
  }
  
  /**
   * Process incoming historical data messages
   */
  private def processHistoricalData(message: String): Unit = {
    decode[HistoricalDataImport](message) match {
      case Right(data) =>
        logger.info(s"Processing historical data import: ${data.dataType}, ${data.records.size} records")
        
        // Process based on data type
        data.dataType match {
          case "users" =>
            // Implementation for user imports
            
          case "events" =>
            // Implementation for event imports
            
          case "participations" =>
            // Implementation for participation imports
            
          case "friendships" =>
            // Implementation for friendship imports
            
          case unknown =>
            logger.warn(s"Received unknown historical data type: $unknown")
        }
        
      case Left(error) =>
        logger.error(s"Failed to parse historical data message: ${error.getMessage}")
        throw new RuntimeException(s"Invalid historical data format: ${error.getMessage}")
    }
  }
  
  /**
   * Load all data asynchronously from the database
   * @return Future representing completion of all data loading
   */
  def loadAllAsync(): Future[Unit] = {
    val futures = Seq(
      // Load users from database
      txManager.withTransaction { conn =>
        val users = findAllUsersSync(conn)
        logger.info(s"Loaded ${users.size} users from database")
        users
      }.recover { case ex =>
        logger.error("Failed to load users", ex)
        throw ex
      },
      
      // Load events from database
      txManager.withTransaction { conn =>
        val events = findFutureEventsSync(conn)
        logger.info(s"Loaded ${events.size} events from database")
        events
      }.recover { case ex =>
        logger.error("Failed to load events", ex)
        throw ex
      },
      
      // Load participations from database
      txManager.withTransaction { conn =>
        val participations = findAllParticipationsSync(conn)
        logger.info(s"Loaded ${participations.size} participations from database")
        participations
      }.recover { case ex =>
        logger.error("Failed to load participations", ex)
        throw ex
      },
      
      // Load friendships from database
      txManager.withTransaction { conn =>
        val friendships = findAllFriendshipsSync(conn)
        logger.info(s"Loaded ${friendships.size} friendships from database")
        friendships
      }.recover { case ex =>
        logger.error("Failed to load friendships", ex)
        throw ex
      }
    )
    
    Future.sequence(futures).map(_ => ())
  }
  
  /**
   * Load all data synchronously, blocking until complete
   */
  def loadAll(): Unit = {
    try {
      Await.result(loadAllAsync(), 2.minutes)
      logger.info("All data loaded successfully")
      dataLoadCounter.inc()
    } catch {
      case ex: Exception =>
        logger.error("Failed to load all data", ex)
        dataLoadErrorCounter.inc()
        throw ex
    }
  }
  
  // Repository functionality moved directly into DataManager
  
  /**
   * Find all users
   */
  def findAllUsersSync(conn: Connection): List[User] = {
    withPreparedStatement(conn, "SELECT * FROM users") { ps =>
      withResultSet(ps) { rs =>
        val buffer = scala.collection.mutable.ListBuffer.empty[User]
        while (rs.next()) {
          buffer += mapResultSetToUser(rs)
        }
        buffer.toList
      }
    }
  }
  
  /**
   * Find future events
   */
  def findFutureEventsSync(conn: Connection): List[Event] = {
    val sql = "SELECT * FROM events WHERE start_time > NOW() ORDER BY start_time"
    withPreparedStatement(conn, sql) { ps =>
      withResultSet(ps) { rs =>
        val buffer = scala.collection.mutable.ListBuffer.empty[Event]
        while (rs.next()) {
          buffer += mapResultSetToEvent(rs)
        }
        buffer.toList
      }
    }
  }
  
  /**
   * Find all participations
   */
  def findAllParticipationsSync(conn: Connection): List[Participation] = {
    withPreparedStatement(conn, "SELECT * FROM participations") { ps =>
      withResultSet(ps) { rs =>
        val buffer = scala.collection.mutable.ListBuffer.empty[Participation]
        while (rs.next()) {
          buffer += mapResultSetToParticipation(rs)
        }
        buffer.toList
      }
    }
  }
  
  /**
   * Find all friendships
   */
  def findAllFriendshipsSync(conn: Connection): List[UserFriend] = {
    withPreparedStatement(conn, "SELECT * FROM friends") { ps =>
      withResultSet(ps) { rs =>
        val buffer = scala.collection.mutable.ListBuffer.empty[UserFriend]
        while (rs.next()) {
          buffer += mapResultSetToFriendship(rs)
        }
        buffer.toList
      }
    }
  }
  
  /**
   * Get events by location
   */
  def getEventsByLocation(location: String): Future[List[Event]] = {
    val cacheKey = s"events:location:$location"
    getCachedValue(cacheKey) match {
      case Some(events: List[Event]) =>
        Future.successful(events)
      case _ =>
        txManager.withTransaction { conn =>
          val sql = """
            SELECT e.* FROM events e
            WHERE e.location = ? AND e.status = 'active'
            ORDER BY e.start_time ASC
          """
          
          val events = withPreparedStatement(conn, sql) { ps =>
            ps.setString(1, location)
            
            withResultSet(ps) { rs =>
              val buffer = scala.collection.mutable.ListBuffer.empty[Event]
              while (rs.next()) {
                buffer += mapResultSetToEvent(rs)
              }
              buffer.toList
            }
          }
          
          // Cache the results
          cacheValue(cacheKey, events, ShortCacheTTL)
          events
        }
    }
  }
  
  /**
   * Get user profile
   */
  def getUserProfile(userId: Long): Future[Option[User]] = {
    val cacheKey = s"users:$userId"
    getCachedValue(cacheKey) match {
      case Some(user: Option[User]) =>
        Future.successful(user)
      case _ =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM users WHERE id = ?"
          
          val userOpt = withPreparedStatement(conn, sql) { ps =>
            ps.setLong(1, userId)
            
            withResultSet(ps) { rs =>
              if (rs.next()) Some(mapResultSetToUser(rs)) else None
            }
          }
          
          // Cache the results
          cacheValue(cacheKey, userOpt, DefaultCacheTTL)
          userOpt
        }
    }
  }
  
  /**
   * Extract keywords from an event
   */
  def getEventKeywords(eventId: Long): Future[Map[String, Int]] = {
    txManager.withTransaction { conn =>
      val sql = "SELECT * FROM events WHERE id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, eventId)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val event = mapResultSetToEvent(rs)
            event.keywordFrequencies
          } else {
            Map.empty[String, Int]
          }
        }
      }
    }
  }
  
  /**
   * Helper methods for database operations
   */
  private def withPreparedStatement[T](connection: Connection, sql: String)(operation: PreparedStatement => T): T = {
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      operation(ps)
    } finally {
      if (ps != null) {
        try {
          ps.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close prepared statement: ${e.getMessage}", e)
        }
      }
    }
  }
  
  private def withResultSet[T](ps: PreparedStatement)(operation: ResultSet => T): T = {
    var rs: ResultSet = null
    try {
      rs = ps.executeQuery()
      operation(rs)
    } finally {
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close result set: ${e.getMessage}", e)
        }
      }
    }
  }
  
  // Mapping methods from ResultSet to domain objects
  private def mapResultSetToUser(rs: ResultSet): User = {
    // Implementation omitted for brevity
    User(
      id = rs.getLong("id"),
      name = rs.getString("name"),
      email = rs.getString("email"),
      preferences = Map.empty // Would extract from database in real implementation
    )
  }
  
  private def mapResultSetToEvent(rs: ResultSet): Event = {
    // Implementation omitted for brevity 
    Event(
      id = rs.getLong("id"),
      name = rs.getString("name"),
      description = rs.getString("description"),
      location = rs.getString("location"),
      startTime = rs.getTimestamp("start_time").toInstant,
      keywordFrequencies = Map.empty // Would extract from database in real implementation
    )
  }
  
  private def mapResultSetToParticipation(rs: ResultSet): Participation = {
    // Implementation omitted for brevity
    Participation(
      userId = rs.getLong("user_id"),
      eventId = rs.getLong("event_id"),
      status = rs.getString("status")
    )
  }
  
  private def mapResultSetToFriendship(rs: ResultSet): UserFriend = {
    // Implementation omitted for brevity
    UserFriend(
      userId = rs.getLong("user_id"),
      friendId = rs.getLong("friend_id"),
      strength = rs.getDouble("strength")
    )
  }
  
  // Cache methods
  private def getCachedValue[T](key: String): Option[T] = {
    val now = System.currentTimeMillis()
    cache.get(key).flatMap { entry =>
      if (entry.expiration > now) {
        Some(entry.value.asInstanceOf[T])
      } else {
        cache.remove(key)
        None
      }
    }
  }
  
  private def cacheValue[T](key: String, value: T, ttl: Duration = DefaultCacheTTL): Unit = {
    val expiration = System.currentTimeMillis() + ttl.toMillis
    cache.put(key, CacheEntry(value, expiration))
  }
  
  /**
   * Close resources when shutting down
   */
  def shutdown(): Unit = {
    logger.info("Shutting down DataManager...")
    
    // Close RabbitMQ channels
    historicalDataChannel.foreach(ch => if (ch.isOpen) ch.close())
    recommenderSyncChannel.foreach(ch => if (ch.isOpen) ch.close())
    
    // Close transaction manager
    txManager.close()
    
    logger.info("DataManager shut down")
  }
}

/**
 * Companion object for DataManager creation
 */
object DataManager {
  def apply()(implicit ec: ExecutionContext): DataManager = {
    val manager = new DataManager()
    manager.start()
    manager
  }
}