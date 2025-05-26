package application.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.{DataManagerConfigSection, ConfigurationService}
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import io.circe.parser.decode
import io.circe.generic.auto._
import io.circe.syntax._
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.{ExecutionContext, Future, Await, Promise}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import utils.metrics.{MetricsRegistry, HealthCheckRegistry, SystemStatus, HealthStatus, HealthCheckResult}
import utils.error.DatabaseException
import java.time.Instant

/**
 * DataManager component responsible for all data access operations.
 * Single access point for all database operations, consolidating all repository functionality.
 */
class DataManager(configService: ConfigurationService = new ConfigurationService())(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "data-manager"
  private val initialized = new AtomicBoolean(false)
  private val isShutdown = new AtomicBoolean(false)
  
  // Config
  private val dbConfig = configService.get[DataManagerConfigSection]
  
  // Connection pool
  private lazy val dataSource: HikariDataSource = createDataSource()
  
  // Transaction manager for database operations
  private val txManager = new DataManagerTransactions(this)
  
  // RabbitMQ channels
  private var historicalDataChannel: Option[Channel] = None
  private var recommenderSyncChannel: Option[Channel] = None
  
  // Metrics
  private val dataLoadCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "data", "load_count"),
    "Number of data load operations",
    "operations"
  )
  
  private val dataLoadErrorCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "data", "load_error_count"),
    "Number of data load errors",
    "errors"
  )
  
  private val cacheHitCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "cache", "hit_count"),
    "Number of cache hits",
    "hits"
  )
  
  private val cacheMissCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "cache", "miss_count"),
    "Number of cache misses",
    "misses"
  )
  
  private val connectionUsageCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "connection", "usage"),
    "Number of database connections used",
    "connections"
  )
  
  private val connectionErrorCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "connection", "error"),
    "Number of database connection errors",
    "errors"
  )
  
  private val activeConnectionsGauge = MetricsRegistry.buildGauge(
    MetricsRegistry.MetricMetadata(componentName, "pool", "active_connections"),
    "Number of active connections in the pool",
    "connections"
  )
  private val idleConnectionsGauge = MetricsRegistry.buildGauge(
    MetricsRegistry.MetricMetadata(componentName, "pool", "idle_connections"),
    "Number of idle connections in the pool",
    "connections"
  )
  private val waitingThreadsGauge = MetricsRegistry.buildGauge(
    MetricsRegistry.MetricMetadata(componentName, "pool", "waiting_threads"),
    "Number of threads waiting for a connection",
    "threads"
  )
  private val connectionTimeout = MetricsRegistry.buildHistogram(
    MetricsRegistry.MetricMetadata(componentName, "connection", "timeout"),
    "Connection acquisition timeout distribution",
    "milliseconds"
  )
  private val queryExecutionTime = MetricsRegistry.buildHistogram(
    MetricsRegistry.MetricMetadata(componentName, "query", "execution_time"),
    "Query execution time distribution",
    "milliseconds"
  )
  
  // Cache mechanism
  private case class CacheEntry[T](value: T, expiration: Long)
  private val cache = scala.collection.concurrent.TrieMap[String, CacheEntry[Any]]()
  private val DefaultCacheTTL = dbConfig.defaultCacheTTL
  private val ShortCacheTTL = dbConfig.shortCacheTTL
  private val LongCacheTTL = 30.minutes

  // Helper methods for caching
  private def cacheKey(prefix: String, id: Any): String = s"$prefix-$id"
  
  private def getFromCache[T](prefix: String, id: Any): Option[T] = {
    val key = cacheKey(prefix, id)
    cache.get(key) match {
      case Some(entry) if entry.expiration > System.currentTimeMillis() =>
        cacheHitCounter.inc()
        Some(entry.value.asInstanceOf[T])
      case Some(entry) =>
        // Expired entry, remove it
        cache.remove(key)
        cacheMissCounter.inc()
        None
      case None =>
        cacheMissCounter.inc()
        None
    }
  }
  
  private def putInCache[T](prefix: String, id: Any, value: T, ttl: Duration = DefaultCacheTTL): T = {
    val key = cacheKey(prefix, id)
    val expiration = System.currentTimeMillis() + ttl.toMillis
    cache.put(key, CacheEntry(value, expiration))
    value
  }
  
  private def invalidateCache(prefix: String, id: Any): Unit = {
    val key = cacheKey(prefix, id)
    cache.remove(key)
  }
  
  // Méthodes utilitaires pour les opérations SQL
  private def withPreparedStatement[T](connection: Connection, sql: String)(f: PreparedStatement => T): T = {
    val stmt = connection.prepareStatement(sql)
    try {
      f(stmt)
    } finally {
      stmt.close()
    }
  }

  private def withResultSet[T](stmt: PreparedStatement)(f: ResultSet => T): T = {
    val rs = stmt.executeQuery()
    try {
      f(rs)
    } finally {
      rs.close()
    }
  }
  
  /**
   * Crée et configure une source de données HikariCP
   */
  private def createDataSource(): HikariDataSource = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(dbConfig.jdbcUrl)
    hikariConfig.setUsername(dbConfig.username)
    hikariConfig.setPassword(dbConfig.password)
    hikariConfig.setMaximumPoolSize(dbConfig.maxPoolSize)
    hikariConfig.setMinimumIdle(dbConfig.minIdle)
    hikariConfig.setIdleTimeout(dbConfig.idleTimeout)
    hikariConfig.setConnectionTimeout(dbConfig.connectionTimeout)
    hikariConfig.setMaxLifetime(dbConfig.maxLifetime)
    hikariConfig.setPoolName("recommendation-engine-pool")
    
    new HikariDataSource(hikariConfig)
  }
  
  /**
   * Obtient une connexion depuis le pool
   */
  def getConnection(): Connection = {
    connectionUsageCounter.inc()
    try {
      dataSource.getConnection()
    } catch {
      case e: SQLException =>
        connectionErrorCounter.inc()
        throw DatabaseException(s"Failed to get database connection: ${e.getMessage}", e)
    }
  }
  
  // CRUD operations for User
  
  /**
   * Get user by ID
   */
  def getUserProfile(userId: Long): Future[Option[User]] = {
    getFromCache[Option[User]]("user", userId) match {
      case Some(cachedUser) => Future.successful(cachedUser)
      case None => 
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM users WHERE user_id = ?"
          withPreparedStatement(conn, sql) { ps =>
            ps.setLong(1, userId)
            withResultSet(ps) { rs =>
              val user = if (rs.next()) {
                Some(mapResultSetToUser(rs))
              } else {
                None
              }
              putInCache("user", userId, user, ShortCacheTTL)
              user
            }
          }
        }
    }
  }
  
  /**
   * Create or update a user
   */
  def upsertUser(user: User): Future[User] = {
    txManager.withTransaction { conn =>
      val sql = if (user.userId <= 0) {
        """INSERT INTO users(name, email_address, locale, birth_year, gender, location, timezone, is_generated) 
           VALUES(?, ?, ?, ?, ?, ?, ?, ?) RETURNING user_id"""
      } else {
        """UPDATE users SET name = ?, email_address = ?, locale = ?, birth_year = ?, gender = ?, 
           location = ?, timezone = ?, is_generated = ? WHERE user_id = ? RETURNING user_id"""
      }
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setString(1, user.name.orNull)
        ps.setString(2, user.emailAddress.orNull)
        ps.setString(3, user.locale)
        user.birthYear match {
          case Some(year) => ps.setInt(4, year)
          case None => ps.setNull(4, java.sql.Types.INTEGER)
        }
        ps.setString(5, user.gender.orNull)
        ps.setString(6, user.location.orNull)
        user.timezone match {
          case Some(tz) => ps.setInt(7, tz)
          case None => ps.setNull(7, java.sql.Types.INTEGER)
        }
        ps.setBoolean(8, user.isGenerated)
        
        if (user.userId > 0) {
          ps.setLong(9, user.userId)
        }
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val newId = rs.getLong(1)
            val updatedUser = user.copy(userId = newId)
            // Invalidate cache
            invalidateCache("user", newId)
            updatedUser
          } else {
            user
          }
        }
      }
    }
  }
  
  /**
   * Get users by IDs (batch operation)
   */
  def getUsersByIds(userIds: Seq[Long]): Future[Map[Long, User]] = {
    if (userIds.isEmpty) {
      return Future.successful(Map.empty)
    }
    
    // Check cache first
    val fromCache = userIds.flatMap(id => getFromCache[Option[User]]("user", id).flatten.map(u => id -> u))
    val cachedMap = fromCache.toMap
    val missingIds = userIds.filterNot(cachedMap.contains)
    
    if (missingIds.isEmpty) {
      return Future.successful(cachedMap)
    }
    
    // Fetch missing users
    txManager.withTransaction { conn =>
      val placeholders = missingIds.map(_ => "?").mkString(",")
      val sql = s"SELECT * FROM users WHERE user_id IN ($placeholders)"
      
      withPreparedStatement(conn, sql) { ps =>
        missingIds.zipWithIndex.foreach { case (id, i) =>
          ps.setLong(i + 1, id)
        }
        
        withResultSet(ps) { rs =>
          val users = scala.collection.mutable.Map.empty[Long, User]
          while (rs.next()) {
            val user = mapResultSetToUser(rs)
            users(user.userId) = user
            putInCache("user", user.userId, Some(user), ShortCacheTTL)
          }
          
          // Combine with cached results
          cachedMap ++ users
        }
      }
    }
  }
  
  /**
   * Delete a user by ID
   */
  def deleteUser(userId: Long): Future[Boolean] = {
    txManager.withTransaction { conn =>
      val sql = "DELETE FROM users WHERE user_id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        val count = ps.executeUpdate()
        invalidateCache("user", userId)
        count > 0
      }
    }
  }
  
  // CRUD operations for Event
  
  /**
   * Get event by ID
   */
  def getEvent(eventId: Long): Future[Option[Event]] = {
    getFromCache[Option[Event]]("event", eventId) match {
      case Some(cachedEvent) => Future.successful(cachedEvent)
      case None =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM events WHERE id = ?"
          
          withPreparedStatement(conn, sql) { ps =>
            ps.setLong(1, eventId)
            withResultSet(ps) { rs =>
              val event = if (rs.next()) {
                Some(mapResultSetToEvent(rs))
              } else {
                None
              }
              putInCache("event", eventId, event, ShortCacheTTL)
              event
            }
          }
        }
    }
  }
  
  /**
   * Get all active events
   */
  def getAllActiveEvents(): Future[List[Event]] = {
    getFromCache[List[Event]]("events", "active") match {
      case Some(cachedEvents) => Future.successful(cachedEvents)
      case None =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM events WHERE status = 'active' AND start_time > ?"
          
          withPreparedStatement(conn, sql) { ps =>
            // Events starting within the next 30 days
            ps.setLong(1, System.currentTimeMillis() - 86400000) // 1 day ago (to include ongoing events)
            
            withResultSet(ps) { rs =>
              val events = scala.collection.mutable.ListBuffer.empty[Event]
              while (rs.next()) {
                val event = mapResultSetToEvent(rs)
                events += event
                // Also cache individual events
                event.id.foreach(id => putInCache("event", id, Some(event), ShortCacheTTL))
              }
              val eventsList = events.toList
              putInCache("events", "active", eventsList, ShortCacheTTL)
              eventsList
            }
          }
        }
    }
  }
  
  /**
   * Create or update an event
   */
  def upsertEvent(event: Event): Future[Event] = {
    txManager.withTransaction { conn =>
      val sql = if (event.id.isEmpty) {
        """INSERT INTO events(name, description, start_time, end_time, location, created_by, status) 
           VALUES(?, ?, ?, ?, ?, ?, ?) RETURNING id"""
      } else {
        """UPDATE events SET name = ?, description = ?, start_time = ?, end_time = ?, 
           location = ?, status = ? WHERE id = ? RETURNING id"""
      }
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setString(1, event.name)
        ps.setString(2, event.description.orNull)
        ps.setLong(3, event.startTime)
        event.endTime match {
          case Some(time) => ps.setLong(4, time)
          case None => ps.setNull(4, java.sql.Types.BIGINT)
        }
        ps.setString(5, event.location.orNull)
        
        if (event.id.isEmpty) {
          ps.setLong(6, event.createdBy)
          ps.setString(7, event.status)
        } else {
          ps.setString(6, event.status)
          ps.setLong(7, event.id.get)
        }
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val newId = rs.getLong(1)
            val updatedEvent = event.copy(id = Some(newId))
            
            // Update participants if provided
            if (event.participants.nonEmpty) {
              updateEventParticipants(conn, newId, event.participants)
            }
            
            // Invalidate cache
            invalidateCache("event", newId)
            invalidateCache("events", "active")
            
            updatedEvent
          } else {
            event
          }
        }
      }
    }
  }
  
  /**
   * Update event participants
   */
  private def updateEventParticipants(conn: Connection, eventId: Long, participants: Set[Long]): Unit = {
    // First delete existing participants
    val deleteSql = "DELETE FROM event_participants WHERE event_id = ?"
    withPreparedStatement(conn, deleteSql) { ps =>
      ps.setLong(1, eventId)
      ps.executeUpdate()
    }
    
    // Then insert new participants
    if (participants.nonEmpty) {
      val insertSql = "INSERT INTO event_participants(event_id, user_id) VALUES(?, ?)"
      withPreparedStatement(conn, insertSql) { ps =>
        participants.foreach { userId =>
          ps.setLong(1, eventId)
          ps.setLong(2, userId)
          ps.addBatch()
        }
        ps.executeBatch()
      }
    }
  }
  
  /**
   * Delete an event
   */
  def deleteEvent(eventId: Long): Future[Boolean] = {
    txManager.withTransaction { conn =>
      val sql = "DELETE FROM events WHERE id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, eventId)
        val count = ps.executeUpdate()
        
        // Also delete participants
        if (count > 0) {
          deleteEventParticipants(conn, eventId)
        }
        
        // Invalidate cache
        invalidateCache("event", eventId)
        invalidateCache("events", "active")
        
        count > 0
      }
    }
  }
  
  /**
   * Delete event participants
   */
  private def deleteEventParticipants(conn: Connection, eventId: Long): Unit = {
    val sql = "DELETE FROM event_participants WHERE event_id = ?"
    withPreparedStatement(conn, sql) { ps =>
      ps.setLong(1, eventId)
      ps.executeUpdate()
    }
  }
  
  /**
   * Get events by creator user ID
   */
  def getEventsByCreator(userId: Long): Future[List[Event]] = {
    txManager.withTransaction { conn =>
      val sql = "SELECT * FROM events WHERE created_by = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        
        withResultSet(ps) { rs =>
          val events = scala.collection.mutable.ListBuffer.empty[Event]
          while (rs.next()) {
            events += mapResultSetToEvent(rs)
          }
          events.toList
        }
      }
    }
  }
  
  // CRUD operations for UserEvent and ProcessedEvent
  
  /**
   * Store a raw user event
   */
  def storeUserEvent(event: UserEvent): Future[UserEvent] = {
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO user_events(user_id, event_type, item_id, properties, timestamp) 
                   VALUES(?, ?, ?, ?, ?) RETURNING id"""
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, event.userId)
        ps.setString(2, event.eventType)
        event.itemId match {
          case Some(itemId) => ps.setLong(3, itemId)
          case None => ps.setNull(3, java.sql.Types.BIGINT)
        }
        ps.setString(4, event.properties.map(p => io.circe.Encoder.encodeMap[String, String].apply(p).noSpaces).orNull)
        ps.setLong(5, event.timestamp)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val id = rs.getLong(1)
            event
          } else {
            event
          }
        }
      }
    }
  }
  
  /**
   * Store a processed event
   */
  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO processed_events(user_id, event_type, item_id, properties, timestamp, processing_timestamp) 
                   VALUES(?, ?, ?, ?, ?, ?) RETURNING id"""
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, event.userId)
        ps.setString(2, event.eventType)
        event.itemId match {
          case Some(itemId) => ps.setLong(3, itemId)
          case None => ps.setNull(3, java.sql.Types.BIGINT)
        }
        ps.setString(4, io.circe.Encoder.encodeMap[String, String].apply(event.properties).noSpaces)
        ps.setLong(5, event.timestamp)
        ps.setLong(6, event.processingTimestamp)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val id = rs.getLong(1)
            event.copy(id = Some(id))
          } else {
            event
          }
        }
      }
    }
  }
  
  /**
   * Get processed events for a user
   */
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    txManager.withTransaction { conn =>
      val sql = "SELECT * FROM processed_events WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        ps.setInt(2, limit)
        
        withResultSet(ps) { rs =>
          val events = scala.collection.mutable.ListBuffer.empty[ProcessedEvent]
          while (rs.next()) {
            events += mapResultSetToProcessedEvent(rs)
          }
          events.toList
        }
      }
    }
  }
  
  /**
   * Get recent processed events
   */
  def getRecentProcessedEvents(limit: Int = 100): Future[List[ProcessedEvent]] = {
    getFromCache[List[ProcessedEvent]]("events", "recent") match {
      case Some(events) => Future.successful(events)
      case None =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM processed_events ORDER BY timestamp DESC LIMIT ?"
          
          withPreparedStatement(conn, sql) { ps =>
            ps.setInt(1, limit)
            
            withResultSet(ps) { rs =>
              val events = scala.collection.mutable.ListBuffer.empty[ProcessedEvent]
              while (rs.next()) {
                events += mapResultSetToProcessedEvent(rs)
              }
              val eventsList = events.toList
              putInCache("events", "recent", eventsList, ShortCacheTTL)
              eventsList
            }
          }
        }
    }
  }
  
  /**
   * Batch store processed events
   */
  def batchStoreProcessedEvents(events: List[ProcessedEvent]): Future[Int] = {
    if (events.isEmpty) {
      return Future.successful(0)
    }
    
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO processed_events(user_id, event_type, item_id, properties, timestamp, processing_timestamp) 
                   VALUES(?, ?, ?, ?, ?, ?)"""
      
      withPreparedStatement(conn, sql) { ps =>
        events.foreach { event =>
          ps.setLong(1, event.userId)
          ps.setString(2, event.eventType)
          event.itemId match {
            case Some(itemId) => ps.setLong(3, itemId)
            case None => ps.setNull(3, java.sql.Types.BIGINT)
          }
          ps.setString(4, io.circe.Encoder.encodeMap[String, String].apply(event.properties).noSpaces)
          ps.setLong(5, event.timestamp)
          ps.setLong(6, event.processingTimestamp)
          ps.addBatch()
        }
        
        // Execute batch and sum the results
        ps.executeBatch().sum
      }
    }
  }
  
  // CRUD operations for Friends
  
  /**
   * Get user friends
   */
  def getUserFriends(userId: Long): Future[List[UserFriend]] = {
    getFromCache[List[UserFriend]]("friends", userId) match {
      case Some(friends) => Future.successful(friends)
      case None =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM user_friends WHERE user_id = ?"
          
          withPreparedStatement(conn, sql) { ps =>
            ps.setLong(1, userId)
            
            withResultSet(ps) { rs =>
              val friends = scala.collection.mutable.ListBuffer.empty[UserFriend]
              while (rs.next()) {
                friends += mapResultSetToUserFriend(rs)
              }
              val friendsList = friends.toList
              putInCache("friends", userId, friendsList, ShortCacheTTL)
              friendsList
            }
          }
        }
    }
  }
  
  /**
   * Update user friends
   */
  def upsertUserFriends(userFriends: List[UserFriend]): Future[Int] = {
    if (userFriends.isEmpty) {
      return Future.successful(0)
    }
    
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO user_friends(user_id, friend_id, strength) 
                   VALUES(?, ?, ?) ON CONFLICT(user_id, friend_id) 
                   DO UPDATE SET strength = EXCLUDED.strength"""
      
      withPreparedStatement(conn, sql) { ps =>
        userFriends.foreach { friend =>
          ps.setLong(1, friend.userId)
          ps.setLong(2, friend.friendId)
          ps.setDouble(3, friend.strength)
          ps.addBatch()
        }
        
        // Track user IDs for cache invalidation
        val userIds = userFriends.map(_.userId).distinct
        
        // Execute batch and invalidate cache
        val result = ps.executeBatch().sum
        
        // Invalidate cache for each affected user
        userIds.foreach(id => invalidateCache("friends", id))
        
        result
      }
    }
  }
  
  // CRUD operations for Participation
  
  /**
   * Add event participation
   */
  def addEventParticipation(participation: Participation): Future[Boolean] = {
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO event_participations(user_id, event_id, status) 
                   VALUES(?, ?, ?) ON CONFLICT(user_id, event_id) 
                   DO UPDATE SET status = EXCLUDED.status"""
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, participation.userId)
        ps.setLong(2, participation.eventId)
        ps.setString(3, participation.status)
        
        val count = ps.executeUpdate()
        
        // Invalidate cache
        invalidateCache("event", participation.eventId)
        invalidateCache("events", "active")
        
        count > 0
      }
    }
  }
  
  /**
   * Get event participations by event ID
   */
  def getEventParticipations(eventId: Long): Future[List[Participation]] = {
    txManager.withTransaction { conn =>
      val sql = "SELECT user_id, event_id, status FROM event_participations WHERE event_id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, eventId)
        
        withResultSet(ps) { rs =>
          val participations = scala.collection.mutable.ListBuffer.empty[Participation]
          while (rs.next()) {
            participations += mapResultSetToParticipation(rs)
          }
          participations.toList
        }
      }
    }
  }
  
  /**
   * Get user participations by user ID
   */
  def getUserParticipations(userId: Long): Future[List[Participation]] = {
    txManager.withTransaction { conn =>
      val sql = "SELECT user_id, event_id, status FROM event_participations WHERE user_id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        
        withResultSet(ps) { rs =>
          val participations = scala.collection.mutable.ListBuffer.empty[Participation]
          while (rs.next()) {
            participations += mapResultSetToParticipation(rs)
          }
          participations.toList
        }
      }
    }
  }
  
  // CRUD operations for Recommendations
  
  /**
   * Store a recommendation
   */
  def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO recommendations(user_id, score, timestamp) 
                   VALUES(?, ?, ?) RETURNING id"""
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, recommendation.userId)
        ps.setDouble(2, recommendation.score)
        ps.setLong(3, recommendation.timestamp)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val id = rs.getLong(1)
            
            // Store recommended items
            storeRecommendedItems(conn, id, recommendation.recommendedItems)
            
            recommendation.copy(id = Some(id))
          } else {
            recommendation
          }
        }
      }
    }
  }
  
  /**
   * Store recommended items for a recommendation
   */
  private def storeRecommendedItems(conn: Connection, recommendationId: Long, items: List[RecommendedItem]): Int = {
    if (items.isEmpty) {
      return 0
    }
    
    val sql = """INSERT INTO recommended_items(recommendation_id, item_id, score, reason) 
                 VALUES(?, ?, ?, ?)"""
    
    withPreparedStatement(conn, sql) { ps =>
      items.foreach { item =>
        ps.setLong(1, recommendationId)
        ps.setLong(2, item.itemId)
        ps.setDouble(3, item.score)
        ps.setString(4, item.reason)
        ps.addBatch()
      }
      
      ps.executeBatch().sum
    }
  }
  
  /**
   * Get recommendation for user
   */
  def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    getFromCache[Option[Recommendation]]("recommendation", userId) match {
      case Some(recommendation) => Future.successful(recommendation)
      case None =>
        txManager.withTransaction { conn =>
          val sql = "SELECT * FROM recommendations WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1"
          
          withPreparedStatement(conn, sql) { ps =>
            ps.setLong(1, userId)
            
            withResultSet(ps) { rs =>
              if (rs.next()) {
                val recId = rs.getLong("id")
                val rec = mapResultSetToRecommendation(rs)
                
                // Get recommended items
                val items = getRecommendedItems(conn, recId)
                val fullRec = rec.copy(recommendedItems = items)
                
                val result = Some(fullRec)
                putInCache("recommendation", userId, result, ShortCacheTTL)
                result
              } else {
                None
              }
            }
          }
        }
    }
  }
  
  /**
   * Get recent recommendation for user within time window
   */
  def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = {
    val cutoffTime = System.currentTimeMillis() - timeWindow
    
    txManager.withTransaction { conn =>
      val sql = "SELECT * FROM recommendations WHERE user_id = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT 1"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        ps.setLong(2, cutoffTime)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val recId = rs.getLong("id")
            val rec = mapResultSetToRecommendation(rs)
            
            // Get recommended items
            val items = getRecommendedItems(conn, recId)
            Some(rec.copy(recommendedItems = items))
          } else {
            None
          }
        }
      }
    }
  }
  
  /**
   * Get recommended items for a recommendation
   */
  private def getRecommendedItems(conn: Connection, recommendationId: Long): List[RecommendedItem] = {
    val sql = "SELECT * FROM recommended_items WHERE recommendation_id = ? ORDER BY score DESC"
    
    withPreparedStatement(conn, sql) { ps =>
      ps.setLong(1, recommendationId)
      
      withResultSet(ps) { rs =>
        val items = scala.collection.mutable.ListBuffer.empty[RecommendedItem]
        while (rs.next()) {
          items += mapResultSetToRecommendedItem(rs)
        }
        items.toList
      }
    }
  }
  
  // Mapping methods
  
  private def mapResultSetToUser(rs: ResultSet): User = {
    User(
      userId = rs.getLong("user_id"),
      name = Option(rs.getString("name")),
      emailAddress = Option(rs.getString("email_address")),
      locale = rs.getString("locale"),
      birthYear = Option(rs.getObject("birth_year")).map(_.asInstanceOf[Integer].toInt),
      gender = Option(rs.getString("gender")),
      location = Option(rs.getString("location")),
      timezone = Option(rs.getObject("timezone")).map(_.asInstanceOf[Integer].toInt),
      isGenerated = rs.getBoolean("is_generated")
    )
  }
  
  private def mapResultSetToEvent(rs: ResultSet): Event = {
    Event(
      id = Some(rs.getLong("id")),
      name = rs.getString("name"),
      description = Option(rs.getString("description")),
      startTime = rs.getLong("start_time"),
      endTime = Option(rs.getObject("end_time")).map(_.asInstanceOf[java.lang.Long]),
      location = Option(rs.getString("location")),
      createdBy = rs.getLong("created_by"),
      status = rs.getString("status"),
      keywords = Option(rs.getString("keywords")).map(_.split(",").toList).getOrElse(List.empty),
      keywordFrequencies = Map.empty,  // Would need separate query for keyword frequencies
      participants = Set.empty  // Would need separate query for participants
    )
  }
  
  private def mapResultSetToProcessedEvent(rs: ResultSet): ProcessedEvent = {
    val propertiesJson = Option(rs.getString("properties")).getOrElse("{}")
    val properties = io.circe.parser.parse(propertiesJson).flatMap(_.as[Map[String, String]]).getOrElse(Map.empty)
    
    ProcessedEvent(
      id = Some(rs.getLong("id")),
      userId = rs.getLong("user_id"),
      eventType = rs.getString("event_type"),
      itemId = Option(rs.getObject("item_id")).map(_.asInstanceOf[java.lang.Long]),
      properties = properties,
      timestamp = rs.getLong("timestamp"),
      processingTimestamp = rs.getLong("processing_timestamp")
    )
  }
  
  private def mapResultSetToUserFriend(rs: ResultSet): UserFriend = {
    UserFriend(
      userId = rs.getLong("user_id"),
      friendId = rs.getLong("friend_id"),
      strength = rs.getDouble("strength")
    )
  }
  
  private def mapResultSetToParticipation(rs: ResultSet): Participation = {
    Participation(
      userId = rs.getLong("user_id"),
      eventId = rs.getLong("event_id"),
      status = rs.getString("status")
    )
  }
  
  private def mapResultSetToRecommendation(rs: ResultSet): Recommendation = {
    Recommendation(
      id = Some(rs.getLong("id")),
      userId = rs.getLong("user_id"),
      recommendedItems = List.empty,  // Will be populated separately
      score = rs.getDouble("score"),
      timestamp = rs.getLong("timestamp")
    )
  }
  
  private def mapResultSetToRecommendedItem(rs: ResultSet): RecommendedItem = {
    RecommendedItem(
      itemId = rs.getLong("item_id"),
      score = rs.getDouble("score"),
      reason = rs.getString("reason")
    )
  }
  
  // DDL operations
  
  /**
   * Initialize database tables if they don't exist
   */
  def initTables(): Future[Unit] = {
    txManager.withTransaction { conn =>
      val createTables = List(
        """CREATE TABLE IF NOT EXISTS users (
          |  user_id BIGSERIAL PRIMARY KEY,
          |  name VARCHAR(255),
          |  email_address VARCHAR(255),
          |  locale VARCHAR(10) NOT NULL DEFAULT 'en_US',
          |  birth_year INTEGER,
          |  gender VARCHAR(10),
          |  location VARCHAR(255),
          |  timezone INTEGER,
          |  is_generated BOOLEAN NOT NULL DEFAULT FALSE
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS events (
          |  id BIGSERIAL PRIMARY KEY,
          |  name VARCHAR(255) NOT NULL,
          |  description TEXT,
          |  start_time BIGINT NOT NULL,
          |  end_time BIGINT,
          |  location VARCHAR(255),
          |  created_by BIGINT NOT NULL,
          |  status VARCHAR(50) NOT NULL DEFAULT 'active',
          |  keywords VARCHAR(255)
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS event_participants (
          |  event_id BIGINT NOT NULL,
          |  user_id BIGINT NOT NULL,
          |  PRIMARY KEY (event_id, user_id)
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS user_events (
          |  id BIGSERIAL PRIMARY KEY,
          |  user_id BIGINT NOT NULL,
          |  event_type VARCHAR(50) NOT NULL,
          |  item_id BIGINT,
          |  properties JSONB,
          |  timestamp BIGINT NOT NULL
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS processed_events (
          |  id BIGSERIAL PRIMARY KEY,
          |  user_id BIGINT NOT NULL,
          |  event_type VARCHAR(50) NOT NULL,
          |  item_id BIGINT,
          |  properties JSONB NOT NULL DEFAULT '{}',
          |  timestamp BIGINT NOT NULL,
          |  processing_timestamp BIGINT NOT NULL
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS user_friends (
          |  user_id BIGINT NOT NULL,
          |  friend_id BIGINT NOT NULL,
          |  strength FLOAT NOT NULL DEFAULT 1.0,
          |  PRIMARY KEY (user_id, friend_id)
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS event_participations (
          |  user_id BIGINT NOT NULL,
          |  event_id BIGINT NOT NULL,
          |  status VARCHAR(50) NOT NULL,
          |  PRIMARY KEY (user_id, event_id)
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS recommendations (
          |  id BIGSERIAL PRIMARY KEY,
          |  user_id BIGINT NOT NULL,
          |  score FLOAT NOT NULL,
          |  timestamp BIGINT NOT NULL
          |)""".stripMargin,
          
        """CREATE TABLE IF NOT EXISTS recommended_items (
          |  recommendation_id BIGINT NOT NULL,
          |  item_id BIGINT NOT NULL,
          |  score FLOAT NOT NULL,
          |  reason VARCHAR(255),
          |  PRIMARY KEY (recommendation_id, item_id)
          |)""".stripMargin,
          
        """CREATE INDEX IF NOT EXISTS idx_processed_events_user_id ON processed_events(user_id)""",
        """CREATE INDEX IF NOT EXISTS idx_user_friends_user_id ON user_friends(user_id)""",
        """CREATE INDEX IF NOT EXISTS idx_event_participants_event_id ON event_participants(event_id)""",
        """CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON recommendations(user_id)""",
        """CREATE INDEX IF NOT EXISTS idx_events_status_start_time ON events(status, start_time)"""
      )
      
      createTables.foreach { sql =>
        withPreparedStatement(conn, sql) { ps =>
          ps.execute()
        }
      }
      
      logger.info("Database tables initialized")
      ()
    }
  }
}
