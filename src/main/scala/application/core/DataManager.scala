package application.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
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
class DataManager(
  configService: ConfigurationServiceInterface,
  dbProvider: DatabaseConfigProvider
)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "data-manager"
  private val initialized = new AtomicBoolean(false)
  private val isShutdown = new AtomicBoolean(false)
  
  // Gestionnaire de transactions
  private val txManager = new DataManagerTransactions(this)(ec)
  
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
  private val DefaultCacheTTL = 30.minutes
  private val ShortCacheTTL = 5.minutes
  private val LongCacheTTL = 30.minutes

  // Helper methods for caching
  private def cacheKey(prefix: String, id: Any): String = s"$prefix:$id"
  
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
  
  /**
   * Helper method to store data in cache
   */
  private def putInCache[T](prefix: String, key: Any, value: T, ttl: scala.concurrent.duration.Duration): Unit = {
    try {
      val cacheKey = this.cacheKey(prefix, key)  // Use the cacheKey method instead of string interpolation
      logger.debug(s"Adding to cache: $cacheKey with TTL $ttl ms")
      cache.put(cacheKey, CacheEntry(value, System.currentTimeMillis() + ttl.toMillis))
    } catch {
      case e: Exception =>
        logger.error(s"Error adding to cache: ${e.getMessage}", e)
    }
  }
  
  def invalidateCache(prefix: String, id: Any): Unit = {
    val key = cacheKey(prefix, id)
    cache.remove(key)
  }

  /**
   * Clear the cache, primarily for testing purposes.
   */
  def clearCache(): Unit = {
    cache.clear()
    logger.debug("Cache cleared for testing")
  }
  
  // Méthodes utilitaires pour les opérations SQL
  /**
   * Helper method to execute operations with a prepared statement and clean up resources
   */
  private def withPreparedStatement[T](conn: Connection, sql: String)(block: PreparedStatement => T): T = {
    val ps = conn.prepareStatement(sql)
    try {
      if (ps == null) {
        throw new SQLException(s"Failed to create PreparedStatement for SQL: $sql")
      }
      block(ps)
    } finally {
      if (ps != null) {
        ps.close()
      }
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
  
  private def withConnection[T](f: Connection => T): T = {
    val conn = getConnection()
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }
  
  /**
   * Obtient une connexion depuis le pool
   */
  def getConnection(): Connection = {
    connectionUsageCounter.inc()
    try {
      dbProvider.getConnection()
    } catch {
      case e: SQLException =>
        connectionErrorCounter.inc()
        throw DatabaseException(s"Failed to get database connection: ${e.getMessage}", e)
    }
  }
  
  private def handleDatabaseOperation[T](operation: => Future[T]): Future[T] = {
    val future = try {
      operation
    } catch {
      case e: SQLException =>
        Future.failed(DatabaseException(
          s"Database operation failed: ${e.getMessage}", 
          e, 
          Map("component" -> componentName)
        ))
      case e: Exception =>
        Future.failed(DatabaseException(
          s"Unexpected error during database operation: ${e.getMessage}", 
          e, 
          Map("component" -> componentName)
        ))
    }
    
    future.recoverWith {
      case e: SQLException =>
        Future.failed(DatabaseException(
          s"Database operation failed: ${e.getMessage}", 
          e, 
          Map("component" -> componentName)
        ))
      case e: Exception =>
        Future.failed(DatabaseException(
          s"Unexpected error during database operation: ${e.getMessage}", 
          e, 
          Map("component" -> componentName)
        ))
    }
  }

  // CRUD operations for User
  
  /**
   * Get user profile with all related information
   */
  def getUserProfile(userId: Long): Future[Option[User]] = {
    getFromCache[Option[User]]("user", userId) match {
      case Some(cachedUser) => 
        logger.debug(s"Using cached user profile for user $userId")
        Future.successful(cachedUser)
      case None =>
        logger.debug(s"Cache miss for user $userId, fetching from database")
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
              
              // Make sure to store in cache, even if None
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
            putInCache("user", newId, Some(updatedUser), LongCacheTTL)
            updatedUser
          } else {
            user
          }
        }
      }
    }
  }
  
  /**
   * Update a user
   */
  def updateUser(user: User): Future[User] = {
    txManager.withTransaction { conn =>
      val sql = "UPDATE users SET name = ?, email_address = ?, locale = ?, birth_year = ?, gender = ?, location = ?, timezone = ?, is_generated = ? WHERE user_id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setString(1, user.name.orNull)                    // Fix: use .orNull for Option[String]
        ps.setString(2, user.emailAddress.orNull)           // Fix: use emailAddress instead of email
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
        ps.setLong(9, user.userId)
        
        val rowsUpdated = ps.executeUpdate()
        
        if (rowsUpdated > 0) {
          // Update cache
          putInCache("user", user.userId, Some(user), LongCacheTTL)
          user
        } else {
          throw new SQLException(s"User with ID ${user.userId} not found for update")
        }
      }
    }
  }
  
  /**
   * Create a new user
   */
  def createUser(user: User): Future[User] = {
    txManager.withTransaction { conn =>
      val sql = "INSERT INTO users(name, email_address, locale, birth_year, gender, location, timezone, is_generated) VALUES(?, ?, ?, ?, ?, ?, ?, ?) RETURNING user_id"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setString(1, user.name.orNull)           // Fix: use .orNull for Option[String]
        ps.setString(2, user.emailAddress.orNull)   // Fix: use emailAddress instead of email
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
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val userId = rs.getLong(1)
            val createdUser = user.copy(userId = userId)
            
            // Cache the created user
            putInCache("user", userId, Some(createdUser), LongCacheTTL)
            
            createdUser
          } else {
            throw new SQLException("Failed to create user - no ID returned")
          }
        }
      }
    }
  }
  
  /**
   * Get users by IDs in batch
   */
  def getUsersByIds(userIds: Seq[Long]): Future[Map[Long, User]] = {
    if (userIds.isEmpty) {
      return Future.successful(Map.empty[Long, User])
    }

    // On vérifie d'abord le cache
    val cachedUsers: Map[Long, User] = userIds.flatMap { id =>
      getFromCache[Option[User]]("user", id).flatten.map(user => id -> user)
    }.toMap

    // Si on a trouvé tous les utilisateurs en cache, on retourne directement
    if (cachedUsers.size == userIds.size) {
      return Future.successful(cachedUsers)
    }

    // On cherche les IDs qui ne sont pas dans le cache
    val missingIds = userIds.filterNot(cachedUsers.contains)

    // On charge depuis la DB les utilisateurs manquants
    txManager.withTransaction { conn =>
      // On crée une requête avec des placeholders pour la clause IN
      val placeholders = missingIds.map(_ => "?").mkString(",")
      val sql = s"SELECT * FROM users WHERE user_id IN ($placeholders)"
      
      logger.debug(s"Executing batch user query for ${missingIds.size} users: $sql")
      
      withPreparedStatement(conn, sql) { ps =>
        // On définit chaque ID comme paramètre
        missingIds.zipWithIndex.foreach { case (id, index) =>
          ps.setLong(index + 1, id)
          logger.debug(s"Setting parameter ${index + 1} to user ID: $id")
        }
        
        withResultSet(ps) { rs =>
          // On charge tous les utilisateurs trouvés dans la DB
          var dbUsers = Map.empty[Long, User]
          while (rs.next()) {
            val user = mapResultSetToUser(rs)
            val userId = user.userId
            dbUsers += (userId -> user)
            
            // On met aussi en cache les utilisateurs trouvés
            putInCache("user", userId, Some(user), LongCacheTTL)
            logger.debug(s"Retrieved user from DB: $userId")
          }
          
          logger.debug(s"Found ${dbUsers.size} users in DB, ${cachedUsers.size} in cache")
          
          // On combine avec les résultats du cache
          cachedUsers ++ dbUsers
        }
      }
    }
  }

  /**
   * Batch get user profiles
   */
  def batchGetUserProfiles(userIds: List[Long]): Future[Map[Long, User]] = {
    if (userIds.isEmpty) {
      return Future.successful(Map.empty[Long, User])
    }
    
    // Vérifier le cache d'abord
    val cachedUsers = userIds.flatMap { userId =>
      getFromCache[Option[User]]("user", userId) match {
        case Some(Some(user)) => Some(userId -> user)
        case _ => None
      }
    }.toMap
    
    val uncachedIds = userIds.filterNot(cachedUsers.contains)
    
    if (uncachedIds.isEmpty) {
      Future.successful(cachedUsers)
    } else {
      txManager.withTransaction { conn =>
        val placeholders = uncachedIds.map(_ => "?").mkString(",")
        val sql = s"SELECT * FROM users WHERE user_id IN ($placeholders)"
        
        logger.debug(s"Executing batch user query for ${uncachedIds.size} users: $sql")
        
        withPreparedStatement(conn, sql) { ps =>
          uncachedIds.zipWithIndex.foreach { case (userId, index) =>
            ps.setLong(index + 1, userId)
            logger.debug(s"Setting parameter ${index + 1} to user ID: $userId")
          }
          
          withResultSet(ps) { rs =>
            var dbUsers = Map.empty[Long, User]
            while (rs.next()) {
              val user = mapResultSetToUser(rs)
              val userId = user.userId
              dbUsers += (userId -> user)
              
              // Mettre en cache les utilisateurs trouvés
              putInCache("user", userId, Some(user), LongCacheTTL)
              logger.debug(s"Retrieved user from DB: $userId")
            }
            
            logger.debug(s"Found ${dbUsers.size} users in DB, ${cachedUsers.size} in cache")
            
            // Combiner avec les résultats du cache
            cachedUsers ++ dbUsers
          }
        }
      }
    }
  }
  
  /**
   * Batch get users
   */
  def batchGetUsers(userIds: List[Long]): Future[Map[Long, User]] = {
    if (userIds.isEmpty) {
      return Future.successful(Map.empty)
    }

    // Check cache first
    val cachedUsers = userIds.flatMap { userId =>
      getFromCache[Option[User]]("user", userId) match {
        case Some(Some(user)) => Some(userId -> user)
        case _ => None
      }
    }.toMap

    val uncachedIds = userIds.filterNot(cachedUsers.contains)

    if (uncachedIds.isEmpty) {
      Future.successful(cachedUsers)
    } else {
      txManager.withTransaction { conn =>
        val placeholders = uncachedIds.map(_ => "?").mkString(",")
        val sql = s"SELECT * FROM users WHERE user_id IN ($placeholders)"
        
        logger.debug(s"Executing batch user query for ${uncachedIds.size} users: $sql")
        
        withPreparedStatement(conn, sql) { ps =>
          uncachedIds.zipWithIndex.foreach { case (userId, index) =>
            ps.setLong(index + 1, userId)
            logger.debug(s"Setting parameter ${index + 1} to user ID: $userId")
          }
          
          withResultSet(ps) { rs =>
            var dbUsers = Map.empty[Long, User]
            while (rs.next()) {
              val user = mapResultSetToUser(rs)
              val userId = user.userId
              dbUsers += (userId -> user)
              
              // Put in cache
              putInCache("user", userId, Some(user), LongCacheTTL)
              logger.debug(s"Retrieved user from DB: $userId")
            }
            
            logger.debug(s"Found ${dbUsers.size} users in DB, ${cachedUsers.size} in cache")
            
            // Return combined results
            cachedUsers ++ dbUsers
          }
        }
      }
    }
  }
  
  /**
   * Delete a user by ID
   */
  def deleteUser(userId: Long): Future[Boolean] = {
    txManager.withTransaction { conn =>
      // First delete related data
      deleteUserRelatedData(conn, userId)
      
      // Then delete the user
      val sql = "DELETE FROM users WHERE user_id = ?"
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, userId)
        val count = ps.executeUpdate()
        
        if (count > 0) {
          // Invalidate cache
          invalidateCache("user", userId)
          true
        } else {
          false
        }
      }
    }
  }

  /**
   * Supprime les données liées à un utilisateur quand l'utilisateur est supprimé
   */
  private def deleteUserRelatedData(conn: Connection, userId: Long): Unit = {
    // Supprimer les amitiés de l'utilisateur
    val friendsSql = "DELETE FROM user_friends WHERE user_id = ? OR friend_id = ?"
    withPreparedStatement(conn, friendsSql) { ps =>
      ps.setLong(1, userId)
      ps.setLong(2, userId)
      ps.executeUpdate()
    }
    
    // Supprimer les participations aux événements
    val participationsSql = "DELETE FROM event_participations WHERE user_id = ?"
    withPreparedStatement(conn, participationsSql) { ps =>
      ps.setLong(1, userId)
      ps.executeUpdate()
    }
    
    // Supprimer les événements créés par l'utilisateur
    val eventsSql = "DELETE FROM events WHERE created_by = ?"
    withPreparedStatement(conn, eventsSql) { ps =>
      ps.setLong(1, userId)
      ps.executeUpdate()
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
                   VALUES(?, ?, ?, ?::jsonb, ?, ?) RETURNING id"""
      
      withPreparedStatement(conn, sql) { ps =>
        ps.setLong(1, event.userId)
        ps.setString(2, event.eventType)
        event.itemId match {
          case Some(id) => ps.setLong(3, id)
          case None => ps.setNull(3, java.sql.Types.BIGINT)
        }
        // Utiliser ::jsonb pour forcer le cast
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
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = handleDatabaseOperation {
    txManager.withTransaction { conn =>
      val sql = "SELECT * FROM processed_events WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?"
      
      logger.debug(s"Executing query: $sql with userId=$userId, limit=$limit")
      
      withPreparedStatement(conn, sql) { ps =>
        if (ps == null) {
          throw new SQLException("PreparedStatement is null")
        }
        
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
    txManager.withTransaction { conn =>
      val sql = """INSERT INTO processed_events(user_id, event_type, item_id, properties, timestamp, processing_timestamp)
                   VALUES(?, ?, ?, ?::jsonb, ?, ?)"""
      
      withPreparedStatement(conn, sql) { ps =>
        events.foreach { event =>
          ps.setLong(1, event.userId)
          ps.setString(2, event.eventType)
          event.itemId match {
            case Some(id) => ps.setLong(3, id)
            case None => ps.setNull(3, java.sql.Types.BIGINT)
          }
          // Utiliser ::jsonb pour forcer le cast
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
      endTime = Option(rs.getLong("end_time")).filter(_ != 0),
      location = Option(rs.getString("location")),
      createdBy = rs.getLong("created_by"),
      status = rs.getString("status"),
      keywords = List.empty,
      keywordFrequencies = Map.empty,
      participants = Set.empty
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
  def initTables(): Future[Unit] = handleDatabaseOperation {
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

  /**
   * Perform a health check on the DataManager component
   * @return true if all connections and resources are available, false otherwise
   */
  def healthCheck(): Boolean = {
    try {
      // Vérifier l'accès à la base de données
      val dbConnectionOk = isDatabaseAccessible()
      
      // Vérifier l'accès au cache si applicable
      val cacheAccessOk = isCacheAccessible()
      
      // Autres vérifications spécifiques au DataManager
      // ...
      
      dbConnectionOk && cacheAccessOk
    } catch {
      case e: Exception =>
        logger.error(s"DataManager health check failed: ${e.getMessage}", e)
        false
    }
  }

  /**
   * Check if database is accessible
   * @return true if database is accessible, false otherwise
   */
  def isDatabaseAccessible(): Boolean = {
    try {
      withConnection { conn =>
        withPreparedStatement(conn, "SELECT 1") { ps =>
          val rs = ps.executeQuery()
          rs.next() // Si ça fonctionne, la base est accessible
        }
      }
      true
    } catch {
      case e: Exception =>
        logger.warn(s"Database not accessible: ${e.getMessage}")
        false
    }
  }

  /**
   * Check if cache is accessible
   * @return true if cache is accessible, false otherwise
   */
  def isCacheAccessible(): Boolean = {
    try {
      // Vérifier l'accès au cache si applicable
      true // Remplacer par l'implémentation réelle
    } catch {
      case e: Exception =>
        logger.warn(s"Cache not accessible: ${e.getMessage}")
        false
    }
  }

  /**
   * Ferme les ressources du DataManager
   * Alias pour shutdown() pour compatibilité avec les tests
   */
  def close(): Unit = shutdown()

  /**
   * Arrête proprement le DataManager et libère toutes les ressources
   */
  def shutdown(): Unit = {
    if (isShutdown.compareAndSet(false, true)) {
      logger.info("Shutting down DataManager...")
      
      // Fermeture du gestionnaire de transactions
      txManager.close()
      
      // Fermer les canaux RabbitMQ
      if (historicalDataChannel.isDefined) {
        try {
          historicalDataChannel.get.close()
        } catch {
          case e: Exception => logger.warn(s"Error closing historical data channel: ${e.getMessage}", e)
        }
      }
      
      if (recommenderSyncChannel.isDefined) {
        try {
          recommenderSyncChannel.get.close()
        } catch {
          case e: Exception => logger.warn(s"Error closing recommender sync channel: ${e.getMessage}", e)
        }
      }
      
      // Autres nettoyages...
      
      logger.info("DataManager shutdown complete")
    }
  }
}
