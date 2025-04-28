package repositories

import models._
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import org.slf4j.LoggerFactory

class Repository(implicit val ec: ExecutionContext) extends RepositoryInterface {
  
  private val logger = LoggerFactory.getLogger(getClass)

  // Implements RepositoryInterface methods
  
  // Event processing methods
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = executeWithErrorHandling({
    withConnection { connection =>
      val sql = """
        INSERT INTO processed_events 
          (user_id, event_type, item_id, properties, timestamp, processing_timestamp)
        VALUES (?, ?, ?, ?::jsonb, ?, ?) 
        RETURNING id
      """
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, event.userId)
        ps.setString(2, event.eventType)
        ps.setObject(3, event.itemId.orNull)
        ps.setString(4, event.properties.asJson.noSpaces)
        ps.setLong(5, event.timestamp)
        ps.setLong(6, event.processingTimestamp)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val id = rs.getLong("id")
            event.copy(id = Some(id))
          } else {
            event
          }
        }
      }
    }
  }, s"Error storing processed event: $event")
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = Future {
    withConnection { connection =>
      val events = scala.collection.mutable.ListBuffer[ProcessedEvent]()
      val sql = """
        SELECT id, user_id, event_type, item_id, properties, timestamp, processing_timestamp 
        FROM processed_events 
        WHERE user_id = ? 
        ORDER BY timestamp DESC 
        LIMIT ?
      """
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, userId)
        ps.setInt(2, limit)
        
        withResultSet(ps) { rs =>
          while (rs.next()) {
            val propertiesStr = rs.getString("properties")
            val properties = decode[Map[String, String]](propertiesStr).getOrElse(Map.empty)
            
            events += ProcessedEvent(
              id = Some(rs.getLong("id")),
              userId = rs.getLong("user_id"),
              eventType = rs.getString("event_type"),
              itemId = Option(rs.getObject("item_id")).map(_.asInstanceOf[Long]),
              properties = properties,
              timestamp = rs.getLong("timestamp"),
              processingTimestamp = rs.getLong("processing_timestamp")
            )
          }
        }
      }
      
      events.toList
    }
  }
  
  override def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]] = {
    // Implementation
    Future.successful(Seq.empty)
  }
  
  // Recommendation methods
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = Future {
    withConnection { connection =>
      val sql = """
        INSERT INTO recommendations 
          (user_id, recommended_items, score, timestamp)
        VALUES (?, ?::jsonb, ?, ?) 
        RETURNING id
      """
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, recommendation.userId)
        ps.setString(2, recommendation.recommendedItems.asJson.noSpaces)
        ps.setDouble(3, recommendation.score)
        ps.setLong(4, recommendation.timestamp)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            val id = rs.getLong("id")
            recommendation.copy(id = Some(id))
          } else {
            recommendation
          }
        }
      }
    }
  }
  
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    // Implementation
    Future.successful(None)
  }
  
  override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = {
    // Implementation  
    Future.successful(None)
  }
  
  // Database setup
  override def initTables(): Future[Unit] = executeWithErrorHandling({
    withConnection { connection =>
      val statement = connection.createStatement()
      
      // Table for processed events
      statement.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
          id SERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          event_type VARCHAR(255) NOT NULL,
          item_id BIGINT,
          properties JSONB NOT NULL,
          timestamp BIGINT NOT NULL,
          processing_timestamp BIGINT NOT NULL
        )
      """)
      
      // Table for recommendations
      statement.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
          id SERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          recommended_items JSONB NOT NULL,
          score DOUBLE PRECISION NOT NULL,
          timestamp BIGINT NOT NULL
        )
      """)
      
      () // Return Unit
    }
  }, "Error initializing tables")
  
  // Helper methods for DB operations
  protected def executeWithErrorHandling[T](operation: => T, errorMessage: String = "Repository operation failed"): Future[T] = {
    Future {
      try {
        operation
      } catch {
        case e: Exception =>
          logger.error(s"$errorMessage: ${e.getMessage}", e)
          throw e
      }
    }
  }
  
  protected def withConnection[T](operation: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = DatabaseConfig.getConnection
      operation(connection)
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close connection: ${e.getMessage}", e)
        }
      }
    }
  }
  
  protected def withPreparedStatement[T](connection: Connection, sql: String)
                                    (operation: PreparedStatement => T): T = {
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
  
  protected def withResultSet[T](ps: PreparedStatement)(operation: ResultSet => T): T = {
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
}