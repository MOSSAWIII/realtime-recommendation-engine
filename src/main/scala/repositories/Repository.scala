package repositories

import models._
import scala.concurrent.{ExecutionContext, Future}
import java.sql.{Connection, PreparedStatement, ResultSet}
import config.DatabaseConfig
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._

class Repository(implicit ec: ExecutionContext) {
  
  // Initialisation des tables
  def initTables(): Future[Unit] = Future {
    val connection = DatabaseConfig.getConnection()
    try {
      val statement = connection.createStatement()
      
      // Table des événements traités
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
      
      // Table des recommandations
      statement.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
          id SERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          recommended_items JSONB NOT NULL,
          score DOUBLE PRECISION NOT NULL,
          timestamp BIGINT NOT NULL
        )
      """)
      
      println("Tables initialized successfully")
    } finally {
      connection.close()
    }
  }
  
  // ----- Méthodes pour les événements traités -----
  
  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = Future {
    val connection = DatabaseConfig.getConnection()
    try {
      val sql = """
        INSERT INTO processed_events 
          (user_id, event_type, item_id, properties, timestamp, processing_timestamp)
        VALUES (?, ?, ?, ?::jsonb, ?, ?) 
        RETURNING id
      """
      
      val ps = connection.prepareStatement(sql)
      ps.setLong(1, event.userId)
      ps.setString(2, event.eventType)
      ps.setObject(3, event.itemId.orNull)
      ps.setString(4, event.properties.asJson.noSpaces)
      ps.setLong(5, event.timestamp)
      ps.setLong(6, event.processingTimestamp)
      
      val rs = ps.executeQuery()
      if (rs.next()) {
        val id = rs.getLong("id")
        event.copy(id = Some(id))
      } else {
        event
      }
    } finally {
      connection.close()
    }
  }
  
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = Future {
    val connection = DatabaseConfig.getConnection()
    val events = scala.collection.mutable.ListBuffer[ProcessedEvent]()
    
    try {
      val sql = """
        SELECT id, user_id, event_type, item_id, properties, timestamp, processing_timestamp 
        FROM processed_events 
        WHERE user_id = ? 
        ORDER BY timestamp DESC 
        LIMIT ?
      """
      
      val ps = connection.prepareStatement(sql)
      ps.setLong(1, userId)
      ps.setInt(2, limit)
      val rs = ps.executeQuery()
      
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
      
      events.toList
    } finally {
      connection.close()
    }
  }
  
  def getRecentProcessedEvents(limit: Int = 1000): Future[List[ProcessedEvent]] = Future {
    val connection = DatabaseConfig.getConnection()
    val events = scala.collection.mutable.ListBuffer[ProcessedEvent]()
    
    try {
      val sql = """
        SELECT id, user_id, event_type, item_id, properties, timestamp, processing_timestamp 
        FROM processed_events 
        ORDER BY timestamp DESC 
        LIMIT ?
      """
      
      val ps = connection.prepareStatement(sql)
      ps.setInt(1, limit)
      val rs = ps.executeQuery()
      
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
      
      events.toList
    } finally {
      connection.close()
    }
  }
  
  // ----- Méthodes pour les recommandations -----
  
  def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = Future {
    val connection = DatabaseConfig.getConnection()
    try {
      val sql = """
        INSERT INTO recommendations 
          (user_id, recommended_items, score, timestamp)
        VALUES (?, ?::jsonb, ?, ?) 
        RETURNING id
      """
      
      val ps = connection.prepareStatement(sql)
      ps.setLong(1, recommendation.userId)
      ps.setString(2, recommendation.recommendedItems.asJson.noSpaces)
      ps.setDouble(3, recommendation.score)
      ps.setLong(4, recommendation.timestamp)
      
      val rs = ps.executeQuery()
      if (rs.next()) {
        val id = rs.getLong("id")
        recommendation.copy(id = Some(id))
      } else {
        recommendation
      }
    } finally {
      connection.close()
    }
  }
  
  def getLatestRecommendationForUser(userId: Long): Future[Option[Recommendation]] = Future {
    val connection = DatabaseConfig.getConnection()
    try {
      val sql = """
        SELECT id, user_id, recommended_items, score, timestamp
        FROM recommendations
        WHERE user_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
      """
      
      val ps = connection.prepareStatement(sql)
      ps.setLong(1, userId)
      val rs = ps.executeQuery()
      
      if (rs.next()) {
        val itemsStr = rs.getString("recommended_items")
        val items = decode[List[RecommendedItem]](itemsStr).getOrElse(List.empty)
        
        Some(Recommendation(
          id = Some(rs.getLong("id")),
          userId = rs.getLong("user_id"),
          recommendedItems = items,
          score = rs.getDouble("score"),
          timestamp = rs.getLong("timestamp")
        ))
      } else {
        None
      }
    } finally {
      connection.close()
    }
  }
}