package repositories

import models._
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * Repository générique basé sur JDBC pour les opérations à haute performance.
 * 
 * Ce repository utilise des connexions JDBC directes pour:
 * - Les opérations sur les événements traités (haute fréquence)
 * - Les recommandations (lecture/écriture intensive)
 * 
 * Cette approche a été choisie pour maximiser les performances en:
 * - Évitant l'overhead d'un ORM pour les opérations critiques
 * - Permettant un contrôle fin des requêtes SQL et de la sérialisation
 */
class Repository(implicit val ec: ExecutionContext) extends RepositoryInterface {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Méthode utilitaire qui gère automatiquement:
   * - L'obtention d'une connexion depuis le pool
   * - La fermeture de la connexion après utilisation
   * - La gestion propre des exceptions
   */
  private def withConnection[T](operation: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = DatabaseConfig.getConnection()
      operation(connection)
    } catch {
      case e: Exception =>
        logger.error(s"Database operation failed: ${e.getMessage}", e)
        throw e
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => 
            logger.warn(s"Failed to close connection: ${e.getMessage}", e)
        }
      }
    }
  }
  
  /**
   * Utilitaire pour préparer une requête et gérer sa fermeture
   */
  private def withPreparedStatement[T](connection: Connection, sql: String)
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
          case e: Exception => 
            logger.warn(s"Failed to close prepared statement: ${e.getMessage}", e)
        }
      }
    }
  }
  
  /**
   * Utilitaire pour exécuter une requête et extraire les résultats
   */
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
          case e: Exception => 
            logger.warn(s"Failed to close result set: ${e.getMessage}", e)
        }
      }
    }
  }

  private def executeWithErrorHandling[T](block: => T, errorMsg: String = ""): Future[T] = {
    try {
      Future.successful(block)
    } catch {
      case e: Exception =>
        logger.error(s"${errorMsg}: ${e.getMessage}", e)
        Future.failed(e)
    }
  }
  
  // Initialisation des tables
  def initTables()(implicit ec: ExecutionContext): Future[Unit] = executeWithErrorHandling({
    withConnection { connection =>
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
      
      () // Retourner Unit
    }
  }, "Erreur lors de l'initialisation des tables")
  
  // ----- Méthodes pour les événements traités -----
  
  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = executeWithErrorHandling({
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
  }, s"Erreur lors du stockage d'un événement traité: $event")
  
  // Explicitly identify this as the ProcessedEvent repository implementation
  // to distinguish it from the domain-specific repositories
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = Future {
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
  
  def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]] = {
    executeWithErrorHandling({
      withConnection { connection =>
        val sql = """
          SELECT id, user_id, event_type, item_id, properties, timestamp, processing_timestamp
          FROM processed_events
          ORDER BY processing_timestamp DESC
          LIMIT ?
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setInt(1, limit)
          
          withResultSet(ps) { rs =>
            val events = new scala.collection.mutable.ArrayBuffer[ProcessedEvent]()
            
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
            
            events.toSeq
          }
        }
      }
    }, s"Erreur lors de la récupération des événements traités récents")
  }
  
  // ----- Méthodes pour les recommandations -----
  
  // Same for recommendations
  def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = Future {
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
  
  def getLatestRecommendationForUser(userId: Long): Future[Option[Recommendation]] = Future {
    withConnection { connection =>
      val sql = """
        SELECT id, user_id, recommended_items, score, timestamp
        FROM recommendations
        WHERE user_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
      """
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, userId)
        
        withResultSet(ps) { rs =>
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
        }
      }
    }
  }
  
  def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = 
    executeWithErrorHandling({
      withConnection { connection =>
        val sql = """
          SELECT id, user_id, recommended_items, score, timestamp 
          FROM recommendations 
          WHERE user_id = ? 
          ORDER BY timestamp DESC 
          LIMIT 1
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, userId)
          withResultSet(ps) { rs =>
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
          }
        }
      }
    }, s"Erreur lors de la récupération des recommandations pour l'utilisateur $userId")
  
  def getRecentRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    executeWithErrorHandling({
      withConnection { connection =>
        val now = System.currentTimeMillis()
        val earliestTimestamp = now - 3600000 // Example time window of 1 hour
        
        val sql = """
          SELECT id, user_id, recommended_items, score, timestamp 
          FROM recommendations 
          WHERE user_id = ? AND timestamp > ?
          ORDER BY timestamp DESC 
          LIMIT 1
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, userId)
          ps.setLong(2, earliestTimestamp)
          
          withResultSet(ps) { rs =>
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
          }
        }
      }
    }, s"Erreur lors de la récupération des recommandations récentes pour l'utilisateur $userId")
  }

  def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = {
    executeWithErrorHandling({
      withConnection { connection =>
        val currentTime = System.currentTimeMillis()
        val cutoffTime = currentTime - timeWindow
        
        val sql = """
          SELECT id, user_id, recommended_items, score, timestamp 
          FROM recommendations 
          WHERE user_id = ? AND timestamp > ? 
          ORDER BY timestamp DESC 
          LIMIT 1
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, userId)
          ps.setLong(2, cutoffTime)
          
          withResultSet(ps) { rs =>
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
          }
        }
      }
    }, s"Erreur lors de la récupération des recommandations récentes pour l'utilisateur $userId")
  }
}