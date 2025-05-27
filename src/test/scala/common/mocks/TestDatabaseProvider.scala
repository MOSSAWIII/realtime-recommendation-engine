package common.mocks

import infrastructure.config.DatabaseConfigProvider
import org.mockito.Mockito._
import scala.concurrent.ExecutionContext
import java.sql.Connection
import org.slf4j.LoggerFactory

class TestDatabaseProvider extends DatabaseConfigProvider {
  private val logger = LoggerFactory.getLogger(getClass)
  private var mockConnection: Option[Connection] = None
  
  // Initialiser automatiquement une connexion mockée par défaut
  {
    setMockConnection(mock(classOf[Connection]))
  }
  
  def setMockConnection(conn: Connection): Unit = {
    mockConnection = Some(conn)
    logger.debug(s"Mock connection set: $conn")
  }
  
  override def getConnection(): Connection = {
    mockConnection.getOrElse {
      logger.error("No mock connection set")
      throw new RuntimeException("No mock connection set")
    }
  }
  
  // Implémentation de la méthode close() manquante
  override def close(): Unit = {
    logger.debug("Closing mock database provider")
    mockConnection.foreach { conn => 
      try {
        conn.close()
      } catch {
        case e: Exception => logger.warn(s"Error closing mock connection: ${e.getMessage}")
      }
    }
    mockConnection = None
  }
}