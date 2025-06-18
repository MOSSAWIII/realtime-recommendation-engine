package common.mocks

import infrastructure.config.DatabaseConfigProvider
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, anyString}
import org.slf4j.LoggerFactory

class TestDatabaseProvider extends DatabaseConfigProvider {
  private val logger = LoggerFactory.getLogger(getClass)
  private var mockConnection: Option[Connection] = None
  private var realConnection: Option[Connection] = None
  
  // Initialiser automatiquement une connexion mockée par défaut
  {
    setMockConnection(setupMockConnection())
  }
  
  // Pour les tests d'intégration, permettre une vraie connexion
  def setRealConnection(url: String, user: String, password: String): Unit = {
    realConnection = Some(DriverManager.getConnection(url, user, password))
  }
  
  def setMockConnection(conn: Connection): Unit = {
    mockConnection = Some(conn)
    logger.debug(s"Mock connection set: $conn")
  }
  
  override def getConnection(): Connection = {
    realConnection.getOrElse {
      mockConnection.getOrElse {
        logger.error("No mock connection set")
        throw new RuntimeException("No mock connection set")
      }
    }
  }
  
  override def close(): Unit = {
    logger.debug("Closing database provider")
    realConnection.foreach { conn =>
      try {
        conn.close()
      } catch {
        case e: Exception => logger.warn(s"Error closing real connection: ${e.getMessage}")
      }
    }
    realConnection = None
    
    mockConnection.foreach { conn => 
      try {
        conn.close()
      } catch {
        case e: Exception => logger.warn(s"Error closing mock connection: ${e.getMessage}")
      }
    }
    mockConnection = None
  }

  private def setupMockConnection(): Connection = {
    val mockConnection = mock(classOf[Connection])
    val mockPreparedStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockConnection.prepareStatement(any[String], any[Int])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    
    mockConnection
  }
}

object TestDatabaseProvider {
  
  def createMockConnection(): Connection = {
    val mockConnection = mock(classOf[Connection])
    val mockPreparedStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    // Configure the mock to return a valid PreparedStatement
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    
    // Configure the PreparedStatement to return a valid ResultSet
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    
    // Configure ResultSet behavior
    when(mockResultSet.next()).thenReturn(false) // Empty result set by default
    
    mockConnection
  }
}