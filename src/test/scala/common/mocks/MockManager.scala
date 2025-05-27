package common.mocks

import java.sql.{Connection, PreparedStatement, ResultSet}
import com.rabbitmq.client.{Channel, Connection => RabbitConnection}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.anyString
import org.slf4j.LoggerFactory

/**
 * Gestionnaire centralisé de mocks pour les tests
 */
object MockManager {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Crée et configure une connexion de base de données mockée
   */
  def mockDatabaseConnection(): Connection = {
    val conn = mock(classOf[Connection])
    val stmt = mock(classOf[PreparedStatement])
    val rs = mock(classOf[ResultSet])
    
    // Configuration de base
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    when(stmt.executeQuery()).thenReturn(rs)
    
    logger.debug(s"Created mock database connection: $conn")
    conn
  }
  
  /**
   * Crée et configure un canal RabbitMQ mocké
   */
  def mockRabbitMQChannel(): Channel = {
    val channel = mock(classOf[Channel])
    
    // Configuration de base
    when(channel.isOpen).thenReturn(true)
    
    logger.debug(s"Created mock RabbitMQ channel: $channel")
    channel
  }
}

