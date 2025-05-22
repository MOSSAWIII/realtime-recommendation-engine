package common.mocks

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}
import com.rabbitmq.client.{Channel, Connection => RabbitMQConnection}
import common.utils.{DatabaseConfig, RabbitMQManager}
import scala.concurrent.ExecutionContext

object MockProvider {
  /**
   * Creates a mock database connection for testing
   */
  def mockDatabaseConfig(): Connection = {
    val mockConnection = PowerMockito.mock(classOf[Connection])
    val mockStatement = PowerMockito.mock(classOf[PreparedStatement])
    val mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    // Configure DatabaseConfig to return our mock connection
    try {
      PowerMockito.mockStatic(classOf[DatabaseConfig.type])
      PowerMockito.when(DatabaseConfig.getConnection).thenReturn(mockConnection)
    } catch {
      case _: Exception => // Already mocked, ignore
    }
    
    mockConnection
  }
  
  /**
   * Creates a mock RabbitMQ channel and connection for testing
   */
  def mockRabbitMQConfig(): (Channel, RabbitMQConnection) = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    val mockConnection = PowerMockito.mock(classOf[RabbitMQConnection])
    
    // Mock RabbitMQManager static methods
    try {
      PowerMockito.mockStatic(classOf[RabbitMQManager.type])
      PowerMockito.when(RabbitMQManager.getChannel(any[String]())).thenReturn(mockChannel)
      PowerMockito.when(RabbitMQManager.createChannel(any[String]())).thenReturn(mockChannel)
      PowerMockito.when(RabbitMQManager.getConnection).thenReturn(mockConnection)
    } catch {
      case _: Exception => // Already mocked, ignore
    }
    
    (mockChannel, mockConnection)
  }
  
  /**
   * Provides an execution context for testing
   */
  def testExecutionContext: ExecutionContext = ExecutionContext.global
}

