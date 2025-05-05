package testutil

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._  // Added this import for anyString()
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}
import config.DatabaseConfig
import scala.concurrent.ExecutionContext

object TestImports {
  // Create a mock connection for testing repository operations
  def getMockConnection(): Connection = {
    val mockConnection = mock(classOf[Connection])
    val mockStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    mockConnection
  }
  
  /**
   * Sets up mocks for the DatabaseConfig object
   * Modified to return the mockConnection
   */
  def mockDatabaseConfig(): Connection = {
    // Mock the DatabaseConfig object
    PowerMockito.mockStatic(classOf[DatabaseConfig.type])
    
    // Create a mock connection that can be returned by DatabaseConfig.getConnection()
    val mockConnection = PowerMockito.mock(classOf[Connection])
    val mockStatement = PowerMockito.mock(classOf[PreparedStatement])
    val mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    // Set up the chain of mocks
    PowerMockito.when(mockConnection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(mockStatement)
    PowerMockito.when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    // Configure DatabaseConfig.getConnection() to return our mock connection (without parentheses)
    PowerMockito.when(DatabaseConfig.getConnection).thenReturn(mockConnection)
    
    // Return the mock connection
    mockConnection
  }
  
  // Add missing mockRabbitMQConfig method
  def mockRabbitMQConfig(): com.rabbitmq.client.Channel = {
    val mockChannel = PowerMockito.mock(classOf[com.rabbitmq.client.Channel])
    PowerMockito.mockStatic(classOf[config.RabbitMQConfig.type])
    when(config.RabbitMQConfig.createChannel()).thenReturn(mockChannel)
    mockChannel
  }
  
  // Provide an execution context for testing
  implicit val testExecutionContext: ExecutionContext = 
    ExecutionContext.global
}

