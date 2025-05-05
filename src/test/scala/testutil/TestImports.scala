package testutil

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._  // Added this import for anyString()
import org.mockito.ArgumentMatchers.{anyString => mockitoAnyString} // Added this import for mockitoAnyString
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}
import config.DatabaseConfig
import scala.concurrent.ExecutionContext
import com.rabbitmq.client.{Channel, Connection => RabbitMQConnection}
import config.RabbitMQManager

/**
 * Common test imports and utilities
 */
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
    PowerMockito.when(mockConnection.prepareStatement(mockitoAnyString())).thenReturn(mockStatement)
    PowerMockito.when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    // Configure DatabaseConfig.getConnection() to return our mock connection (without parentheses)
    PowerMockito.when(DatabaseConfig.getConnection).thenReturn(mockConnection)
    
    // Return the mock connection
    mockConnection
  }
  
  /**
   * Mock RabbitMQConfig and return a mock Channel
   * This is for backward compatibility with existing tests
   */
  def mockRabbitMQConfig(): Channel = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    
    // If RabbitMQManager is being mocked, we need to set up the static mocks
    try {
      PowerMockito.mockStatic(classOf[RabbitMQManager.type])
      PowerMockito.when(RabbitMQManager.getChannel(mockitoAnyString())).thenReturn(mockChannel)
      PowerMockito.when(RabbitMQManager.createChannel(mockitoAnyString())).thenReturn(mockChannel)
    } catch {
      case _: Exception => // Ignore if already mocked
    }
    
    mockChannel
  }
  
  /**
   * Helper method to create a complete set of mocks for tests
   */
  def setupTestEnvironment(): (Channel, RabbitMQConnection) = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    val mockConnection = PowerMockito.mock(classOf[RabbitMQConnection])
    
    PowerMockito.mockStatic(classOf[RabbitMQManager.type])
    PowerMockito.when(RabbitMQManager.getChannel(mockitoAnyString())).thenReturn(mockChannel)
    PowerMockito.when(RabbitMQManager.createChannel(mockitoAnyString())).thenReturn(mockChannel)
    PowerMockito.when(RabbitMQManager.getConnection).thenReturn(mockConnection)
    
    (mockChannel, mockConnection)
  }
  
  // Provide an execution context for testing
  implicit val testExecutionContext: ExecutionContext = 
    ExecutionContext.global
}

