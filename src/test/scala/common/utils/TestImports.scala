package common.utils

import java.sql.Connection
import com.rabbitmq.client.Channel
import scala.concurrent.ExecutionContext

/**
 * TestImports provides helper methods for tests, delegating to MockProvider
 * for actual implementation to avoid duplication
 */
object TestImports {
  /**
   * Mock database configuration for testing
   */
  def mockDatabaseConfig(): Connection = {
    MockProvider.mockDatabaseConfig()
  }
  
  /**
   * Mock RabbitMQ configuration for testing
   */
  def mockRabbitMQConfig(): Channel = {
    val (channel, _) = MockProvider.mockRabbitMQConfig()
    channel
  }
  
  /**
   * Set up a complete test environment with all mocks configured
   */
  def setupTestEnvironment(): (Channel, Connection) = {
    val connection = mockDatabaseConfig() 
    val channel = mockRabbitMQConfig()
    (channel, connection)
  }
  
  /**
   * Get test execution context
   */
  implicit val testExecutionContext: ExecutionContext = MockProvider.testExecutionContext
}

