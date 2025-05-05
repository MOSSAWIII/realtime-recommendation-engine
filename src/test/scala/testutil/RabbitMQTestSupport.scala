package testutil

import com.rabbitmq.client.{Channel, Connection}
import config.RabbitMQManager
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.Suite
import scala.util.Try

/**
 * Trait to provide RabbitMQ testing support
 * Can be mixed into test classes to provide mock RabbitMQ functionality
 */
trait RabbitMQTestSupport extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  private var mockChannels: Map[String, Channel] = Map.empty

  // Set up mocks before tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Optional: set up test config if needed
    val testConfig = ConfigFactory.parseString("""
      rabbitmq {
        host = "localhost"
        port = 5672
        username = "guest"
        password = "guest"
      }
    """)
    Try(RabbitMQManager.setConfig(testConfig))
  }
  
  // Clean up after all tests
  override def afterAll(): Unit = {
    Try(RabbitMQManager.closeConnection())
    super.afterAll()
  }
  
  // Reset mocks between tests
  override def beforeEach(): Unit = {
    super.beforeEach()
    mockChannels = Map.empty
  }
  
  // Create a mock channel for a specific component
  def createMockChannel(componentName: String): Channel = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    mockChannels += (componentName -> mockChannel)
    when(mockChannel.isOpen).thenReturn(true)
    mockChannel
  }
  
  // Mock RabbitMQManager to return mock channels
  def mockRabbitMQManager(): Unit = {
    PowerMockito.mockStatic(classOf[RabbitMQManager.type])
    
    // Mock the createChannel method
    PowerMockito.when(RabbitMQManager.createChannel(anyString())).thenAnswer { invocation =>
      val componentName = invocation.getArgument[String](0)
      mockChannels.getOrElse(componentName, createMockChannel(componentName))
    }
    
    // Mock the getChannel method (same behavior as createChannel)
    PowerMockito.when(RabbitMQManager.getChannel(anyString())).thenAnswer { invocation =>
      val componentName = invocation.getArgument[String](0)
      mockChannels.getOrElse(componentName, createMockChannel(componentName))
    }
    
    // Mock closeChannel to do nothing
    PowerMockito.doNothing().when(RabbitMQManager, "closeChannel", anyString())
    
    // Mock closeAllChannels to do nothing
    PowerMockito.doNothing().when(RabbitMQManager, "closeAllChannels")
    
    // Mock closeConnection to do nothing
    PowerMockito.doNothing().when(RabbitMQManager, "closeConnection")
  }
  
  // Helper method to verify that a channel was created with a specific name
  def verifyChannelCreated(componentName: String): Unit = {
    PowerMockito.verifyStatic(RabbitMQManager.getClass)
    RabbitMQManager.createChannel(componentName)
  }
  
  // Helper method to verify that a channel was closed with a specific name
  def verifyChannelClosed(componentName: String): Unit = {
    PowerMockito.verifyStatic(RabbitMQManager.getClass)
    RabbitMQManager.closeChannel(componentName)
  }
}

