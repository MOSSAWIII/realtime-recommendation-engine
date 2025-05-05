package testutil

import com.rabbitmq.client.{Channel, Connection}
import config.RabbitMQManager
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{anyString => mockitoAnyString}
import org.powermock.api.mockito.PowerMockito
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.Suite
import scala.util.Try
import scala.collection.mutable

/**
 * Enhanced RabbitMQ test support that handles both mocked and real connections
 * for different test scenarios.
 */
trait RabbitMQTestManager extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  // Track test resources for proper cleanup
  private val mockChannels = mutable.Map[String, Channel]()
  private val realChannels = mutable.Map[String, Channel]()
  private var realConnection: Option[Connection] = None
  private var mockingEnabled: Boolean = true

  /**
   * Configure whether to use real or mock RabbitMQ connections
   */
  def useMockConnections(enabled: Boolean = true): Unit = {
    mockingEnabled = enabled
  }
  
  /**
   * Set up test environment
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (mockingEnabled) {
      setupMockRabbitMQ()
    } else {
      setupRealRabbitMQ()
    }
  }
  
  /**
   * Clean up test resources
   */
  override def afterAll(): Unit = {
    closeAllResources()
    super.afterAll()
  }
  
  /**
   * Reset between tests
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    if (mockingEnabled) {
      resetMocks()
    }
  }
  
  /**
   * Set up mocking for RabbitMQ
   */
  private def setupMockRabbitMQ(): Unit = {
    PowerMockito.mockStatic(classOf[RabbitMQManager.type])
    
    // Mock the createChannel/getChannel methods
    PowerMockito.when(RabbitMQManager.getChannel(mockitoAnyString())).thenAnswer { invocation =>
      val componentName = invocation.getArgument[String](0)
      mockChannels.getOrElseUpdate(componentName, createMockChannel(componentName))
    }
    
    PowerMockito.when(RabbitMQManager.createChannel(mockitoAnyString())).thenAnswer { invocation =>
      val componentName = invocation.getArgument[String](0)
      mockChannels.getOrElseUpdate(componentName, createMockChannel(componentName))
    }
    
    // Mock closeChannel
    PowerMockito.doAnswer { invocation =>
      val componentName = invocation.getArgument[String](0)
      mockChannels.remove(componentName)
      null
    }.when(RabbitMQManager, "closeChannel", mockitoAnyString())
    
    // Mock closeAllChannels
    PowerMockito.doAnswer { _ =>
      mockChannels.clear()
      null
    }.when(RabbitMQManager, "closeAllChannels")
  }
  
  /**
   * Set up real RabbitMQ connection for integration tests
   */
  private def setupRealRabbitMQ(): Unit = {
    import com.rabbitmq.client.ConnectionFactory
    val factory = new ConnectionFactory()
    factory.setHost("test-rabbitmq")
    factory.setPort(5672)
    factory.setUsername("guest")
    factory.setPassword("guest")
    
    // Use retry pattern for container startup timing issues
    var connected = false
    var attempts = 0
    val maxAttempts = 5
    
    while (!connected && attempts < maxAttempts) {
      try {
        realConnection = Some(factory.newConnection())
        connected = true
      } catch {
        case e: Exception =>
          attempts += 1
          println(s"RabbitMQ connection attempt $attempts/$maxAttempts failed: ${e.getMessage}")
          Thread.sleep(2000)
      }
    }
    
    if (!connected) {
      throw new RuntimeException("Failed to connect to RabbitMQ test container")
    }
  }
  
  /**
   * Create a real channel for integration tests
   */
  def createRealChannel(componentName: String): Channel = {
    realConnection.map { conn =>
      val channel = conn.createChannel()
      realChannels.put(componentName, channel)
      channel
    }.getOrElse(throw new RuntimeException("No real RabbitMQ connection available"))
  }
  
  /**
   * Create a mock channel for unit tests
   */
  def createMockChannel(componentName: String): Channel = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    when(mockChannel.isOpen).thenReturn(true)
    mockChannel
  }
  
  /**
   * Reset mocks between tests
   */
  private def resetMocks(): Unit = {
    mockChannels.foreach { case (_, channel) => 
      reset(channel)
      when(channel.isOpen).thenReturn(true)
    }
  }
  
  /**
   * Clean up all resources
   */
  private def closeAllResources(): Unit = {
    // Close real channels
    realChannels.foreach { case (_, channel) => 
      try {
        if (channel.isOpen) {
          channel.close()
        }
      } catch {
        case _: Exception => // Ignore errors during cleanup
      }
    }
    realChannels.clear()
    
    // Close real connection
    realConnection.foreach { conn =>
      try {
        if (conn.isOpen) {
          conn.close()
        }
      } catch {
        case _: Exception => // Ignore errors during cleanup
      }
    }
    realConnection = None
    
    // Clear mock tracking
    mockChannels.clear()
  }
  
  /**
   * Verify channel was closed
   */
  def verifyChannelClosed(componentName: String): Unit = {
    if (mockingEnabled) {
      PowerMockito.verifyStatic(RabbitMQManager.getClass)
      RabbitMQManager.closeChannel(componentName)
    }
  }
}

