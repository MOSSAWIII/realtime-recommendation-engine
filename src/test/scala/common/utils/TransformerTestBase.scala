package testutil

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{ExecutionContext, Promise}
import java.util.UUID
import java.util.concurrent.CountDownLatch

import application.core.Transformer
import common.utils.RabbitMQManager

abstract class TransformerTestBase extends AnyWordSpec with BeforeAndAfterEach with Matchers {
  
  // Components
  protected var mockDataManager: MockDataManager = _
  protected var transformer: TestTransformer = _
  
  // Configuration
  protected var testQueueName: String = _
  protected implicit val ec: ExecutionContext = ExecutionContext.global
  
  /**
   * Configure test queue name with unique identifier
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    testQueueName = s"test_queue_${UUID.randomUUID().toString}"
    setupQueue()
  }
  
  /**
   * Setup transformer with appropriate configuration
   * @param latchSize Number of events to wait for
   */
w  protected def setupTransformer(latchSize: Int = 0): CountDownLatch = {
    // Setup test queue for isolating the test
    testQueueName = s"test-queue-${UUID.randomUUID().toString}"
    setupQueue()
    
    // Create transformer with mock data manager
    val mockDataManager = new MockDataManager(if (latchSize > 0) new CountDownLatch(latchSize) else null)
    
    // Use TestTransformer to avoid real RabbitMQ connections
    transformer = new TestTransformer(
      mockDataManager, 
      testQueueName, 
      Promise[Boolean]()
    )
    
    transformer.start()
    
    if (latchSize > 0) new CountDownLatch(latchSize) else new CountDownLatch(0)
  }
  
  /**
   * Create the test queue
   */
  protected def setupQueue(): Unit = {
    try {
      val channel = RabbitMQManager.createChannel("test-setup")
      channel.queueDeclare(testQueueName, true, false, false, null)
      RabbitMQManager.closeChannel("test-setup")
    } catch {
      case e: Exception => 
        fail(s"Failed to setup test queue: ${e.getMessage}")
    }
  }
  
  /**
   * Clean up resources
   */
  override def afterEach(): Unit = {
    if (transformer != null) {
      transformer.close()
    }
    
    // Clean up queue
    try {
      val cleanupChannel = RabbitMQManager.createChannel("test-cleanup")
      cleanupChannel.queueDelete(testQueueName)
      RabbitMQManager.closeChannel("test-cleanup")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    
    super.afterEach()
  }
}

