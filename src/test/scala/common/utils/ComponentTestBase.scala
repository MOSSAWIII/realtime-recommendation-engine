package testutil

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import scala.concurrent.ExecutionContext
import common.utils.RabbitMQManager
import core.{DataManager, Transformer, Recommender}
import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory

trait ComponentTestBase extends BeforeAndAfterAll with Matchers {
  // Logger for all test classes
  protected val logger = LoggerFactory.getLogger(getClass)
  
  // Common test components
  protected var dataManager: MockDataManager = _
  protected var transformer: Transformer = _
  protected var recommender: Recommender = _
  
  // Implicit execution context for async operations
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  
  /**
   * Initialize components with the new dependency chain
   * DataManager → Transformer → Recommender
   */
  protected def initializeComponents(latch: CountDownLatch = null): Unit = {
    // Initialize the mock DataManager
    dataManager = new MockDataManager(latch)
    
    // Initialize Transformer with DataManager
    transformer = new Transformer(dataManager)
    
    // Initialize Recommender with DataManager
    recommender = new Recommender(dataManager)
    
    logger.info("Test components initialized")
  }
  
  /**
   * Start all components in correct order
   */
  protected def startComponents(): Unit = {
    // Start DataManager first
    dataManager.start()
    
    // Start Recommender before Transformer to ensure recommendations can be handled
    recommender.start()
    
    // Start Transformer last - it now consumes messages directly
    transformer.start()
    
    logger.info("Test components started")
  }
  
  /**
   * Close all components in reverse order of startup
   */
  protected def closeComponents(): Unit = {
    // Close in reverse order
    if (transformer != null) transformer.close()
    if (recommender != null) recommender.close()
    if (dataManager != null) dataManager.close()
    
    // Additional cleanup
    RabbitMQManager.closeAllChannels()
    
    logger.info("Test components closed")
  }
  
  /**
   * Standard cleanup in afterAll
   */
  override def afterAll(): Unit = {
    closeComponents()
    super.afterAll()
  }
}

