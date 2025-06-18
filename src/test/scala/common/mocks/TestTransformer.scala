package common.mocks

import application.core.{DataManager, Transformer}
import domain.models._
import utils.metrics.{HealthCheckRegistry, HealthCheckResult, HealthStatus}
import com.rabbitmq.client.Channel
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.{CountDownLatch, TimeUnit}  // Ajouter TimeUnit ici

class TestTransformer(
  dataManager: DataManager,
  queueName: String,
  publishChannel: Channel,
  processedEventPromise: Promise[ProcessedEvent] = Promise[ProcessedEvent](),
  recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
)(implicit ec: ExecutionContext) extends Transformer(dataManager)(ec) with Matchers {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  private val latch = new CountDownLatch(1)
  
  val mockDataManager = new MockDataManager(
    new TestConfigurationService(),
    new TestDatabaseProvider(),
    null
  )(ec)

  private val componentName = "test-transformer"
  private val component = componentName
  private val check = "processing"  
  private val status = HealthStatus.Healthy
  private var message = "Test transformer operational"
  private val details = Map.empty[String, String]
  
  override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    val processed = ProcessedEvent(
      id = None,
      userId = event.userId,
      eventType = event.eventType,
      itemId = event.itemId,
      properties = event.properties.getOrElse(Map.empty),
      timestamp = event.timestamp,
      processingTimestamp = System.currentTimeMillis()
    )
    
    processedEventPromise.success(processed)
    latch.countDown()
    Future.successful(processed)
  }
  
  def registerHealthChecks(): Unit = {
    val channelsHealthy = publishChannel != null && publishChannel.isOpen
    
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck(componentName, "processing") {
        HealthCheckResult(
          component = componentName,
          check = "processing",
          status = if (channelsHealthy) HealthStatus.Healthy else HealthStatus.Degraded,
          message = if (channelsHealthy) "All channels operational" else "Some channels not available",
          details = Map(
            "publishChannel" -> channelsHealthy.toString
          )
        )
      }
    )
  }

  /**
   * Initialize the transformer - useful for tests
   */
  override def initialize(): Unit = {
    try {
      logger.info("Initializing transformer")
      // Si les canaux ne sont pas initialisés, les initialiser
      if (publishChannel == null || !publishChannel.isOpen) {
        // Logique d'initialisation des canaux si nécessaire
        logger.info("Transformer channels initialized")
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize transformer", e)
        throw e
    }
  }

  private def initializeChannels(): Unit = {
    try {
      logger.info("Test transformer channels initialized")
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize test transformer channels", e)
        throw e
    }
  }

  def waitForProcessing(timeoutSeconds: Int = 5): Boolean = {
    latch.await(timeoutSeconds, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
  }
}

object TestTransformer extends Matchers {
  def test(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    val mockDataManager = new MockDataManager(
      new TestConfigurationService(),
      new TestDatabaseProvider(),
      null
    )(ec)
    val processPromise = Promise[ProcessedEvent]()
    val latch = new CountDownLatch(1)
    val testQueueName = "test_transformer_queue"
    
    val transformer = new TestTransformer(mockDataManager, testQueueName, null, processPromise)(global)
    transformer.start()
    
    latch.await(5, TimeUnit.SECONDS) should be (true)
    transformer.stop()
  }
}

