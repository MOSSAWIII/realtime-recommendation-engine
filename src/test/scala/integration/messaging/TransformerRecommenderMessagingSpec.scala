package integration.messaging

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{AMQP, DeliverCallback, CancelCallback, Channel, Connection, ConnectionFactory}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.generic.auto._

import application.core.{DataManager, Transformer, Recommender}
import domain.models._
import common.base.RabbitMQTestBase
import common.utils.TestResourceManager
import common.mocks.{MockDataManager, TestConfigurationService, TestDatabaseProvider, MockFactory}
import common.fixtures.TestDataFactory
import common.base.MessagingTestBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterEach
import infrastructure.messaging.rabbitmq.RabbitMQManager
import utils.metrics.{HealthCheckRegistry, HealthCheckResult, HealthStatus}

class TransformerRecommenderMessagingSpec extends AnyFlatSpec with Matchers 
  with BeforeAndAfterAll with BeforeAndAfterEach {
  
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Ajouter componentName dans la classe principale
  private val componentName = "transformer-recommender-messaging-spec"
  
  // Test components
  private var dataManager: MockDataManager = _
  private var transformer: Transformer = _
  private var recommender: Recommender = _
  
  private var publishChannel: Channel = _
  private var consumeChannel: Channel = _
  private var processedEventPromise: Promise[ProcessedEvent] = _
  private var recommendationPromise: Promise[Recommendation] = _
  private var connection: Connection = _
  private var streamingConsumerChannel: Channel = _

  private def verifyQueuesExist(): Unit = {
    val testVHost = sys.env.getOrElse("TEST_VHOST", "test_env")
    val expectedQueues = List(
      sys.env.getOrElse("TEST_PROCESSED_EVENTS_QUEUE", "processed_events_queue"),
      sys.env.getOrElse("TEST_RECOMMENDATIONS_QUEUE", "recommendations_queue"),
      sys.env.getOrElse("TEST_HISTORICAL_DATA_QUEUE", "historical_data_queue")
    )
    
    expectedQueues.foreach { queueName =>
      try {
        publishChannel.queueDeclarePassive(queueName)
        logger.info(s"Queue '$queueName' exists in vhost '$testVHost'")
      } catch {
        case e: Exception =>
          logger.error(s"Queue '$queueName' not found in vhost '$testVHost': ${e.getMessage}")
          throw new RuntimeException(s"Required queue '$queueName' not found in test environment")
      }
    }
  }
  
  private def waitForConsumersToBeReady(): Unit = {
    logger.info("Waiting for RabbitMQ consumers to be ready...")
    Thread.sleep(5000)
    
    val testVHost = sys.env.getOrElse("TEST_VHOST", "test_env")
    val queues = List("processed_events_queue", "recommendations_queue")
    
    queues.foreach { queueName =>
      try {
        val queueInfo = publishChannel.queueDeclarePassive(queueName)
        logger.info(s"Queue '$queueName' has ${queueInfo.getConsumerCount} consumers")
      } catch {
        case e: Exception =>
          logger.warn(s"Could not check consumer count for queue '$queueName': ${e.getMessage}")
      }
    }
  }
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    logger.info("=== Test Environment Variables ===")
    logger.info(s"TEST_RUN_ID: ${sys.env.get("TEST_RUN_ID")}")
    logger.info(s"TEST_VHOST: ${sys.env.get("TEST_VHOST")}")
    logger.info(s"TEST_EXCHANGE: ${sys.env.get("TEST_EXCHANGE")}")
    logger.info(s"TEST_PROCESSED_EVENTS_QUEUE: ${sys.env.get("TEST_PROCESSED_EVENTS_QUEUE")}")
    logger.info(s"TEST_RECOMMENDATIONS_QUEUE: ${sys.env.get("TEST_RECOMMENDATIONS_QUEUE")}")
    logger.info(s"RABBITMQ_HOST: ${sys.env.get("RABBITMQ_HOST")}")
    logger.info(s"RABBITMQ_PORT: ${sys.env.get("RABBITMQ_PORT")}")
    logger.info("================================")
    
    val testVHost = sys.env.getOrElse("TEST_VHOST", "test_env")
    setupRabbitMQConnection(testVHost)
    verifyQueuesExist()
    setupMessageConsumers()
    
    RabbitMQManager.setTestVirtualHost(testVHost)
    
    Thread.sleep(2000)
    
    setupTestComponents()
    initializeTestData()
    
    // Initialize transformer with proper channels
    transformer.initialize()
    
    logger.info("Test components initialized successfully")
  }
  
  private def initializeTestData(): Unit = {
    logger.info("Initializing test data...")
    
    if (dataManager != null) {
      dataManager.initializeMockData()
    } else {
      logger.warn("DataManager is null, skipping mock data initialization")
    }
    
    logger.info("Test data initialized")
  }
  
  private def setupRabbitMQConnection(vhost: String): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost(sys.env.getOrElse("RABBITMQ_HOST", "test-rabbitmq"))
    factory.setPort(sys.env.getOrElse("RABBITMQ_PORT", "5672").toInt)
    factory.setUsername("guest")
    factory.setPassword("guest")
    factory.setVirtualHost(vhost)
    
    connection = factory.newConnection()
    publishChannel = connection.createChannel()
    consumeChannel = connection.createChannel()
    streamingConsumerChannel = connection.createChannel()
    
    Thread.sleep(2000)
    
    logger.info(s"Connected to RabbitMQ with vhost: $vhost")
  }
  
  private def setupTestComponents(): Unit = {
    dataManager = MockFactory.createDataManager()
    
    val testVHost = sys.env.getOrElse("TEST_VHOST", "test_env")
    
    transformer = new Transformer(dataManager)
    recommender = new Recommender(dataManager)
    
    recommender.start()
    
    waitForConsumersToBeReady()
    
    logger.info("Test components initialized successfully")
    logger.info(s"Recommender started with test vhost: $testVHost")
  }
  
  private def setupMessageConsumers(): Unit = {
    setupProcessedEventConsumer()
    setupRecommendationConsumer()
  }
  
  private def setupProcessedEventConsumer(): Unit = {
    val queueName = sys.env.getOrElse("TEST_PROCESSED_EVENTS_QUEUE", "processed_events_queue")
    
    logger.info(s"Setting up consumer for queue: $queueName")
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received processed event: $message")
        logger.info(s"TEST CONSUMER: Received processed event message")
        
        decode[ProcessedEvent](message) match {
          case Right(processedEvent) =>
            logger.info(s"TEST CONSUMER: Successfully decoded processed event for user ${processedEvent.userId}")
            if (processedEventPromise != null && !processedEventPromise.isCompleted) {
              processedEventPromise.success(processedEvent)
              logger.info("TEST CONSUMER: Promise completed")
            }
          case Left(error) =>
            logger.error(s"Failed to decode processed event: $error")
        }
      }
    }
    
    val consumerTag = consumeChannel.basicConsume(queueName, true, deliverCallback, (_: String) => {})
    logger.info(s"Consumer registered with tag: $consumerTag")
  }
  
  private def setupRecommendationConsumer(): Unit = {
    val queueName = sys.env.getOrElse("TEST_RECOMMENDATIONS_QUEUE", "recommendations_queue")
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received recommendation: $message")
        
        decode[Recommendation](message) match {
          case Right(recommendation) =>
            if (recommendationPromise != null && !recommendationPromise.isCompleted) {
              recommendationPromise.success(recommendation)
            }
          case Left(error) =>
            logger.error(s"Failed to decode recommendation: $error")
        }
      }
    }
    
    consumeChannel.basicConsume(queueName, true, deliverCallback, (_: String) => {})
  }

  private def setupRecommendationConsumerWithFiltering(expectedUserId: Long): Unit = {
    val queueName = sys.env.getOrElse("TEST_RECOMMENDATIONS_QUEUE", "recommendations_queue")
    
    val routingKey = s"recommendation.result.${expectedUserId}"
    val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
    
    consumeChannel.queueBind(queueName, exchange, routingKey)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received recommendation: $message")
        
        decode[Recommendation](message) match {
          case Right(recommendation) =>
            if (recommendation.userId == expectedUserId) {
              logger.info(s"TEST CONSUMER: Received recommendation for expected user ${expectedUserId}")
              if (recommendationPromise != null && !recommendationPromise.isCompleted) {
                recommendationPromise.success(recommendation)
                logger.info("TEST CONSUMER: Promise completed for correct user")
              }
            } else {
              logger.debug(s"TEST CONSUMER: Ignoring recommendation for user ${recommendation.userId} (expected: ${expectedUserId})")
            }
          case Left(error) =>
            logger.error(s"Failed to decode recommendation: $error")
        }
      }
    }
    
    consumeChannel.basicConsume(queueName, true, deliverCallback, (_: String) => {})
    logger.info(s"Set up filtered recommendation consumer for user ${expectedUserId} with routing key: ${routingKey}")
  }
  
  private def registerHealthChecks(): Unit = {
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck(componentName, "processing") {
        val channelsHealthy = (publishChannel != null && publishChannel.isOpen) &&
                             (consumeChannel != null && consumeChannel.isOpen) &&
                             (streamingConsumerChannel != null && streamingConsumerChannel.isOpen)
        
        HealthCheckResult(
          component = componentName,
          check = "processing",
          status = if (channelsHealthy) HealthStatus.Healthy else HealthStatus.Degraded,
          message = if (channelsHealthy) "All channels operational" else "Some channels not available",
          details = Map(
            "publishChannel" -> (publishChannel != null && publishChannel.isOpen).toString,
            "consumeChannel" -> (consumeChannel != null && consumeChannel.isOpen).toString,
            "streamingChannel" -> (streamingConsumerChannel != null && streamingConsumerChannel.isOpen).toString
          )
        )
      }
    )
  }
  
  override def afterAll(): Unit = {
    try {
      if (recommender != null) {
        logger.info("Stopping recommender...")
        recommender.close()
      }
      
      if (transformer != null) {
        logger.info("Stopping transformer...")
        transformer.close()
      }
      
      if (publishChannel != null && publishChannel.isOpen) publishChannel.close()
      if (consumeChannel != null && consumeChannel.isOpen) consumeChannel.close()
      if (streamingConsumerChannel != null && streamingConsumerChannel.isOpen) streamingConsumerChannel.close()
      if (connection != null && connection.isOpen) connection.close()
    } catch {
      case e: Exception => logger.warn(s"Error during cleanup: ${e.getMessage}")
    }
    super.afterAll()
  }
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    
    processedEventPromise = Promise[ProcessedEvent]()
    recommendationPromise = Promise[Recommendation]()
    
    val testItemId = System.currentTimeMillis() // ✅ Unique par test
    
    if (dataManager != null) {
      // ✅ NOUVEAU : Nettoyer les données existantes avant d'initialiser
      dataManager.clearMockData()
      dataManager.initializeMockData(testItemId)
    }
  }

  /**
   * Initialize the transformer - useful for tests
   */
  def initialize(): Unit = {
    if (publishChannel == null || !publishChannel.isOpen) {
      initializeChannels()
    }
  }

  /**
   * Initialize channels for testing
   */
  private def initializeChannels(): Unit = {
    try {
      logger.info("Test transformer channels initialized")
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize test transformer channels", e)
        throw e
    }
  }

  "Transformer and Recommender messaging integration" should "process user events end-to-end with event-driven smart triggering" in {
    require(transformer != null, "Transformer must be initialized")
    require(recommender != null, "Recommender must be initialized")
    
    val testUserId = 12345L
    val testItemId = 67890L
    
    setupRecommendationConsumerWithFiltering(testUserId)
    
    processedEventPromise = Promise[ProcessedEvent]()
    recommendationPromise = Promise[Recommendation]()
    
    val userEvent = UserEvent(
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("category" -> "movie", "rating" -> "5")),
      timestamp = System.currentTimeMillis()
    )
    
    logger.info(s"Processing user event for user $testUserId")
    
    val processedEventFuture = transformer.processEvent(userEvent)
    val processedEvent = Await.result(processedEventFuture, 5.seconds)
    
    processedEvent.userId shouldBe testUserId
    processedEvent.eventType shouldBe "view"
    processedEvent.itemId shouldBe Some(testItemId)
    
    logger.info("User event processed successfully")
    
    val receivedProcessedEvent = Await.result(processedEventPromise.future, 10.seconds)
    receivedProcessedEvent.userId shouldBe testUserId
    
    logger.info("Processed event received via RabbitMQ")
    
    val receivedRecommendation = Await.result(recommendationPromise.future, 15.seconds)
    
    receivedRecommendation.userId shouldBe testUserId
    receivedRecommendation.recommendedItems should not be empty
    
    logger.info(s"Recommendation received with ${receivedRecommendation.recommendedItems.size} items")
    
    val recommendedItemIds = receivedRecommendation.recommendedItems.map(_.itemId)
    recommendedItemIds should contain atLeastOneElementOf dataManager.getMockEvents().flatMap(_.id)
    logger.info("End-to-end event-driven messaging test completed successfully")
  }
  
  it should "handle multiple concurrent user events" in {
    processedEventPromise = Promise[ProcessedEvent]()
    
    val userIds = List(1001L, 1002L, 1003L)
    val events = userIds.map { userId =>
      UserEvent(
        userId = userId,
        eventType = "click",
        itemId = Some(userId * 10),
        timestamp = System.currentTimeMillis()
      )
    }
    
    val futures = events.map(transformer.processEvent)
    val processedEvents = Await.result(Future.sequence(futures), 10.seconds)
    
    processedEvents should have size 3
    processedEvents.map(_.userId) should contain theSameElementsAs userIds
    
    logger.info("Concurrent event processing test completed")
  }
  
  "complete messaging flow" should "process events through transformer and recommender with smart triggering" in {
    val testUserId = 12345L
    val testItemId = 67890L
    
    setupRecommendationConsumerWithFiltering(testUserId)
    
    processedEventPromise = Promise[ProcessedEvent]()
    recommendationPromise = Promise[Recommendation]()
    
    val userEvent = UserEvent(
      userId = testUserId,
      eventType = "click",
      itemId = Some(testItemId),
      properties = Some(Map("category" -> "movie", "rating" -> "5")),
      timestamp = System.currentTimeMillis()
    )
    
    logger.info(s"Processing user event for user $testUserId")
    
    val processedEventFuture = transformer.processEvent(userEvent)
    val processedEvent = Await.result(processedEventFuture, 5.seconds)
    
    processedEvent.userId shouldBe testUserId
    processedEvent.eventType shouldBe "click"
    processedEvent.itemId shouldBe Some(testItemId)
    
    logger.info("User event processed successfully")
    
    val receivedProcessedEvent = Await.result(processedEventPromise.future, 10.seconds)
    receivedProcessedEvent.userId shouldBe testUserId
    
    logger.info("Processed event received via RabbitMQ")
    
    val receivedRecommendation = Await.result(recommendationPromise.future, 15.seconds)
    
    receivedRecommendation.userId shouldBe testUserId
    receivedRecommendation.recommendedItems should not be empty
    
    logger.info(s"Recommendation received with ${receivedRecommendation.recommendedItems.size} items")
    
    val recommendedItemIds = receivedRecommendation.recommendedItems.map(_.itemId)
    recommendedItemIds should contain atLeastOneElementOf dataManager.getMockEvents().flatMap(_.id)
    logger.info("Complete messaging flow test completed successfully")
  }
}

class TestTransformerForSpec(
  dataManager: DataManager,
  queueName: String,
  publishChannel: Channel,
  processedEventPromise: Promise[ProcessedEvent] = Promise[ProcessedEvent](),
  recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
)(implicit ec: ExecutionContext) extends Transformer(dataManager)(ec) {
  
  private val componentName = "test-transformer-spec"
  private val logger = LoggerFactory.getLogger(getClass)

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
   * Initialize channels for testing
   */
  private def initializeChannels(): Unit = {
    try {
      logger.info("Test transformer channels initialized")
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize test transformer channels", e)
        throw e
    }
  }

  /**
   * Initialize the transformer - useful for tests
   */
  override def initialize(): Unit = {
    if (publishChannel == null || !publishChannel.isOpen) {
      initializeChannels()
    }
  }
}