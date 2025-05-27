package integration.messaging

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{AMQP, DeliverCallback, CancelCallback}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.generic.auto._

import application.core.{Transformer, Recommender}
import domain.models._
import common.base.RabbitMQTestBase
import common.utils.TestResourceManager
import common.mocks.MockDataManager
import common.fixtures.TestDataFactory

class PipelineIntegrationSpec extends RabbitMQTestBase {
  private val logger = LoggerFactory.getLogger(getClass)

  // Test data avec ID uniques
  val testUserId = TestResourceManager.uniqueId()
  val testItemId = TestResourceManager.uniqueId()
  
  // Promise to track recommendation publication
  private var recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
  private var consumerTag: String = _
  private var recommendationChannel: com.rabbitmq.client.Channel = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Use testExchange from RabbitMQTestBase instead of a hardcoded value
    logger.info(s"Setting up test with exchange: $testExchange and queue: $testQueueName")
    
    // Create recommendation channel
    recommendationChannel = createTestChannel("recommendation-channel")
    
    // Create an isolated queue for this test
    val queueName = createIsolatedQueue(recommendationChannel, "recommendation")
    
    // Setup recommendation listener with unique routing key for this test instance
    val routingKey = s"recommendation.result.${testId}"
    
    // Bind queue to exchange with unique routing key
    recommendationChannel.queueBind(queueName, testExchange, routingKey)
    
    // Set up listener
    setupRecommendationListener(queueName)
  }
  
  private def setupRecommendationListener(queueName: String): Unit = {
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received message: $message")
        
        decode[Recommendation](message) match {
          case Right(recommendation) =>
            logger.info(s"Decoded recommendation for user: ${recommendation.userId}")
            
            if (recommendation.userId == testUserId) {
              logger.info(s"Received matching recommendation for test user $testUserId")
              if (!recommendationPromise.isCompleted) {
                recommendationPromise.success(recommendation)
                logger.info("Promise completed successfully")
              } else {
                logger.warn("Promise was already completed")
              }
            } else {
              logger.debug(s"Received recommendation for different user: ${recommendation.userId}")
            }
          case Left(error) => 
            logger.error(s"Failed to decode recommendation: $error")
        }
        
        recommendationChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer cancelled: $consumerTag")
      }
    }
    
    // Store the consumer tag for cleanup
    consumerTag = recommendationChannel.basicConsume(
      queueName,
      false, // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info(s"Recommendation listener set up with consumer tag: $consumerTag")
  }
  
  override def afterAll(): Unit = {
    // Cleanup is handled by RabbitMQTestBase
    super.afterAll()
  }
  
  "The recommendation pipeline" should "process user events and generate recommendations" in {
    // Reset the promise for this test
    recommendationPromise = Promise[Recommendation]()
    
    // Create isolated components for this test
    val mockDataManager = new MockDataManager()
    val transformer = new Transformer(mockDataManager)
    registerForCleanup(transformer)
    val recommenderCloseable = new AutoCloseable {
      override def close(): Unit = {
        // Logique de nettoyage pour recommender
      }
    }
    registerForCleanup(recommenderCloseable)
    
    // Test with unique IDs
    val event = TestDataFactory.createUserEvent(
      userId = testUserId, 
      itemId = Some(testItemId)
    )
    
    logger.info(s"Processing test event for user $testUserId with item $testItemId")
    
    // Process the event through the transformer
    val processedEvent = Await.result(transformer.processEvent(event), 5.seconds)
    processedEvent.userId shouldBe testUserId
    processedEvent.itemId shouldBe Some(testItemId)
    
    logger.info("Waiting for recommendation to be published...")
    
    // Wait for recommendation to be published with a longer timeout
    val recommendation = 
      try {
        Await.result(recommendationPromise.future, 15.seconds)
      } catch {
        case e: java.util.concurrent.TimeoutException => 
          logger.error("Timed out waiting for recommendation")
          fail("Timed out waiting for recommendation")
      }
    
    // Verify recommendation
    recommendation.userId shouldBe testUserId
    recommendation.recommendedItems.nonEmpty shouldBe true
  }
}

