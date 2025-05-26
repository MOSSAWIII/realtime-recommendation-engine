package functional

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
import common.base.FunctionalTestBase
import common.mocks.TestMockManager
import common.mocks.TestMockManager.MockDataManager

class CompletePipelineSpec extends FunctionalTestBase {
  private val logger = LoggerFactory.getLogger(getClass)

  // Test data
  val testUserId = 999L
  val testItemId = 888L
  
  // Promise to track recommendation publication
  private var recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
  private var consumerTag: String = _
  private var recommendationChannel: com.rabbitmq.client.Channel = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    TestMockManager.initConnection()
    
    // Setup recommendation listener
    recommendationChannel = TestMockManager.createChannel("recommendation-channel")
    recommendationChannel.queueDeclare(TestMockManager.Queues.RECOMMENDATIONS, true, false, false, null)
    
    // Create callback to capture recommendations
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, message: com.rabbitmq.client.Delivery): Unit = {
        val recommendationJson = new String(message.getBody, StandardCharsets.UTF_8)
        decode[Recommendation](recommendationJson) match {
          case Right(recommendation) =>
            if (!recommendationPromise.isCompleted) {
              recommendationPromise.success(recommendation)
            }
          case Left(error) =>
            logger.error(s"Failed to parse recommendation: $error")
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer was cancelled: $consumerTag")
      }
    }
    
    // Start consuming recommendations
    consumerTag = recommendationChannel.basicConsume(
      TestMockManager.Queues.RECOMMENDATIONS,
      true,
      deliverCallback,
      cancelCallback
    )
  }
  
  override def afterAll(): Unit = {
    if (recommendationChannel != null && recommendationChannel.isOpen) {
      if (consumerTag != null) {
        try {
          recommendationChannel.basicCancel(consumerTag)
        } catch {
          case _: Exception => // Ignore
        }
      }
      TestMockManager.closeChannel("recommendation-channel")
    }
    
    TestMockManager.cleanup()
    super.afterAll()
  }
  
  "Complete recommendation pipeline" should "process user events and generate recommendations" in {
    // Reset promise for this test
    recommendationPromise = Promise[Recommendation]()
    
    // Setup components
    val mockDataManager = new MockDataManager()
    val transformer = new Transformer(mockDataManager)
    val recommender = new Recommender(mockDataManager)
    
    // Start components
    transformer.start()
    recommender.start()
    
    try {
      // Create and publish a user event
      val userEvent = UserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(testItemId),
        timestamp = System.currentTimeMillis()
      )
      
      // Process the event
      val processedEvent = Await.result(transformer.processEvent(userEvent), 5.seconds)
      
      // Wait for recommendation
      val recommendation = Await.result(recommendationPromise.future, 10.seconds)
      
      // Verify recommendation
      recommendation.userId shouldBe testUserId
      recommendation.recommendedItems.nonEmpty shouldBe true
      
    } finally {
      // Clean up
      transformer.close()
      recommender.close()
    }
  }
}