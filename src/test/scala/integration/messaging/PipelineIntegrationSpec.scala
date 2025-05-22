package integration.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import models.{UserEvent, RecommendationRequest, Recommendation, ProcessedEvent}
import common.utils.RabbitMQManager
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{DeliverCallback, CancelCallback, Channel}
import common.utils.{ComponentTestBase, MockDataManager}

class PipelineIntegrationSpec extends AnyFlatSpec with ComponentTestBase {
  
  // Test data
  val testUserId = 999L
  val testItemId = 888L
  
  // Promise to track recommendation publication
  private var recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
  private var consumerTag: String = _
  private var recommendationChannel: Channel = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Initialize components with new dependency chain
    initializeComponents()
    
    // Setup test data
    setupTestData()
    
    // Setup recommendation listener first to catch all messages
    setupRecommendationListener()
    
    // Start components
    startComponents()
  }
  
  private def setupTestData(): Unit = {
    // Sample pre-processed event
    val testEvent = ProcessedEvent(
      id = Some(1L),
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Map("test" -> "true"),
      timestamp = System.currentTimeMillis() - 5000
    )
    
    // Configure mock data
    dataManager.setupMockData(
      processedEvents = List(testEvent)
    )
    
    logger.info(s"Test data configured for user $testUserId and item $testItemId")
  }
  
  private def setupRecommendationListener(): Unit = {
    recommendationChannel = RabbitMQManager.createChannel("test-rec-listener")
    
    // Make sure the queue exists and clear any previous messages
    recommendationChannel.queueDeclare(RabbitMQManager.Queues.RECOMMENDATIONS, true, false, false, null)
    recommendationChannel.queuePurge(RabbitMQManager.Queues.RECOMMENDATIONS)
    
    logger.info(s"Setting up recommendation listener for user: $testUserId")
    
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
      RabbitMQManager.Queues.RECOMMENDATIONS,
      false, // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info(s"Recommendation listener set up with consumer tag: $consumerTag")
  }
  
  override def afterAll(): Unit = {
    // Cleanup consumer registration if it exists
    if (consumerTag != null && recommendationChannel != null && recommendationChannel.isOpen) {
      try {
        recommendationChannel.basicCancel(consumerTag)
        logger.debug(s"Cancelled consumer with tag: $consumerTag")
      } catch {
        case e: Exception => logger.warn(s"Error cancelling consumer: ${e.getMessage}")
      }
    }
    
    super.afterAll() // This calls closeComponents()
  }
  
  "The recommendation pipeline" should "process user events and generate recommendations" in {
    // Reset the promise
    recommendationPromise = Promise[Recommendation]()
    
    // Create a test event
    val event = UserEvent(
      userId = testUserId, 
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("test" -> "true")),
      timestamp = System.currentTimeMillis()
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

  "Le pipeline de recommandation" should "traiter les événements et générer des recommandations" in {
    // Setup avec MockDataManager qui capture les événements
    
    // Publier un événement de test
    publishTestEvent(event)
    
    // Attendre que l'événement soit traité (avec latch)
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    
    // 1. Vérifier que l'événement a été correctement stocké
    val storedEvents = mockDataManager.storedEvents
    storedEvents should have size 1
    storedEvents.head.userId shouldBe event.userId
    
    // 2. Vérifier que la recommandation a été générée et contient les informations attendues
    val recommendation = waitForRecommendation(15.seconds)
    recommendation.userId shouldBe event.userId
    
    // 3. Vérifier que les recommandations utilisent correctement le contexte
    recommendation.recommendedItems.exists(item => 
      item.reason.contains("based on your recent view") || 
      item.reason.contains("friends are interested")
    ) shouldBe true
    
    // 4. Vérifier l'ordre et les scores des recommandations
    val sortedItems = recommendation.recommendedItems.sortBy(-_.score)
    sortedItems should equal (recommendation.recommendedItems)
  }
}

