package integration.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{Transformer, Recommender, StreamingConsumer}
import models.{UserEvent, RecommendationRequest, Recommendation, ProcessedEvent}
import repositories.Repository
import config.RabbitMQManager
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import org.slf4j.LoggerFactory

class PipelineIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Components for testing
  var repository: TestRepository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  var consumer: StreamingConsumer = _
  
  // Test data
  val testUserId = 999L
  val testItemId = 888L
  
  // Promise to track recommendation publication
  var recommendationPromise: Promise[Recommendation] = Promise[Recommendation]()
  var consumerTag: String = _
  
  override def beforeAll(): Unit = {
    repository = new TestRepository()
    Await.result(repository.initTables(), 5.seconds)
    
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    consumer = new StreamingConsumer(transformer)
    
    // Setup recommendation listener
    setupRecommendationListener()
    
    // Start recommender to handle requests
    recommender.start()
  }
  
  override def afterAll(): Unit = {
    // Cleanup consumer registration if it exists
    if (consumerTag != null) {
      try {
        val channel = RabbitMQManager.getChannel("test-rec-listener")
        if (channel != null && channel.isOpen) {
          channel.basicCancel(consumerTag)
          logger.debug(s"Cancelled consumer with tag: $consumerTag")
        }
      } catch {
        case e: Exception => logger.warn(s"Error cancelling consumer: ${e.getMessage}")
      }
    }
    
    // Close all components
    recommender.close()
    transformer.close()
    consumer.close()
    RabbitMQManager.closeAllChannels()
  }
  
  "The recommendation pipeline" should "process user events and generate recommendations" in {
    // Reset the promise
    recommendationPromise = Promise[Recommendation]()
    
    // Create a test event
    val event = UserEvent(
      userId = testUserId, 
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("test" -> "true"))
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
        Await.result(recommendationPromise.future, 15.seconds) // Increased timeout
      } catch {
        case e: java.util.concurrent.TimeoutException => 
          logger.error("Timed out waiting for recommendation")
          fail("Timed out waiting for recommendation")
      }
    
    // Verify recommendation
    recommendation.userId shouldBe testUserId
    recommendation.recommendedItems.nonEmpty shouldBe true
  }
  
  private def setupRecommendationListener(): Unit = {
    val channel = RabbitMQManager.createChannel("test-rec-listener")
    
    // Make sure the queue exists and clear any previous messages
    channel.queueDeclare(RabbitMQManager.Queues.RECOMMENDATIONS, true, false, false, null)
    channel.queuePurge(RabbitMQManager.Queues.RECOMMENDATIONS)
    
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
        
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer cancelled: $consumerTag")
      }
    }
    
    // Store the consumer tag for cleanup
    consumerTag = channel.basicConsume(
      RabbitMQManager.Queues.RECOMMENDATIONS,
      false, // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info(s"Recommendation listener set up with consumer tag: $consumerTag")
  }
  
  // Test repository to help with testing
  class TestRepository extends Repository {
    override def storeProcessedEvent(event: models.ProcessedEvent) = 
      Future.successful(event.copy(id = Some(1L)))
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] =
      Future.successful(List(
        models.ProcessedEvent(
          id = Some(1L),
          userId = userId,
          eventType = "view",
          itemId = Some(testItemId),
          properties = Map("test" -> "true"),
          timestamp = System.currentTimeMillis()
        )
      ))
      
    override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = 
      Future.successful(recommendation.copy(id = Some(1L)))
      
    override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = 
      Future.successful(None)
      
    override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = 
      Future.successful(None)
      
    override def getUserProfile(userId: Long): Future[Option[models.User]] = 
      Future.successful(Some(models.User(userId = userId, name = Some("Test User"))))
      
    override def getUserFriends(userId: Long): Future[List[models.UserFriend]] = 
      Future.successful(List())
      
    override def getAllActiveEvents(): Future[List[models.Event]] = 
      Future.successful(List(
        models.Event(
          id = Some(testItemId),
          name = "Test Event",
          startTime = System.currentTimeMillis(),
          createdBy = 1L,
          status = "active"
        )
      ))
      
    override def initTables(): Future[Unit] = Future.successful(())
  }
}

