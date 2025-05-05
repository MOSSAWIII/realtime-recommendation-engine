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
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.DeliverCallback

class PipelineIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  // Components for testing
  var repository: TestRepository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  var consumer: StreamingConsumer = _
  
  // Test data
  val testUserId = 999L
  val testItemId = 888L
  
  // Promise to track recommendation publication
  var recommendationPromise: Promise[Recommendation] = _
  
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
    
    // Process the event through the transformer
    val processedEvent = Await.result(transformer.processEvent(event), 5.seconds)
    processedEvent.userId shouldBe testUserId
    processedEvent.itemId shouldBe Some(testItemId)
    
    // Wait for recommendation to be published
    val recommendation = Await.result(recommendationPromise.future, 10.seconds)
    
    // Verify recommendation
    recommendation.userId shouldBe testUserId
    recommendation.recommendedItems.nonEmpty shouldBe true
  }
  
  private def setupRecommendationListener(): Unit = {
    val channel = RabbitMQManager.createChannel("test-rec-listener")
    channel.queueDeclare(RabbitMQManager.Queues.RECOMMENDATIONS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        import io.circe.parser._
        decode[Recommendation](message) match {
          case Right(recommendation) if recommendation.userId == testUserId =>
            if (!recommendationPromise.isCompleted) {
              recommendationPromise.success(recommendation)
            }
          case _ => // Ignore other recommendations
        }
        
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(
      RabbitMQManager.Queues.RECOMMENDATIONS,
      false,
      deliverCallback,
      consumerTag => ()
    )
  }
  
  // Test repository to help with testing
  class TestRepository extends Repository()(global) {
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
  }
}

