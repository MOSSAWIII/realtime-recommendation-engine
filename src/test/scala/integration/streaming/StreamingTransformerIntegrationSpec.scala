package integration.streaming

import com.rabbitmq.client.{Channel, DeliverCallback}
import config.RabbitMQManager
import core.{StreamingConsumer, Transformer}
import integration.IntegrationTestBase
import models.{UserEvent, ProcessedEvent}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import repositories.Repository
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.CountDownLatch
import java.nio.charset.StandardCharsets
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._

class StreamingTransformerIntegrationSpec extends IntegrationTestBase with Eventually with IntegrationPatience {
  
  // Components under test
  var transformer: Transformer = _
  var consumer: StreamingConsumer = _
  
  // Test channels
  var producerChannel: Channel = _
  var resultChannel: Channel = _
  
  // Test data
  val testUserId = 1234L
  val testItemId = 5678L
  val testEvents = ArrayBuffer[ProcessedEvent]()
  val messagesReceived = new CountDownLatch(1)
  
  // Mock repository - the implicit EC is provided by IntegrationTestBase
  val mockRepository = new MockRepository()

  override def setupTestResources(): Unit = {
    // Set up channels for testing
    producerChannel = RabbitMQManager.createChannel("test-producer")
    resultChannel = RabbitMQManager.createChannel("test-result-consumer")
    
    // Declare required queues
    producerChannel.queueDeclare(RabbitMQManager.Queues.USER_EVENTS, true, false, false, null)
    resultChannel.queueDeclare("processed_events", true, false, false, null)
    
    // Initialize components
    transformer = new Transformer(mockRepository)
    consumer = new StreamingConsumer(transformer)
    
    // Set up result consumer
    val callback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[ProcessedEvent](message) match {
          case Right(event) =>
            testEvents.append(event)
            messagesReceived.countDown()
          case Left(error) =>
            fail(s"Failed to decode result message: ${error.getMessage}")
        }
        resultChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    resultChannel.basicConsume("processed_events", false, callback, _ => ())
    
    // Start consumer
    consumer.start()
  }
  
  override def cleanupTestResources(): Unit = {
    if (consumer != null) {
      consumer.close()
    }
    
    RabbitMQManager.closeChannel("test-producer")
    RabbitMQManager.closeChannel("test-result-consumer")
  }
  
  "The streaming pipeline" should {
    "process user events and produce transformed events" in {
      // Create test event
      val userEvent = UserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(testItemId),
        timestamp = System.currentTimeMillis(),
        properties = Some(Map("source" -> "test"))
      )
      
      // Send event to the USER_EVENTS queue
      producerChannel.basicPublish(
        "", // Default exchange
        RabbitMQManager.Queues.USER_EVENTS,
        null, // No properties
        userEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
      )
      
      // Wait for processing to complete
      messagesReceived.await()
      
      // Verify results
      testEvents.size should be > 0
      
      val processedEvent = testEvents.find(_.userId == testUserId)
      processedEvent shouldBe defined
      processedEvent.foreach { event =>
        event.userId shouldBe testUserId
        event.itemId shouldBe Some(testItemId)
        event.eventType shouldBe "view"
      }
    }
  }
}

// Fix the MockRepository by adding an override modifier
class MockRepository(implicit override val ec: ExecutionContext) extends repositories.Repository {
  var storedEvents = scala.collection.mutable.ArrayBuffer[models.ProcessedEvent]()
  
  override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
    storedEvents += event
    Future.successful(event.copy(id = Some(1L)))
  }
  
  override def initTables(): Future[Unit] = Future.successful(())

  // Implement any other required methods from Repository with simple implementations
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = 
    Future.successful(storedEvents.filter(_.userId == userId).toList.take(limit))
    
  override def storeRecommendation(recommendation: models.Recommendation): Future[models.Recommendation] =
    Future.successful(recommendation.copy(id = Some(1L)))
}