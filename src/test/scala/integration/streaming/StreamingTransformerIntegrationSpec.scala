package integration.streaming

import com.rabbitmq.client.{Channel, DeliverCallback}
import config.RabbitMQManager
import core.{StreamingConsumer, Transformer}
import models.{UserEvent, ProcessedEvent}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.charset.StandardCharsets
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import scala.concurrent.ExecutionContext.Implicits.global

class StreamingTransformerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
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
  
  // Mock repository - using global ExecutionContext implicitly
  val mockRepository = new MockRepository()

  override def beforeAll(): Unit = {
    println(s"Connecting to RabbitMQ at ${sys.env.getOrElse("RABBITMQ_HOST", "test-rabbitmq")}:${sys.env.getOrElse("RABBITMQ_PORT", "5672")}")
    
    // Set up channels for testing - using the externally provided RabbitMQ instance
    val connectionAttempts = 5
    var connected = false
    var attempts = 0
    
    while (!connected && attempts < connectionAttempts) {
      try {
        producerChannel = RabbitMQManager.createChannel("test-producer")
        resultChannel = RabbitMQManager.createChannel("test-result-consumer")
        connected = true
      } catch {
        case e: Exception =>
          attempts += 1
          println(s"Attempt $attempts/$connectionAttempts to connect to RabbitMQ: ${e.getMessage}")
          Thread.sleep(5000) // Wait 5 seconds before retrying
      }
    }
    
    if (!connected) {
      fail("Could not connect to RabbitMQ after multiple attempts")
    }
    
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
  
  override def afterAll(): Unit = {
    if (consumer != null) {
      consumer.close()
    }
    
    if (producerChannel != null && producerChannel.isOpen) {
      RabbitMQManager.closeChannel("test-producer")
    }
    
    if (resultChannel != null && resultChannel.isOpen) {
      RabbitMQManager.closeChannel("test-result-consumer")
    }
  }
  
  "The streaming pipeline" should "process user events and produce transformed events" in {
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
    
    // Wait for processing to complete with timeout
    val received = messagesReceived.await(60, TimeUnit.SECONDS)
    
    // Verify results
    received shouldBe true
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

// Implement a MockRepository - WITHOUT the implicit parameter
class MockRepository extends repositories.Repository {
  // The implicit ExecutionContext comes from the import at the top of the file
  
  var storedEvents = scala.collection.mutable.ArrayBuffer[models.ProcessedEvent]()
  
  override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
    storedEvents += event
    Future.successful(event.copy(id = Some(1L)))
  }
  
  override def initTables(): Future[Unit] = Future.successful(())
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[models.ProcessedEvent]] = 
    Future.successful(storedEvents.filter(_.userId == userId).toList.take(limit))
    
  override def storeRecommendation(recommendation: models.Recommendation): Future[models.Recommendation] =
    Future.successful(recommendation.copy(id = Some(1L)))
    
  // Any other required methods can be stubbed as needed
}