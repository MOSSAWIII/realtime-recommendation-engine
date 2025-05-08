package integration.streaming

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import testutil.IntegrationSpec
import config.{TestConfig, RabbitMQManager}
import core.{StreamingConsumer, Transformer}
import repositories.Repository
import models.UserEvent
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import com.rabbitmq.client.{AMQP, DeliverCallback, CancelCallback}
import java.util.UUID
import java.nio.charset.StandardCharsets
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class StreamingTransformerIntegrationSpec extends IntegrationSpec with BeforeAndAfterAll {

  // Test configuration
  val testQueueName = s"test_events_${UUID.randomUUID().toString}"
  var repository: Repository = _
  var transformer: Transformer = _
  var consumer: StreamingConsumer = _

  override def beforeAll(): Unit = {
    // Log test mode status to help with debugging
    println(s"Running in test mode: ${TestConfig.isTestMode}")

    try {
      repository = new MockRepository()
      transformer = new Transformer(repository)(ec)

      val channel = RabbitMQManager.createChannel("test")
      channel.queueDeclare(testQueueName, false, false, true, null)
      
      consumer = new TestStreamingConsumer(transformer, testQueueName)
      consumer.start()
      
      Thread.sleep(1000)
    } catch {
      case e: Exception =>
        println(s"Test setup failed: ${e.getMessage}")
        e.printStackTrace()
        fail("Test setup failed", e)
    }
  }

  override def afterAll(): Unit = {
    if (consumer != null) {
      consumer.close()
    }
    
    val channel = RabbitMQManager.createChannel("cleanup")
    channel.queueDelete(testQueueName)
    RabbitMQManager.closeAllChannels()
  }

  "StreamingConsumer and Transformer" should "process events and store them in the database" in {
    val testEvent = UserEvent(userId = 99, eventType = "test_view", itemId = Some(999))
    val eventJson = testEvent.asJson.noSpaces

    val channel = RabbitMQManager.createChannel("publisher")
    channel.basicPublish("", testQueueName, new AMQP.BasicProperties.Builder().build(), eventJson.getBytes)
    
    println(s"Test event published to queue: $testQueueName")
    Thread.sleep(5000)

    val storedEvents = Await.result(repository.getProcessedEventsForUser(99), 5.seconds)
    println(s"Stored events count: ${storedEvents.size}")
    storedEvents.size should be >= 1
    storedEvents.exists(e => e.userId == 99 && e.eventType == "test_view" && e.itemId.contains(999)) shouldBe true
  }

  // Custom consumer that listens to our test queue
  class TestStreamingConsumer(transformer: Transformer, queueName: String) extends StreamingConsumer(transformer) {
    override def start(): Unit = {
      println(s"Starting test consumer for queue: $queueName")
      val channel = RabbitMQManager.createChannel("test-consumer")

      // FIXED: Define DeliverCallback directly instead of using non-existent createDeliverCallback
      val deliverCallback = new DeliverCallback {
        override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
          val message = new String(delivery.getBody, StandardCharsets.UTF_8)
          println(s"Test consumer received message: $message")
          
          decode[UserEvent](message) match {
            case Right(event) =>
              println(s"Processing event for user ${event.userId}")
              processEvent(event) 
              println(s"Successfully processed event for user ${event.userId}")
            case Left(error) =>
              println(s"Failed to decode message: ${error.getMessage}")
          }
          
          channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        }
      }

      val cancelCallback = new CancelCallback {
        override def handle(consumerTag: String): Unit = {
          println(s"Consumer cancelled: $consumerTag")
        }
      }
      
      channel.basicConsume(queueName, false, deliverCallback, cancelCallback)
      println(s"Test consumer started and listening to queue: $queueName")
    }
  }

  // MockRepository implementation
  class MockRepository extends Repository()(ec) {
    import scala.collection.mutable.ArrayBuffer

    val storedEvents = ArrayBuffer[models.ProcessedEvent]()

    override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
      println(s"Storing event in mock repository: $event")
      storedEvents += event
      Future.successful(event.copy(id = Some(1L)))
    }

    override def initTables(): Future[Unit] = Future.successful(())
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[models.ProcessedEvent]] = {
      println(s"Getting events for user $userId, current count: ${storedEvents.size}")
      Future.successful(storedEvents.filter(_.userId == userId).toList.take(limit))
    }
  }
}