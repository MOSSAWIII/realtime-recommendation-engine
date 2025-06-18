package integration.messaging

import common.base.IsolatedTestBase
import common.fixtures.TestDataFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import application.core.{Transformer, DataManager}
import domain.models.{UserEvent, ProcessedEvent}
import infrastructure.messaging.rabbitmq.RabbitMQManager
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import com.rabbitmq.client.{AMQP, DeliverCallback, CancelCallback, Channel}
import java.nio.charset.StandardCharsets
import scala.concurrent.{Await, Promise, ExecutionContext}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import common.mocks.{TestConfigurationService, TestDatabaseProvider}

class StreamingTransformerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with IsolatedTestBase {
  override implicit val ec: ExecutionContext = ExecutionContext.global
  private val logger = LoggerFactory.getLogger(getClass)
  
  var transformer: Transformer = _
  var dataManager: DataManager = _
  var messagePromise: Promise[ProcessedEvent] = _
  var consumerTag: String = _
  var consumerChannel: Channel = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Créer les services de configuration pour les tests
    val configService = new TestConfigurationService()
    val dbProvider = new TestDatabaseProvider()
    
    // Corriger l'instanciation du DataManager
    dataManager = new DataManager(configService, dbProvider)
    
    // Corriger l'enregistrement pour le nettoyage
    registerForCleanup(new AutoCloseable {
      override def close(): Unit = dataManager.shutdown()
    })
    
    // Set up the transformer with real DataManager
    transformer = new Transformer(dataManager)
    registerForCleanup(transformer)
    
    // Set up consumer channel for listening to processed events
    consumerChannel = RabbitMQManager.createChannel("test-consumer")
    
    // Set up listener for processed events using the real queue
    setupProcessedEventsListener()
  }

  private def setupProcessedEventsListener(): Unit = {
    // Use the REAL queue name from production code
    val queueName = RabbitMQManager.Queues.PROCESSED_EVENTS
    
    // Declare the queue (idempotent operation)
    consumerChannel.queueDeclare(queueName, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.info(s"Test received processed event: $message")
        
        decode[ProcessedEvent](message) match {
          case Right(processedEvent) =>
            if (messagePromise != null && !messagePromise.isCompleted) {
              messagePromise.success(processedEvent)
            }
            consumerChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
          case Left(error) =>
            logger.error(s"Failed to parse processed event: $error")
            consumerChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer cancelled: $consumerTag")
      }
    }
    
    consumerTag = consumerChannel.basicConsume(queueName, false, deliverCallback, cancelCallback)
    logger.info(s"Set up processed events listener with consumer tag: $consumerTag")
  }
  
  "The Transformer" should "process streaming events correctly" in {
    // Reset promise for this test
    messagePromise = Promise[ProcessedEvent]()
    
    // Start the REAL transformer - this will consume from the real queue
    transformer.start()
    
    // Wait for transformer to be ready
    Thread.sleep(3000)
    
    // Create test event using existing test utilities
    val testEvent = TestDataFactory.createUserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L)
    )
    
    // Publish using the REAL infrastructure method
    // The transformer should have a method to publish or we can use RabbitMQ directly
    publishUserEventToRealQueue(testEvent)
    
    // Wait for the processed event with reasonable timeout
    val result = Await.result(messagePromise.future, 20.seconds)
    
    // Verify the processed event matches what the real transformer produces
    result.userId shouldBe testEvent.userId
    result.eventType shouldBe testEvent.eventType
    result.itemId shouldBe testEvent.itemId
  }
  
  private def publishUserEventToRealQueue(event: UserEvent): Unit = {
    // Use the REAL exchange and routing key from production code
    val publishChannel = RabbitMQManager.createChannel("test-publisher")
    
    try {
      val exchange = RabbitMQManager.Exchanges.USER_ACTIONS
      val routingKey = RabbitMQManager.RoutingKeys.USER_ACTIONS
      
      // Declare exchange (idempotent)
      publishChannel.exchangeDeclare(exchange, "topic", true)
      
      val props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .build()
      
      // Corriger cette ligne :
      val message = event.asJson.noSpaces
      
      publishChannel.basicPublish(exchange, routingKey, props, message.getBytes(StandardCharsets.UTF_8))
      logger.info(s"Published test user event: $message")
    } finally {
      RabbitMQManager.closeChannel("test-publisher")
    }
  }
  
  override def afterAll(): Unit = {
    // Clean up consumer
    if (consumerTag != null && consumerChannel != null && consumerChannel.isOpen) {
      consumerChannel.basicCancel(consumerTag)
    }
    
    // Close consumer channel
    if (consumerChannel != null && consumerChannel.isOpen) {
      RabbitMQManager.closeChannel("test-consumer")
    }
    
    super.afterAll()
  }
}