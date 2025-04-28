package integration.streaming

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{StreamingConsumer, Transformer}
import repositories.Repository
import config.{RabbitMQManager, RabbitMQConfig, DatabaseConfig}
import models.{UserEvent, ProcessedEvent, Event}
import io.circe.syntax._
import io.circe.generic.auto._
import java.util.UUID
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import java.sql.Connection
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import com.typesafe.config.ConfigFactory
import com.rabbitmq.client.ConnectionFactory
import java.util.concurrent.TimeoutException

class StreamingTransformerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private val rabbitMQConfig = ConfigFactory.load("application-test.conf").getConfig("rabbitmq")

  // Queues de test uniques pour éviter les conflits
  val testQueueName = s"test_events_${UUID.randomUUID().toString.replaceAll("-", "")}"
  
  // Composants à tester
  var repository: TestRepository = _
  var transformer: Transformer = _
  var consumer: StreamingConsumer = _
  
  override def beforeAll(): Unit = {
    try {
      // Afficher la configuration RabbitMQ
      logger.info(s"RabbitMQ Host: ${rabbitMQConfig.getString("host")}")
      logger.info(s"RabbitMQ Port: ${rabbitMQConfig.getInt("port")}")
      logger.info(s"RabbitMQ Username: ${rabbitMQConfig.getString("username")}")
      logger.info(s"Using test queue: $testQueueName")
      
      // Vérifier connexion RabbitMQ avant de continuer
      val factory = new ConnectionFactory()
      factory.setHost(rabbitMQConfig.getString("host"))
      factory.setPort(rabbitMQConfig.getInt("port"))
      factory.setUsername(rabbitMQConfig.getString("username"))
      factory.setPassword(rabbitMQConfig.getString("password"))
      val connection = factory.newConnection()
      connection.close()
      
      // Si connexion réussie, initialiser les composants
      repository = new TestRepository()
      transformer = new Transformer(repository)
      
      // Configurer le canal RabbitMQ
      val channel = RabbitMQManager.createChannel("test-publisher")
      channel.queueDeclare(testQueueName, false, false, true, null)
      
      // Créer StreamingConsumer qui utilise notre transformer
      consumer = new TestStreamingConsumer(transformer, testQueueName)
      consumer.start()
      
      // Attendre que tout soit initialisé
      Thread.sleep(1000)
    } catch {
      case e: Exception => 
        logger.error(s"Failed to initialize test: ${e.getMessage}", e)
        fail(s"Test setup failed: ${e.getMessage}")
    }
  }
  
  override def afterAll(): Unit = {
    try {
      // Nettoyer
      if (consumer != null) consumer.close()
      if (transformer != null) transformer.close()
      
      // Supprimer la queue de test
      val channel = RabbitMQManager.createChannel("test-cleanup")
      try {
        channel.queueDelete(testQueueName)
      } finally {
        channel.close()
      }
    } catch {
      case e: Exception => logger.error(s"Error during test cleanup: ${e.getMessage}", e)
    }
  }
  
  "StreamingConsumer and Transformer integration" should "process events end-to-end" in {
    logger.info("Starting StreamingTransformerIntegrationSpec test")
    
    // Créer une promesse avec timeout plus long
    val eventPromise = Promise[ProcessedEvent]()
    repository.nextEventPromise = eventPromise
    
    val timestamp = System.currentTimeMillis()
    val event = Event(
      id = Some(1L),
      name = "Test Event",
      description = Some("Description"),
      startTime = timestamp,
      endTime = Some(timestamp + 3600000),
      location = Some("Location"),
      createdBy = 1L, // Not organizerId
      status = "active", // Required field
      keywords = List.empty,
      keywordFrequencies = Map.empty
    )
    
    // Publier le message
    val channel = RabbitMQManager.createChannel("test-publisher")
    try {
      val json = event.asJson.noSpaces
      logger.info(s"Publishing test event: $json")
      channel.basicPublish("", testQueueName, null, json.getBytes(StandardCharsets.UTF_8))
      
      // Utiliser un timeout plus long et gérer les échecs
      implicit val timeoutDuration: FiniteDuration = 30.seconds
      val futureResult = eventPromise.future
      
      futureResult.onComplete {
        case Success(event) => logger.info(s"Event processing succeeded: $event")
        case Failure(ex) => logger.error(s"Event processing failed: ${ex.getMessage}", ex)
      }(global)
      
      val result = try {
        Await.result(futureResult, timeoutDuration)
      } catch {
        case e: TimeoutException => 
          fail(s"Test timed out after ${timeoutDuration.toSeconds} seconds - RabbitMQ or Transformer might not be working correctly")
      }
      
      // Assertions
      result.userId shouldBe 999
      result.eventType shouldBe "product_view"
      result.itemId shouldBe Some(888)
      result.properties should not be empty
      result.properties should contain key "processed_at"
      result.properties should contain key "source"
      result.properties("source") shouldBe "mobile_app"
      
      // Vérifier les timestamps
      result.timestamp should be > 0L
      result.processingTimestamp should be > result.timestamp
    } finally {
      channel.close()
    }
  }
}

// Classes d'aide pour le test
class TestRepository extends Repository()(global) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  var nextEventPromise: Promise[ProcessedEvent] = _
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    logger.info(s"Repository processing event: $event")
    println(s"Repository processing event: $event")
    // Compléter la promesse pour signaler que l'événement a été traité
    if (nextEventPromise != null && !nextEventPromise.isCompleted) {
      nextEventPromise.success(event)
    }
    
    Future.successful(event.copy(id = Some(999L)))
  }
}

class TestStreamingConsumer(transformer: Transformer, testQueueName: String)(implicit ec: ExecutionContext) 
  extends StreamingConsumer(transformer)(ec) {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def start(): Unit = {
    val channel = RabbitMQManager.createChannel("test-consumer")
    channel.queueDeclare(testQueueName, false, false, true, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        logger.info(s"Received message: $message")
        
        import io.circe.parser._
        decode[UserEvent](message) match {
          case Right(event) =>
            try {
              processEvent(event)
              channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
              logger.info(s"Successfully processed event, sent ACK")
            } catch {
              case ex: Exception =>
                logger.error(s"Error processing event: ${ex.getMessage}", ex)
                channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
            }
          case Left(error) =>
            logger.error(s"Failed to decode user event: ${error.getMessage}", error)
            // Message mal formaté, ne pas reessayer
            channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer was canceled: $consumerTag")
      }
    }
    
    channel.basicConsume(testQueueName, false, deliverCallback, cancelCallback)
    logger.info(s"Started listening on queue: $testQueueName")
  }
}

class TestableRepository(mockConnection: Connection)(override implicit val ec: ExecutionContext) extends Repository()(ec) {
  
  // Change from override to a new method
  protected def withConnection[T](operation: Connection => T): T = {
    operation(mockConnection)
  }
}