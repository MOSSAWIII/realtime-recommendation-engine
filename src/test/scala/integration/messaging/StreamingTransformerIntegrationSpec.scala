package integration.transformation

import common.base.IntegrationTestBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import common.base.IntegrationSpec
import common.utils.{TestConfig, RabbitMQManager}
import application.core.{StreamingConsumer, Transformer}
import application.core.Repository
import domain.models.UserEvent
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import com.rabbitmq.client.{AMQP, DeliverCallback, CancelCallback}
import java.util.UUID
import java.nio.charset.StandardCharsets
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit}

class StreamingTransformerIntegrationSpec extends IntegrationTestBase with BeforeAndAfterAll {

  // Test configuration
  val testQueueName = s"test_events_${UUID.randomUUID().toString}"
  var repository: Repository = _
  var transformer: Transformer = Transformer()
  var consumer: TestStreamingConsumer = _
  // Add a promise to track message processing
  val messageProcessedPromise = Promise[Boolean]()

  override def beforeAll(): Unit = {
    // Log test mode status to help with debugging
    println(s"Running in test mode: ${TestConfig.isTestMode}")

    try {
      // Use a mock repository for tests instead of a real database connection
      repository = new MockRepository()
      
      // Use explicit ec parameter to resolve ambiguity
      transformer = new Transformer(repository)(ec)

      // Create a test queue with durable=true to match production settings
      val channel = TestMockManager.createChannel("test-setup")
      channel.queueDeclare(testQueueName, true, false, false, null)
      println(s"Test queue created: $testQueueName")
      
      // Create consumer with the promise to track message processing
      consumer = new TestStreamingConsumer(transformer, testQueueName, messageProcessedPromise)
      consumer.start()
      
      // Wait for consumer to initialize
      Thread.sleep(2000)
    } catch {
      case e: Exception =>
        println(s"Test setup failed: ${e.getMessage}")
        e.printStackTrace()
        fail("Test setup failed", e)
    }
  }

  override def afterAll(): Unit = {
    // Clean up resources
    if (consumer != null) {
      consumer.close()
    }
    
    val channel = TestMockManager.createChannel("cleanup")
    try {
      channel.queueDelete(testQueueName)
    } finally {
      TestMockManager.closeAllChannels()
    }
  }

  "StreamingConsumer and Transformer" should "process events and store them in the database" in {
    // Arrange: Prepare a test event with unique ID for tracking
    val testUserId = 99
    val testEvent = UserEvent(userId = testUserId, eventType = "test_view", itemId = Some(999))
    val eventJson = testEvent.asJson.noSpaces

    println(s"Publishing test event to queue $testQueueName: $eventJson")

    // Act: Publish the event to the queue with persistent delivery mode
    val channel = TestMockManager.createChannel("publisher")
    val props = new AMQP.BasicProperties.Builder()
      .deliveryMode(2) // Make message persistent
      .build()
    
    channel.basicPublish("", testQueueName, props, eventJson.getBytes(StandardCharsets.UTF_8))
    
    // Wait for message processing with timeout
    val processed = try {
      Await.result(messageProcessedPromise.future, 10.seconds)
      true
    } catch {
      case _: Exception => false
    }
    
    // Additional wait to ensure repository operations complete
    Thread.sleep(1000)
    
    // Debug output
    println(s"Message processed: $processed")
    val storedEvents = Await.result(repository.getProcessedEventsForUser(testUserId), 5.seconds)
    println(s"Stored events count: ${storedEvents.size}")
    
    // Assert: Verify that the event has been stored
    storedEvents.size should be >= 1
    storedEvents.exists(e => e.userId == testUserId && e.eventType == "test_view" && e.itemId.contains(999)) shouldBe true
  }

  "Transformer" should "traiter des lots d'événements efficacement" in {
    // Configurer test
    val eventsCount = 100
    val events = (1 to eventsCount).map(i => createTestEvent(i))
    val latch = new CountDownLatch(eventsCount)
    val mockDataManager = new MockDataManager(latch)
    val transformer = new TestTransformer(mockDataManager, testQueueName, Promise[Boolean]())
    
    // Démarrer le composant
    transformer.start()
    
    // Publier les événements
    val startTime = System.currentTimeMillis()
    events.foreach(publishEvent)
    
    // Attendre que tous soient traités avec timeout
    val allProcessed = latch.await(30, TimeUnit.SECONDS)
    val endTime = System.currentTimeMillis()
    
    // Vérifications
    allProcessed shouldBe true
    mockDataManager.storedEvents should have size eventsCount
    
    // Assertions de performance
    val processingTime = endTime - startTime
    println(s"Traitement de $eventsCount événements en $processingTime ms")
    processingTime should be < 15000L  // Moins de 15 secondes
    
    // Vérifier les métriques internes
    val metrics = transformer.getMetricsData() // Méthode exposant les données brutes
    metrics("totalProcessed") shouldBe eventsCount
    metrics("averageProcessingTime").asInstanceOf[Double] should be < 100.0  // Moyenne < 100ms
  }

  "Transformer" should "gérer correctement les erreurs de DataManager" in {
    // Créer un DataManager qui échoue délibérément
    val failingDataManager = new MockDataManager() {
      override def storeProcessedEvent(event: domain.models.ProcessedEvent): Future[domain.models.ProcessedEvent] = 
        Future.failed(new RuntimeException("Test failure"))
    }
    
    val transformer = new TestTransformer(failingDataManager, testQueueName, Promise[Boolean]())
    transformer.start()
    
    // Publier un événement
    val event = UserEvent(userId = 123L, eventType = "test", itemId = Some(456L), 
                        properties = None, timestamp = System.currentTimeMillis())
    publishEvent(event)
    
    // Attendre un moment pour le traitement
    Thread.sleep(2000)
    
    // Vérifier que le transformer continue de fonctionner malgré l'erreur
    transformer.isActive shouldBe true // isActive est une méthode de test pour vérifier l'état
    transformer.errorCounter.get() shouldBe 1 // Le compteur d'erreurs a été incrémenté
  }

  "DataManager" should "être correctement utilisé par le Recommender" in {
    // Configurer le mockDataManager avec des données test
    mockDataManager.setupMockData(
      users = List(testUser),
      events = List(testEvent),
      processedEvents = List(testProcessedEvent),
      friendships = List(testFriendship)
    )
    
    // Créer un recommender avec ce DataManager
    val recommender = new Recommender(mockDataManager)
    
    // Générer une recommandation
    val requestFuture = recommender.recommendForUser(testRequest)
    val recommendation = Await.result(requestFuture, 5.seconds)
    
    // Vérifier que la recommandation utilise les données du DataManager
    recommendation.userId shouldBe testRequest.userId
    recommendation.recommendedItems.exists(_.itemId == testEvent.id.get) shouldBe true
    
    // Vérifier que la recommandation a été stockée dans le DataManager
    val storedRecommendation = Await.result(
      mockDataManager.getRecentRecommendationForUser(testRequest.userId, 60000), 5.seconds)
    
    storedRecommendation shouldBe defined
    storedRecommendation.get.userId shouldBe testRequest.userId
  }

  // Custom consumer that listens to our test queue
  class TestStreamingConsumer(
    transformer: Transformer, 
    queueName: String,
    processPromise: Promise[Boolean]
  ) extends StreamingConsumer(transformer) {
    
    private var consumerTag: String = _
    private var channel: com.rabbitmq.client.Channel = _
    
    override def start(): Unit = {
      println(s"Starting test consumer for queue: $queueName")
      channel = TestMockManager.createChannel("test-consumer")
      
      // Ensure the queue exists
      channel.queueDeclare(queueName, true, false, false, null)

      val deliverCallback = new DeliverCallback {
        override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
          val message = new String(delivery.getBody, StandardCharsets.UTF_8)
          println(s"Test consumer received message: $message")
          
          decode[UserEvent](message) match {
            case Right(event) =>
              println(s"Processing event for user ${event.userId}")
              try {
                processEvent(event) 
                println(s"Successfully processed event for user ${event.userId}")
                // Signal that processing completed successfully
                if (!processPromise.isCompleted) {
                  processPromise.success(true)
                }
              } catch {
                case e: Exception => 
                  println(s"Error processing event: ${e.getMessage}")
                  e.printStackTrace()
                  if (!processPromise.isCompleted) {
                    processPromise.failure(e)
                  }
              }
            case Left(error) =>
              println(s"Failed to decode message: ${error.getMessage}")
              if (!processPromise.isCompleted) {
                processPromise.failure(new RuntimeException(s"Failed to decode message: ${error.getMessage}"))
              }
          }
          
          // Always acknowledge the message
          channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        }
      }

      val cancelCallback = new CancelCallback {
        override def handle(consumerTag: String): Unit = {
          println(s"Consumer cancelled: $consumerTag")
        }
      }
      
      // Start consuming with manual ack mode (false)
      consumerTag = channel.basicConsume(queueName, false, deliverCallback, cancelCallback)
      println(s"Test consumer started and listening to queue: $queueName with tag: $consumerTag")
    }
    
    override def close(): Unit = {
      println("Closing test consumer")
      if (channel != null && channel.isOpen && consumerTag != null) {
        try {
          channel.basicCancel(consumerTag)
          println(s"Consumer with tag $consumerTag canceled")
        } catch {
          case e: Exception => println(s"Error canceling consumer: ${e.getMessage}")
        }
      }
      super.close()
    }
  }

  // MockRepository implementation with better logging
  class MockRepository extends Repository()(ec) {
    import scala.collection.mutable.ArrayBuffer

    val storedEvents = ArrayBuffer[domain.models.ProcessedEvent]()

    override def storeProcessedEvent(event: domain.models.ProcessedEvent): Future[domain.models.ProcessedEvent] = {
      println(s"Storing event in mock repository: $event")
      storedEvents += event
      Future.successful(event.copy(id = Some(1L)))
    }

    override def initTables(): Future[Unit] = Future.successful(())
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[domain.models.ProcessedEvent]] = {
      println(s"Getting events for user $userId, current count: ${storedEvents.size}")
      println(s"All stored events: ${storedEvents.toList}")
      Future.successful(storedEvents.filter(_.userId == userId).toList.take(limit))
    }
  }
}