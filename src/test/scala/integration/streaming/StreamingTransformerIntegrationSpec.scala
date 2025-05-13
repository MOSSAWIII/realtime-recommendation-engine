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
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.slf4j.LoggerFactory
import scala.util.{Success, Failure}

class StreamingTransformerIntegrationSpec extends IntegrationSpec with BeforeAndAfterAll {
  
  // Replace println with proper logging
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Test configuration
  val testQueueName = s"test_events_${UUID.randomUUID().toString}"
  var repository: MockRepository = _
  var transformer: Transformer = _
  var consumer: TestStreamingConsumer = _
  var consumerTag: String = _
  
  // Create a latch to wait for event processing completion
  val processingLatch = new CountDownLatch(1)

  override def beforeAll(): Unit = {
    logger.info(s"Starting integration test with test mode: ${TestConfig.isTestMode}")
    logger.info(s"Using test queue: $testQueueName")

    try {
      // Create and initialize the repository with the latch for signaling
      repository = new MockRepository(processingLatch)
      transformer = new Transformer(repository)(ec)

      // Ensure queue exists and is empty
      val setupChannel = RabbitMQManager.createChannel("test-setup")
      setupChannel.queueDeclare(testQueueName, true, false, false, null)
      setupChannel.queuePurge(testQueueName)
      
      // Create and start consumer
      consumer = new TestStreamingConsumer(transformer, testQueueName, Promise.successful(true))
      consumer.start()
      
      logger.info("Test setup completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Test setup failed: ${e.getMessage}", e)
        fail(s"Test setup failed: ${e.getMessage}")
    }
  }

  override def afterAll(): Unit = {
    logger.info("Cleaning up test resources")
    
    try {
      // Close consumer if it exists
      if (consumer != null) {
        consumer.close()
      }
      
      // Clean up queue
      val cleanupChannel = RabbitMQManager.createChannel("cleanup")
      if (cleanupChannel != null && cleanupChannel.isOpen) {
        try {
          cleanupChannel.queuePurge(testQueueName)
          cleanupChannel.queueDelete(testQueueName)
          logger.info(s"Test queue $testQueueName purged and deleted")
        } catch {
          case e: Exception => logger.warn(s"Error cleaning up queue: ${e.getMessage}")
        }
      }
    } finally {
      // Ensure all channels are closed
      RabbitMQManager.closeAllChannels()
      logger.info("All RabbitMQ channels closed")
    }
  }

  "StreamingConsumer and Transformer" should "process events and store them in the database" in {
    // Create test event with detailed properties for better assertions
    val testUserId = 99L
    val testItemId = 999L
    val testEventType = "test_view"
    val testProperties = Map("category" -> "test", "source" -> "integration_test")
    
    val testEvent = UserEvent(
      userId = testUserId, 
      eventType = testEventType, 
      itemId = Some(testItemId),
      properties = Some(testProperties),
      timestamp = System.currentTimeMillis()
    )
    
    val eventJson = testEvent.asJson.noSpaces
    logger.info(s"Publishing test event to queue $testQueueName: $eventJson")

    try {
      // Publish event with persistent delivery mode
      val channel = RabbitMQManager.createChannel("publisher")
      val props = new AMQP.BasicProperties.Builder()
        .deliveryMode(2) // Persistent
        .build()
        
      channel.basicPublish("", testQueueName, props, eventJson.getBytes(StandardCharsets.UTF_8))
      logger.info("Test event published successfully")
      
      // Wait for processing to complete with timeout
      val success = processingLatch.await(10, TimeUnit.SECONDS)
      
      if (!success) {
        fail("Timed out waiting for event processing to complete")
      }
      
      // Retrieve stored events and perform detailed assertions
      val storedEvents = Await.result(repository.getProcessedEventsForUser(testUserId), 5.seconds)
      logger.info(s"Retrieved ${storedEvents.size} events for user $testUserId")
      
      // 1. Verify we have events
      storedEvents should not be empty
      
      // 2. Find the specific event we're looking for
      val matchingEvent = storedEvents.find(e => 
        e.userId == testUserId && 
        e.eventType == testEventType && 
        e.itemId.contains(testItemId)
      )
      
      // 3. Perform detailed assertions on the event
      matchingEvent shouldBe defined
      matchingEvent.foreach { event =>
        logger.info(s"Found matching event: $event")
        event.userId shouldBe testUserId
        event.eventType shouldBe testEventType
        event.itemId.get shouldBe testItemId
        
        // Verify properties were correctly transferred
        event.properties should contain key "category"
        event.properties should contain key "source"
        event.properties("category") shouldBe "test"
        event.properties("source") shouldBe "integration_test"
        
        // Verify timestamp fields are set
        event.timestamp should be > 0L
        event.processingTimestamp should be > 0L
      }
    } catch {
      case e: Exception =>
        logger.error("Test failed with exception", e)
        fail(s"Test failed: ${e.getMessage}")
    }
  }

  "StreamingConsumer" should "process multiple events in sequence correctly" in {
    val eventCount = 5
    val testUserId = 101L
    val latch = new CountDownLatch(eventCount)
    
    // Remplacer le repository pour utiliser un nouveau latch
    repository = new MockRepository(latch)
    transformer = new Transformer(repository)(ec)
    
    // Publier plusieurs événements
    val events = (1 to eventCount).map { i =>
      UserEvent(
        userId = testUserId,
        eventType = s"test_event_$i",
        itemId = Some(1000L + i),
        properties = Some(Map("index" -> i.toString)),
        timestamp = System.currentTimeMillis()
      )
    }

    events.foreach { event =>
      val eventJson = event.asJson.noSpaces
      val channel = RabbitMQManager.createChannel("publisher-multiple")
      channel.basicPublish("", testQueueName, null, eventJson.getBytes(StandardCharsets.UTF_8))
    }
    
    // Attendre que tous les événements soient traités
    val allProcessed = latch.await(15, TimeUnit.SECONDS)
    allProcessed shouldBe true
    
    // Vérifier que tous les événements ont été stockés
    val storedEvents = Await.result(repository.getProcessedEventsForUser(testUserId), 5.seconds)
    storedEvents.size shouldBe eventCount
  }

  "StreamingConsumer" should "handle malformed messages gracefully" in {
    val validEvent = UserEvent(
      userId = 102L,
      eventType = "test_view",
      itemId = Some(200L),
      properties = Some(Map("test" -> "valid")),
      timestamp = System.currentTimeMillis()
    )
    
    // Créer un nouveau latch pour ce test
    val latch = new CountDownLatch(1)
    repository = new MockRepository(latch)
    transformer = new Transformer(repository)(ec)
    
    val channel = RabbitMQManager.createChannel("publisher-malformed")
    
    // Publier d'abord un message mal formé
    val malformedJson = """{"userId": 102, "eventType": "broken_event", malformed_json}"""
    channel.basicPublish("", testQueueName, null, malformedJson.getBytes(StandardCharsets.UTF_8))
    
    // Puis un message valide pour vérifier que le système continue de fonctionner
    Thread.sleep(1000) // Attendre que le message mal formé soit traité
    channel.basicPublish("", testQueueName, null, validEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    
    // L'événement valide devrait être traité correctement
    val processed = latch.await(10, TimeUnit.SECONDS)
    processed shouldBe true
    
    val storedEvents = Await.result(repository.getProcessedEventsForUser(102L), 5.seconds)
    storedEvents.size shouldBe 1
    storedEvents.head.eventType shouldBe "test_view"
  }

  "StreamingConsumer" should "process events with missing optional fields" in {
    // Événement sans itemId et sans properties
    val minimalEvent = UserEvent(
      userId = 103L,
      eventType = "minimal_event",
      itemId = None,
      properties = None,
      timestamp = System.currentTimeMillis()
    )
    
    val latch = new CountDownLatch(1)
    repository = new MockRepository(latch)
    transformer = new Transformer(repository)(ec)
    
    val channel = RabbitMQManager.createChannel("publisher-minimal")
    channel.basicPublish("", testQueueName, null, minimalEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    
    val processed = latch.await(10, TimeUnit.SECONDS)
    processed shouldBe true
    
    val storedEvents = Await.result(repository.getProcessedEventsForUser(103L), 5.seconds)
    storedEvents.size shouldBe 1
    storedEvents.head.eventType shouldBe "minimal_event"
    storedEvents.head.itemId shouldBe None
    storedEvents.head.properties shouldBe empty
  }

  "StreamingConsumer" should "handle duplicate events appropriately" in {
    val testUserId = 104L
    val testEvent = UserEvent(
      userId = testUserId,
      eventType = "duplicate_test",
      itemId = Some(444L),
      properties = Some(Map("duplicate" -> "true")),
      timestamp = System.currentTimeMillis()
    )
    
    // Pour ce test, nous voulons suivre tous les événements reçus
    val latch = new CountDownLatch(3) // Nous envoyons 3 fois le même événement
    repository = new MockRepository(latch)
    transformer = new Transformer(repository)(ec)
    
    val channel = RabbitMQManager.createChannel("publisher-duplicate")
    
    // Publier le même événement plusieurs fois
    for (_ <- 1 to 3) {
      channel.basicPublish("", testQueueName, null, testEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    }
    
    val processed = latch.await(10, TimeUnit.SECONDS)
    processed shouldBe true
    
    // L'événement devrait être stocké plusieurs fois (pas d'idempotence par défaut)
    val storedEvents = Await.result(repository.getProcessedEventsForUser(testUserId), 5.seconds)
    storedEvents.size shouldBe 3
    
    // Tous les événements devraient avoir les mêmes attributs
    storedEvents.foreach { event =>
      event.userId shouldBe testUserId
      event.eventType shouldBe "duplicate_test"
      event.itemId shouldBe Some(444L)
    }
  }

  "StreamingConsumer" should "handle repository timeout gracefully" in {
    val testUserId = 105L
    val testEvent = UserEvent(
      userId = testUserId,
      eventType = "timeout_test",
      itemId = Some(555L),
      timestamp = System.currentTimeMillis()
    )
    
    // Create a specific latch for this test
    val timeoutLatch = new CountDownLatch(2) // Expect 2 events
    
    // Create a slow repository that simulates delay
    val slowRepository = new MockRepository(timeoutLatch) {
      override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
        if (event.userId == testUserId && event.eventType == "timeout_test") {
          // Simulate a slow operation
          Future {
            logger.info("Simulating slow operation (3 seconds delay)")
            Thread.sleep(3000) // 3 second delay
            val result = super.storeProcessedEvent(event)
            // Use Await.result properly to block until the future completes
            Await.result(result, 5.seconds)
          }(ec)
        } else {
          super.storeProcessedEvent(event)
        }
      }
    }
    
    // Set up a new transformer and consumer with the slow repository
    transformer = new Transformer(slowRepository)(ec)
    consumer = new TestStreamingConsumer(transformer, testQueueName, Promise.successful(true))
    
    // Clear any existing messages
    val setupChannel = RabbitMQManager.createChannel("setup-timeout-test")
    setupChannel.queuePurge(testQueueName)
    RabbitMQManager.closeChannel("setup-timeout-test")
    
    val channel = RabbitMQManager.createChannel("publisher-timeout")
    
    // Publish a test event that will be processed slowly
    channel.basicPublish("", testQueueName, null, testEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    logger.info("Published slow-processing event")
    
    // Wait to ensure the slow processing is in progress
    Thread.sleep(1000)
    
    // Send a second event to verify that the system is still operational
    val secondEvent = testEvent.copy(eventType = "after_timeout")
    channel.basicPublish("", testQueueName, null, secondEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    logger.info("Published second event to verify system responsiveness")
    
    // Wait for both events to be processed (with timeout)
    val bothProcessed = timeoutLatch.await(15, TimeUnit.SECONDS)
    bothProcessed shouldBe true
    
    // Verify both events were stored
    val storedEvents = Await.result(slowRepository.getProcessedEventsForUser(testUserId), 5.seconds)
    storedEvents.size shouldBe 2
    logger.info("Repository timeout test completed successfully")
  }

  "Transformer" should "add metadata to processed events" in {
    val testUserId = 106L
    val testEvent = UserEvent(
      userId = testUserId,
      eventType = "transaction",
      itemId = Some(606L),
      properties = Some(Map(
        "amount" -> "99.99",
        "currency" -> "EUR",
        "product" -> "premium_subscription"
      )),
      timestamp = System.currentTimeMillis()
    )
    
    val latch = new CountDownLatch(1)
    repository = new MockRepository(latch)
    transformer = new Transformer(repository)(ec)
    
    val channel = RabbitMQManager.createChannel("publisher-tx")
    channel.basicPublish("", testQueueName, null, testEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    
    val processed = latch.await(10, TimeUnit.SECONDS)
    processed shouldBe true
    
    val storedEvents = Await.result(repository.getProcessedEventsForUser(testUserId), 5.seconds)
    storedEvents.size shouldBe 1
    
    // Vérifier que les propriétés sont correctement transmises
    val event = storedEvents.head
    event.properties("amount") shouldBe "99.99"
    event.properties("currency") shouldBe "EUR"
    event.properties("product") shouldBe "premium_subscription"
    
    // Vérifier l'ajout de metadata par le transformer (si applicable)
    event.processingTimestamp should be > 0L
  }

  "Transformer" should "process a large batch of events efficiently" in {
    val eventCount = 100
    val testUserId = 107L
    
    // Create a latch to track event processing
    val batchLatch = new CountDownLatch(eventCount)
    
    // Set up the repository with the latch
    val batchRepository = new MockRepository(batchLatch)
    
    val testQueueName = s"test_events_${UUID.randomUUID}"
    val channel = RabbitMQManager.createChannel("publisher-perf")
    
    channel.queueDeclare(testQueueName, true, false, false, null)
    
    val startTime = System.currentTimeMillis()
    val testConsumer = new TestStreamingConsumer(new Transformer(batchRepository), testQueueName, Promise.successful(true))
    testConsumer.start()
    
    // Give consumer time to initialize before publishing
    Thread.sleep(500)
    
    // Publish events
    logger.info(s"Publishing $eventCount events...")
    for (i <- 1 to eventCount) {
      val event = UserEvent(
        userId = testUserId,
        eventType = "perf_test",
        itemId = Some(700L + i),
        properties = Some(Map("batch" -> "performance", "index" -> i.toString)),
        timestamp = System.currentTimeMillis()
      )
      
      channel.basicPublish("", testQueueName, null, event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      
      // Add small pauses to prevent overwhelming the message broker
      if (i % 20 == 0) Thread.sleep(100)
    }
    
    logger.info(s"Published $eventCount events, waiting for processing to complete (timeout: 30 seconds)")
    
    // Wait for all events to be processed with a reasonable timeout
    val allProcessed = batchLatch.await(30, TimeUnit.SECONDS)
    val endTime = System.currentTimeMillis()
    val processingTime = endTime - startTime
    
    logger.info(s"Processing completed: success=$allProcessed, time=${processingTime}ms")
    
    // Check how many events were actually stored rather than just relying on the latch
    val storedEventCount = batchRepository.storedEvents.size
    logger.info(s"Processed $storedEventCount out of $eventCount events")
    
    // Test passes if either latch counted down or all events were stored
    val testSuccess = allProcessed || (storedEventCount >= eventCount)
    testSuccess shouldBe true
    
    if (!allProcessed && storedEventCount >= eventCount) {
      logger.warn(s"Latch didn't complete but all $storedEventCount events were processed. Potential latch issue.")
    }
    
    val remaining = batchLatch.getCount
    if (remaining > 0) {
      val processed = eventCount - remaining.toInt
      logger.error(s"Performance test timed out with $remaining/$eventCount events still pending")
    }
    
    // Fix: Calculate events per second as a Double to avoid type mismatch error
    val eventsPerSecond = storedEventCount.toDouble * 1000.0 / processingTime.toDouble
    logger.info(s"Processing rate: $eventsPerSecond events/second")
    
    // Fix: Now comparing Double with Double
    eventsPerSecond should be > 10.0
  }

  // Updated consumer implementation
  class TestStreamingConsumer(
    transformer: Transformer, 
    queueName: String,
    processPromise: Promise[Boolean]
  ) extends StreamingConsumer(transformer) {
    
    private var consumerTag: String = _
    private val logger = LoggerFactory.getLogger(getClass)
    
    override def start(): Unit = {
      logger.info(s"Starting test streaming consumer for queue: $queueName")
      val channel = RabbitMQManager.createChannel("streaming-consumer")
      
      try {
        // Ensure the queue exists
        channel.queueDeclare(queueName, true, false, false, null)
        
        // Set QoS to avoid overwhelming the consumer
        channel.basicQos(10) // Process max 10 messages at a time
        
        val deliverCallback = new DeliverCallback {
          override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
            val message = new String(delivery.getBody, StandardCharsets.UTF_8)
            logger.debug(s"Received message: $message")
            
            try {
              decode[UserEvent](message) match {
                case Right(event) =>
                  logger.info(s"Processing event for user ${event.userId}")
                  
                  // Process the event and acknowledge after successful processing
                  transformer.processEvent(event).onComplete {
                    case Success(_) => 
                      logger.info(s"Successfully processed event for user ${event.userId}")
                      // Acknowledge the message after successful processing
                      channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                    case Failure(ex) => 
                      logger.error(s"Error processing event: ${ex.getMessage}", ex)
                      // Negative acknowledgment for failed processing, don't requeue to avoid infinite loop
                      channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                      
                      if (!processPromise.isCompleted) {
                        processPromise.failure(ex)
                      }
                  }
                case Left(error) =>
                  logger.error(s"Failed to decode message: ${error.getMessage}")
                  // Negative acknowledgment for malformed messages
                  channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                  
                  if (!processPromise.isCompleted) {
                    processPromise.failure(new RuntimeException(s"Failed to decode message: ${error.getMessage}"))
                  }
              }
            } catch {
              case e: Exception => 
                logger.error(s"Unexpected error processing message: ${e.getMessage}", e)
                // Negative acknowledgment for any exceptions
                channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
            }
          }
        }

        val cancelCallback = new CancelCallback {
          override def handle(consumerTag: String): Unit = {
            logger.info(s"Consumer cancelled: $consumerTag")
          }
        }
        
        // Start consuming with manual ack mode to ensure reliable message processing
        consumerTag = channel.basicConsume(queueName, false, deliverCallback, cancelCallback)
        logger.info(s"Test consumer started and listening to queue: $queueName with tag: $consumerTag")
      } catch {
        case e: Exception =>
          logger.error(s"Failed to start test consumer: ${e.getMessage}", e)
          throw e
      }
    }

    override def close(): Unit = {
      try {
        RabbitMQManager.closeChannel("streaming-consumer")
        logger.info("StreamingConsumer closed")
      } catch {
        case e: Exception =>
          logger.error(s"Error closing consumer: ${e.getMessage}", e)
      }
    }
  }

  // Update the MockRepository class implementation to fix the latch counting issue
  class MockRepository(private val latch: CountDownLatch = new CountDownLatch(0)) extends Repository()(ec) {
    import scala.collection.mutable.ArrayBuffer
    private val logger = LoggerFactory.getLogger(getClass)
    
    val storedEvents = ArrayBuffer[models.ProcessedEvent]()
    private val processedEventIds = scala.collection.mutable.Set[String]() // Track unique events
    
    override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
      // Always store the event first
      synchronized {
        storedEvents += event
      }
      
      logger.info(s"Storing event in mock repository: $event")
      
      // Generate a unique ID for this event to avoid duplicate countdowns
      val eventId = s"${event.userId}-${event.eventType}-${event.itemId.getOrElse("null")}-${event.timestamp}"
      
      // Only count down if this is the first time we're seeing this event
      synchronized {
        if (!processedEventIds.contains(eventId)) {
          processedEventIds.add(eventId)
          
          if (latch.getCount > 0) {
            latch.countDown()
            logger.debug(s"Latch countdown: ${latch.getCount} remaining")
          } else if (latch.getCount == 0) {
            logger.warn(s"Latch already at zero, event processed but not counted: $event")
          }
        } else {
          logger.debug(s"Duplicate event, not counting down: $event")
        }
      }
      
      Future.successful(event.copy(id = Some(1L)))
    }
    
    override def initTables(): Future[Unit] = Future.successful(())
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[models.ProcessedEvent]] = {
      val events = storedEvents.filter(_.userId == userId).toList.take(limit)
      logger.debug(s"Getting events for user $userId: found ${events.size}")
      Future.successful(events)
    }
    
    override def getRecentProcessedEvents(limit: Int): Future[Seq[models.ProcessedEvent]] = {
      Future.successful(storedEvents.take(limit).toSeq)
    }
    
    def reset(): Unit = {
      storedEvents.clear()
      processedEventIds.clear()
    }
  }
}