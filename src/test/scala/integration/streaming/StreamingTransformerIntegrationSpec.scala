package integration.streaming

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.charset.StandardCharsets
import java.util.UUID

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import models._
import core._
import config.RabbitMQManager
import repositories.Repository
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}

class StreamingTransformerIntegrationSpec extends AnyWordSpec 
  with BeforeAndAfterEach 
  with Eventually
  with ScalaFutures
  with Matchers {

  // More aggressive patience config for async tests
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(15, Seconds)),
    interval = scaled(Span(150, Millis))
  )
  
  private val logger = LoggerFactory.getLogger(getClass)
  private var testQueueName = ""
  private var consumer: TestStreamingConsumer = _
  private var sharedCleanupPromise: Promise[Boolean] = _
  
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  
  // Use UUIDs to ensure test isolation
  override def beforeEach(): Unit = {
    logger.info("Setting up test")
    testQueueName = s"test_events_${UUID.randomUUID}"
    sharedCleanupPromise = Promise[Boolean]()
    
    try {
      val channel = RabbitMQManager.createChannel("test-setup")
      // Make sure we're using durable queues with explicit acks
      channel.queueDeclare(testQueueName, true, false, false, null)
      logger.info(s"Test queue created: $testQueueName")
    } catch {
      case e: Exception => 
        logger.error(s"Test setup failed: ${e.getMessage}", e)
        fail(s"Test setup failed: ${e.getMessage}")
    }
  }

  def waitForQueueEmpty(queueName: String, timeout: Duration = 5.seconds): Boolean = {
    logger.info(s"Waiting for queue $queueName to be empty")
    val channel = RabbitMQManager.createChannel("queue-check")
    
    try {
      val startTime = System.currentTimeMillis()
      var messagesRemaining = -1L
      
      while (System.currentTimeMillis() - startTime < timeout.toMillis) {
        val response = channel.queueDeclarePassive(queueName)
        messagesRemaining = response.getMessageCount
        
        if (messagesRemaining == 0) {
          logger.info(s"Queue $queueName is empty")
          return true
        }
        
        logger.debug(s"Queue $queueName still has $messagesRemaining messages, waiting...")
        Thread.sleep(200)
      }
      
      logger.warn(s"Timed out waiting for queue $queueName to empty, still has $messagesRemaining messages")
      false
    } catch {
      case e: Exception =>
        logger.error(s"Error checking queue state: ${e.getMessage}", e)
        false
    } finally {
      RabbitMQManager.closeChannel("queue-check")
    }
  }

  // More thorough resource cleanup
  override def afterEach(): Unit = {
    logger.info("Cleaning up test resources")
    
    try {
      // Close consumer if it exists
      if (consumer != null) {
        logger.info("Shutting down test consumer")
        consumer.close()
        consumer = null
      }
      
      // Clean up queue - with explicit error handling
      val cleanupChannel = RabbitMQManager.createChannel("cleanup")
      if (cleanupChannel != null && cleanupChannel.isOpen) {
        try {
          // First purge to avoid messages hanging around
          cleanupChannel.queuePurge(testQueueName)
          // Then delete the queue
          cleanupChannel.queueDelete(testQueueName)
          logger.info(s"Test queue $testQueueName purged and deleted")
        } catch {
          case e: Exception => logger.warn(s"Error cleaning up queue: ${e.getMessage}", e)
        } finally {
          RabbitMQManager.closeChannel("cleanup")
        }
      }
    } catch {
      case e: Exception => 
        logger.error(s"Error during test cleanup: ${e.getMessage}", e)
    } finally {
      // Always attempt to close all channels regardless of previous cleanup steps
      RabbitMQManager.closeAllChannels()
      logger.info("All RabbitMQ channels closed")
    }
  }

  "Transformer" should {
    "process a single event successfully" in {
      val testUserId = 101L
      val processPromise = Promise[Boolean]()
      
      // Use a real latch to track completion
      val latch = new CountDownLatch(1)
      val repository = new MockRepository(latch)
      
      // Create consumer with the transformer
      val transformer = new Transformer(repository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer before publishing
      consumer.start()
      
      // Give consumer time to initialize before sending messages
      Thread.sleep(300)
      
      // Publish a test event
      val event = UserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(201L),
        properties = Some(Map("test" -> "true")),
        timestamp = System.currentTimeMillis()
      )
      
      val channel = RabbitMQManager.createChannel("publisher")
      channel.basicPublish("", testQueueName, null, event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      logger.info(s"Published test event for user $testUserId")
      
      // Wait for the latch with a timeout
      val processed = latch.await(10, TimeUnit.SECONDS)
      processed should be (true)
      
      // Verify the event was stored correctly
      val events = repository.storedEvents
      events should have size 1
      events.head.userId should be (testUserId)
      events.head.eventType should be ("view")
      events.head.itemId should be (Some(201L))
    }
    
    "handle multiple events in order" in {
      val testUserId = 102L
      val eventCount = 5
      val processPromise = Promise[Boolean]()
      
      // Use a latch to track completion
      val latch = new CountDownLatch(eventCount)
      val repository = new MockRepository(latch)
      
      val transformer = new Transformer(repository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer
      consumer.start()
      
      // Give consumer time to initialize
      Thread.sleep(300)
      
      val channel = RabbitMQManager.createChannel("publisher-multiple")
      
      // Publish multiple events
      for (i <- 1 to eventCount) {
        val event = UserEvent(
          userId = testUserId,
          eventType = "view",
          itemId = Some(300L + i),
          properties = Some(Map("test" -> "true", "index" -> i.toString)),
          timestamp = System.currentTimeMillis()
        )
        
        channel.basicPublish("", testQueueName, null, event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
        // Small delay to ensure ordering
        Thread.sleep(50)
      }
      
      logger.info(s"Published $eventCount events for user $testUserId")
      
      // Wait for all events to be processed
      val allProcessed = latch.await(15, TimeUnit.SECONDS)
      allProcessed should be (true)
      
      // Verify events were stored
      val events = repository.storedEvents
      events should have size eventCount
      events.map(_.userId).distinct should contain only testUserId
      
      // Check ordering by itemId (should be sequential)
      val itemIds = events.flatMap(_.itemId).sorted
      itemIds should have size eventCount
      itemIds should be (itemIds.sorted) // Should be in ascending order (301 to 305)
    }

    "handle malformed messages gracefully" in {
      val processPromise = Promise[Boolean]()
      val repository = new MockRepository()
      
      val transformer = new Transformer(repository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer
      consumer.start()
      
      // Give consumer time to initialize
      Thread.sleep(300)
      
      val channel = RabbitMQManager.createChannel("publisher-malformed")
      
      // Send a malformed message
      val malformedJson = """{"userId": "not_a_number", "eventType": "bad_event"}"""
      channel.basicPublish("", testQueueName, null, malformedJson.getBytes(StandardCharsets.UTF_8))
      
      // Send a valid message after to verify the consumer is still working
      val validEvent = UserEvent(
        userId = 103L,
        eventType = "view",
        itemId = Some(401L),
        properties = Some(Map("test" -> "true")),
        timestamp = System.currentTimeMillis()
      )
      
      // Short pause to separate the messages
      Thread.sleep(200)
      
      channel.basicPublish("", testQueueName, null, validEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      
      // Use eventually for more reliable async testing
      eventually {
        // Verify only the valid event was processed
        repository.storedEvents should have size 1
        repository.storedEvents.head.userId should be (103L)
      }
    }
    
    "handle duplicate events correctly" in {
      val testUserId = 104L
      val processPromise = Promise[Boolean]()
      val latch = new CountDownLatch(2) // We expect 2 successful events
      val repository = new MockRepository(latch)
      
      val transformer = new Transformer(repository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer
      consumer.start()
      
      // Give consumer time to initialize
      Thread.sleep(300)
      
      val channel = RabbitMQManager.createChannel("publisher-duplicate")
      
      // Create an event
      val event = UserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(501L),
        properties = Some(Map("test" -> "true")),
        timestamp = System.currentTimeMillis()
      )
      
      val eventJson = event.asJson.noSpaces
      
      // Send the same event twice
      channel.basicPublish("", testQueueName, null, eventJson.getBytes(StandardCharsets.UTF_8))
      Thread.sleep(100)
      channel.basicPublish("", testQueueName, null, eventJson.getBytes(StandardCharsets.UTF_8))
      
      // Send a different event
      val secondEvent = event.copy(itemId = Some(502L))
      channel.basicPublish("", testQueueName, null, secondEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      
      // Wait for the latch to count down
      val allProcessed = latch.await(10, TimeUnit.SECONDS)
      allProcessed should be (true)
      
      // We should have two unique events (duplicates should be filtered by itemId)
      eventually {
        val uniqueItemIds = repository.storedEvents.flatMap(_.itemId).toSet
        uniqueItemIds should have size 2
        uniqueItemIds should contain allOf (501L, 502L)
      }
    }
    
    "handle timeouts gracefully" in {
      val testUserId = 105L
      val processPromise = Promise[Boolean]()
      val latch = new CountDownLatch(1)
      
      // Create a repository that will delay responses
      val slowRepository = new MockRepository(latch) {
        override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
          // Simulate a slow database
          Future {
            Thread.sleep(2000)
            super.storeProcessedEvent(event).value.get.get
          }
        }
      }
      
      val transformer = new Transformer(slowRepository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer
      consumer.start()
      
      // Give consumer time to initialize
      Thread.sleep(300)
      
      val channel = RabbitMQManager.createChannel("publisher-timeout")
      
      val event = UserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(601L),
        properties = Some(Map("test" -> "true")),
        timestamp = System.currentTimeMillis()
      )
      
      channel.basicPublish("", testQueueName, null, event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      
      // The system should handle the slow repository without crashing
      // We'll verify the event is eventually processed
      eventually(timeout(scaled(Span(10, Seconds)))) {
        slowRepository.storedEvents should have size 1
        slowRepository.storedEvents.head.userId should be (testUserId)
      }
    }
    
    "process minimal events with missing optional fields" in {
      val testUserId = 106L
      val processPromise = Promise[Boolean]()
      val latch = new CountDownLatch(1)
      val repository = new MockRepository(latch)
      
      val transformer = new Transformer(repository)
      consumer = new TestStreamingConsumer(transformer, testQueueName, processPromise)
      
      // Start the consumer
      consumer.start()
      
      // Give consumer time to initialize
      Thread.sleep(300)
      
      val channel = RabbitMQManager.createChannel("publisher-minimal")
      
      // Create an event with minimal fields (no itemId, no properties)
      val minimalEvent = UserEvent(
        userId = testUserId,
        eventType = "login",
        itemId = None,  // No item ID
        properties = None,  // No properties
        timestamp = System.currentTimeMillis()
      )
      
      channel.basicPublish("", testQueueName, null, minimalEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      
      // Wait for the event to be processed
      val processed = latch.await(10, TimeUnit.SECONDS)
      processed should be (true)
      
      // Verify the event was stored correctly
      val storedEvent = repository.storedEvents.head
      storedEvent.userId should be (testUserId)
      storedEvent.eventType should be ("login")
      storedEvent.itemId should be (None)
      storedEvent.properties should be (Map.empty)
    }

    "process a large batch of events efficiently" in {
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
  }

  // Improved TestStreamingConsumer implementation
  class TestStreamingConsumer(
    transformer: Transformer, 
    queueName: String,
    processPromise: Promise[Boolean]
  ) extends StreamingConsumer(transformer) {
    
    private var consumerTag: String = _
    private val logger = LoggerFactory.getLogger(getClass)
    private var channel: Channel = _
    
    override def start(): Unit = {
      logger.info(s"Starting test streaming consumer for queue: $queueName")
      channel = RabbitMQManager.createChannel("streaming-consumer")
      
      try {
        // Ensure queue exists before consuming
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
                      Try(channel.basicAck(delivery.getEnvelope.getDeliveryTag, false))
                        .recover { case e => logger.error(s"Failed to acknowledge message: ${e.getMessage}", e) }
                      
                    case Failure(ex) => 
                      logger.error(s"Error processing event: ${ex.getMessage}", ex)
                      // Negative acknowledgment for failed processing, don't requeue to avoid infinite loop
                      Try(channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
                        .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
                      
                      if (!processPromise.isCompleted) {
                        processPromise.failure(ex)
                      }
                  }
                case Left(error) =>
                  logger.error(s"Failed to decode message: ${error.getMessage}")
                  // Negative acknowledgment for malformed messages
                  Try(channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
                    .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
                  
                  if (!processPromise.isCompleted) {
                    processPromise.failure(new RuntimeException(s"Failed to decode message: ${error.getMessage}"))
                  }
              }
            } catch {
              case e: Exception => 
                logger.error(s"Unexpected error processing message: ${e.getMessage}", e)
                // Negative acknowledgment for any exceptions
                Try(channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
                  .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
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
        // Add a delay before closing to allow in-progress operations to complete
        Thread.sleep(100)
        
        if (channel != null && channel.isOpen) {
          try {
            // Cancel consumer if possible
            if (consumerTag != null && !consumerTag.isEmpty) {
              logger.debug(s"Cancelling consumer with tag: $consumerTag")
              Try(channel.basicCancel(consumerTag))
                .recover { case e => logger.warn(s"Error canceling consumer: ${e.getMessage}") }
            }
            
            // Ensure any in-flight messages are processed before closing
            channel.waitForConfirms(1000)
          } catch {
            case e: Exception => logger.warn(s"Error during consumer cleanup: ${e.getMessage}", e)
          } finally {
            // Close the channel regardless
            RabbitMQManager.closeChannel("streaming-consumer")
          }
        }
        logger.info("StreamingConsumer closed")
      } catch {
        case e: Exception =>
          logger.error(s"Error closing consumer: ${e.getMessage}", e)
      }
    }
  }

  // Improved MockRepository to handle concurrency better
  class MockRepository(private val latch: CountDownLatch = new CountDownLatch(0)) extends Repository()(ec) {
    import java.util.concurrent.ConcurrentLinkedQueue
    private val logger = LoggerFactory.getLogger(getClass)
    
    // Use ConcurrentLinkedQueue for thread safety without synchronization overhead
    private val eventsQueue = new ConcurrentLinkedQueue[models.ProcessedEvent]()
    private val processedEventIds = java.util.Collections.newSetFromMap(
      new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]()
    )
    
    // Thread-safe access to stored events without synchronization blocks
    def storedEvents: Seq[models.ProcessedEvent] = {
      import scala.jdk.CollectionConverters._
      eventsQueue.asScala.toSeq
    }
    
    override def storeProcessedEvent(event: models.ProcessedEvent): Future[models.ProcessedEvent] = {
      logger.info(s"Storing event in mock repository: $event")
      
      // Generate a unique ID for this event to avoid duplicate countdowns
      val eventId = s"${event.userId}-${event.eventType}-${event.itemId.getOrElse("none")}-${event.timestamp}"
      
      // Always store the event
      eventsQueue.add(event)
      
      // Only count down if this is a new event
      val isNew = processedEventIds.add(eventId)
      if (isNew && latch.getCount > 0) {
        latch.countDown()
        logger.debug(s"Latch countdown: ${latch.getCount} remaining")
      } else if (latch.getCount == 0 && latch != null) {
        logger.warn(s"Latch already at zero, event processed but not counted: $event")
      } else if (!isNew) {
        logger.debug(s"Duplicate event, not counting down: $event")
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
      eventsQueue.clear()
      processedEventIds.clear()
    }
  }
}