package unit.core

import core.{StreamingConsumer, Transformer}
import models.{UserEvent, ProcessedEvent}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class StreamingConsumerSpec extends AnyFlatSpec with Matchers {

  "StreamingConsumer" should "process events by forwarding them to the transformer" in {
    // Setup mock transformer
    val mockTransformer = new MockTransformer()
    val consumer = new StreamingConsumer(mockTransformer)
    
    // Test event
    val testEvent = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = Some(Map("category" -> "electronics"))
    )
    
    // Process event
    consumer.processEvent(testEvent)
    
    // Verify transformer was called with correct event
    mockTransformer.lastProcessedEvent shouldBe Some(testEvent)
  }
  
  "StreamingConsumer" should "handle exceptions from transformer gracefully" in {
    // Setup failing transformer
    val failingTransformer = new FailingTransformer()
    val consumer = new StreamingConsumer(failingTransformer)
    
    // Test event
    val testEvent = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = Some(Map.empty[String, String])
    )
    
    // This should not throw exception
    noException should be thrownBy {
      consumer.processEvent(testEvent)
    }
    
    // Verify transformer was called
    failingTransformer.processEventCalled shouldBe true
  }
  
  // Mock transformer that records events
  class MockTransformer extends Transformer(null) {
    var lastProcessedEvent: Option[UserEvent] = None
    
    override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
      lastProcessedEvent = Some(event)
      Future.successful(
        ProcessedEvent(
          id = Some(1L),
          userId = event.userId,
          eventType = event.eventType,
          itemId = event.itemId,
          properties = event.properties.getOrElse(Map.empty[String, String]),
          timestamp = System.currentTimeMillis(),
          processingTimestamp = System.currentTimeMillis()
        )
      )
    }
    
    // Don't use RabbitMQ in tests
    override protected def publishRecommendationRequest(request: models.RecommendationRequest): Unit = {}
    override def close(): Unit = {}
  }
  
  // Transformer that fails when processing events
  class FailingTransformer extends Transformer(null) {
    var processEventCalled = false
    
    override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
      processEventCalled = true
      Future.failed(new RuntimeException("Simulated failure"))
    }
    
    override protected def publishRecommendationRequest(request: models.RecommendationRequest): Unit = {}
    override def close(): Unit = {}
  }
}