package unit.core

import core.Transformer
import models.{UserEvent, ProcessedEvent, RecommendationRequest}
import repositories.Repository
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import config.RabbitMQManager

class TransformerSpec extends AnyFlatSpec with Matchers {

  "Transformer" should "convert UserEvent to ProcessedEvent correctly" in {
    // Create mocks
    val mockRepository = new MockRepository()
    val transformer = new TransformerUnderTest(mockRepository)
    
    // Test data
    val userId = 123L
    val itemId = 456L
    val eventType = "purchase"
    val properties = Map("price" -> "99.99", "currency" -> "USD")
    
    val event = UserEvent(
      userId = userId,
      eventType = eventType,
      itemId = Some(itemId),
      properties = Some(properties)
    )
    
    // Process the event
    val processedEventFuture = transformer.processEvent(event)
    val processedEvent = Await.result(processedEventFuture, 2.seconds)
    
    // Verify conversion
    processedEvent.userId shouldBe userId
    processedEvent.eventType shouldBe eventType
    processedEvent.itemId shouldBe Some(itemId)
    processedEvent.properties shouldBe properties
  }
  
  it should "store processed events in the repository" in {
    // Create mocks
    val mockRepository = new MockRepository()
    val transformer = new TransformerUnderTest(mockRepository)
    
    // Test data
    val event = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = None
    )
    
    // Process the event
    Await.result(transformer.processEvent(event), 2.seconds)
    
    // Verify repository was called
    mockRepository.storedEvent.isDefined shouldBe true
    mockRepository.storedEvent.get.userId shouldBe event.userId
  }
  
  it should "trigger recommendation requests for events with item IDs" in {
    // Create mocks
    val mockRepository = new MockRepository()
    val transformer = new TransformerUnderTest(mockRepository)
    
    // Test data with item ID
    val eventWithItem = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = None
    )
    
    // Process event with item
    Await.result(transformer.processEvent(eventWithItem), 2.seconds)
    
    // Verify recommendation was requested
    transformer.recommendationRequests.size shouldBe 1
    transformer.recommendationRequests.head.userId shouldBe eventWithItem.userId
  }
  
  it should "not trigger recommendation requests for events without item IDs" in {
    // Create mocks
    val mockRepository = new MockRepository()
    val transformer = new TransformerUnderTest(mockRepository)
    
    // Test data without item ID
    val eventWithoutItem = UserEvent(
      userId = 123L,
      eventType = "login",
      itemId = None,
      properties = Some(Map("device" -> "mobile"))
    )
    
    // Process event without item
    Await.result(transformer.processEvent(eventWithoutItem), 2.seconds)
    
    // Verify no recommendation was requested
    transformer.recommendationRequests shouldBe empty
  }
  
  // Test version of Transformer that captures recommendation requests
  class TransformerUnderTest(repository: Repository) extends Transformer(repository) {
    var recommendationRequests: List[RecommendationRequest] = List.empty
    
    override def publishRecommendationRequest(request: RecommendationRequest): Unit = {
      recommendationRequests = request :: recommendationRequests
    }
    
    // Don't actually create RabbitMQ channels
    override def close(): Unit = {}
  }
  
  // Mock repository for testing
  class MockRepository extends Repository {
    var storedEvent: Option[ProcessedEvent] = None
    
    override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
      storedEvent = Some(event)
      Future.successful(event.copy(id = Some(1L)))
    }
    
    override def initTables(): Future[Unit] = Future.successful(())
  }
}