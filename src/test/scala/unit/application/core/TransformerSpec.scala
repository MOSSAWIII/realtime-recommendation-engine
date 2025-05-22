package unit.application.core

import core.{Transformer, DataManager}
import models.{UserEvent, ProcessedEvent, RecommendationRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import common.utils.RabbitMQManager
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentCaptor

class TransformerSpec extends AnyFlatSpec with Matchers {

  "Transformer" should "convert UserEvent to ProcessedEvent correctly" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data
    val userId = 123L
    val itemId = 456L
    val eventType = "purchase"
    val properties = Map("price" -> "99.99", "currency" -> "USD")
    
    val event = UserEvent(
      userId = userId,
      eventType = eventType,
      itemId = Some(itemId),
      properties = Some(properties),
      timestamp = System.currentTimeMillis()
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
  
  it should "trigger recommendation requests for events with item IDs" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data with item ID
    val event = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = None,
      timestamp = System.currentTimeMillis()
    )
    
    // Process the event
    Await.result(transformer.processEvent(event), 2.seconds)
    
    // Verify recommendation request was triggered
    transformer.recommendationRequests should have size 1
    transformer.recommendationRequests.head.userId shouldBe event.userId
    transformer.recommendationRequests.head.itemId shouldBe event.itemId.get
  }
  
  it should "not trigger recommendation requests for events without item IDs" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data without item ID
    val event = UserEvent(
      userId = 123L,
      eventType = "login",
      itemId = None,
      properties = None,
      timestamp = System.currentTimeMillis()
    )
    
    // Process the event
    Await.result(transformer.processEvent(event), 2.seconds)
    
    // Verify no recommendation request was triggered
    transformer.recommendationRequests shouldBe empty
  }
  
  it should "track metrics correctly" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    transformer.resetMetrics() // Start with clean metrics
    
    // Process multiple events
    val events = (1 to 5).map { i =>
      UserEvent(
        userId = 100L + i,
        eventType = "view",
        itemId = Some(200L + i),
        properties = None,
        timestamp = System.currentTimeMillis()
      )
    }
    
    events.foreach(event => Await.result(transformer.processEvent(event), 2.seconds))
    
    // Get metrics output and verify it contains expected data
    val metrics = transformer.getMetrics
    metrics should include("Total events processed: 5")
    metrics should include("Average processing time:")
    metrics should include("Min processing time:")
    metrics should include("Max processing time:")
  }
  
  // Tests de traitement des événements
  it should "consommer et traiter correctement les messages" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data
    val testEvent = UserEvent(
      userId = 123L,
      eventType = "purchase",
      itemId = Some(456L),
      properties = None,
      timestamp = System.currentTimeMillis()
    )
    
    // Process the event
    Await.result(transformer.processEvent(testEvent), 2.seconds)
    
    // Verify that the event was processed
    verify(mockDataManager, times(1)).storeProcessedEvent(any[ProcessedEvent])
    
    // Verify the content of the processed event
    val captor = ArgumentCaptor.forClass(classOf[ProcessedEvent])
    verify(mockDataManager).storeProcessedEvent(captor.capture())
    val capturedEvent = captor.getValue
    
    // Assertions on the event
    capturedEvent.userId shouldBe testEvent.userId
    capturedEvent.eventType shouldBe testEvent.eventType
  }
  
  // Tests pour la gestion des canaux
  it should "gérer correctement les confirmations de publication" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Use a spy to verify internal calls
    val transformer = spy(new TransformerUnderTest(mockDataManager))
    doReturn(null).when(transformer).setupPublishChannel()
    
    // Test data
    val event = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = None,
      timestamp = System.currentTimeMillis()
    )
    
    // Execute test
    transformer.processEvent(event).map { _ =>
      // clearOutstandingConfirms is a method exposed for testing
      transformer.getOutstandingConfirmsSize shouldBe 0
    }
  }
  
  // Test version of Transformer that captures recommendation requests instead of publishing
  class TransformerUnderTest(dataManager: DataManager = null) extends Transformer(dataManager) {
    var recommendationRequests: List[RecommendationRequest] = List.empty
    
    override def publishRecommendationRequest(request: RecommendationRequest): Unit = {
      recommendationRequests = request :: recommendationRequests
    }
    
    // Override channel setup to avoid actual RabbitMQ operations
    override protected def setupPublishChannel() = {
      // Return null instead of establishing actual connection
      null
    }
    
    // Override publishProcessedEvent to avoid RabbitMQ operations
    override def publishProcessedEvent(event: ProcessedEvent): Unit = {
      // Do nothing in tests - don't publish to RabbitMQ
    }
    
    // Override start to avoid actual RabbitMQ operations
    override def start(): Unit = {
      // Do nothing in tests - don't set up consumer channel
    }
    
    // Override close to avoid closing non-existent connections
    override def close(): Unit = {
      // Do nothing in tests - no channels to close
    }
  }
}