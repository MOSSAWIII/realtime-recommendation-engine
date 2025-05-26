package unit.application.core

import application.core.{Transformer, DataManager}
import domain.models.{UserEvent, ProcessedEvent, RecommendationRequest}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentCaptor
import org.powermock.api.mockito.PowerMockito
import common.base.UnitTestBase
import common.fixtures.TestDataFactory

class TransformerSpec extends UnitTestBase {

  "Transformer" should "convert UserEvent to ProcessedEvent correctly" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Use TestDataFactory to create test data
    val event = TestDataFactory.createUserEvent()
    
    // Process the event
    val processedEventFuture = transformer.processEvent(event)
    val processedEvent = Await.result(processedEventFuture, 2.seconds)
    
    // Verify conversion
    processedEvent.userId shouldBe event.userId
    processedEvent.eventType shouldBe event.eventType
    processedEvent.itemId shouldBe event.itemId
    processedEvent.properties shouldBe event.properties.getOrElse(Map.empty)
  }
  
  it should "trigger recommendation requests for events with item IDs" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data with item ID
    val event = TestDataFactory.createUserEventWithItemId()
    
    // Process the event
    Await.result(transformer.processEvent(event), 2.seconds)
    
    // Verify recommendation request was triggered
    transformer.recommendationRequests should have size 1
    transformer.recommendationRequests.head.userId shouldBe event.userId
  }
  
  it should "not trigger recommendation requests for events without item IDs" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data without item ID
    val event = TestDataFactory.createUserEventWithoutItemId()
    
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
    val events = TestDataFactory.createMultipleUserEvents(5)
    
    events.foreach(event => Await.result(transformer.processEvent(event), 2.seconds))
    
    // Get metrics output and verify it contains expected data
    val metrics = transformer.getMetrics
    metrics should include("Total events processed: 5")
    metrics should include("Average processing time:")
    metrics should include("Min processing time:")
    metrics should include("Max processing time:")
  }
  
  it should "consommer et traiter correctement les messages" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Create transformer with mock DataManager
    val transformer = new TransformerUnderTest(mockDataManager)
    
    // Test data
    val testEvent = TestDataFactory.createUserEvent()
    
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
  
  it should "gérer correctement les confirmations de publication" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Use a spy to verify internal calls
    val transformer = spy(new TransformerUnderTest(mockDataManager))
    PowerMockito.doReturn(null).when(transformer, "setupPublishChannel")
    
    // Test data
    val event = TestDataFactory.createUserEventWithItemId()
    
    // Execute test
    transformer.processEvent(event).map { _ =>
      transformer.getOutstandingConfirmsSize shouldBe 0
    }
  }
  
  class TransformerUnderTest(dataManager: DataManager = null) extends Transformer(dataManager) {
    var recommendationRequests: List[RecommendationRequest] = List.empty
    
    override def publishRecommendationRequest(request: RecommendationRequest): Unit = {
      recommendationRequests = request :: recommendationRequests
    }
    
    override protected def setupPublishChannel() = null
    
    override def publishProcessedEvent(event: ProcessedEvent): Unit = {
      // Do nothing in tests - don't publish to RabbitMQ
    }
    
    override def start(): Unit = {
      // Do nothing in tests - don't set up consumer channel
    }
    
    override def close(): Unit = {
      // Do nothing in tests - no channels to close
    }
  }
}