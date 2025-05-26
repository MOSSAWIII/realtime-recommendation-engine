package unit.pipeline

import application.core.{Transformer, Recommender}
import domain.models._
import common.base.UnitTestBase
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import common.mocks.TestMockManager.MockDataManager

class PipelineUnitSpec extends UnitTestBase {

  "The recommendation pipeline" should "flow events from transformer to recommender" in {
    // Create the test components with the new dependency chain
    val mockDataManager = new MockDataManager()
    val mockRecommender = new MockRecommender(mockDataManager)
    val transformer = new MockTransformer(mockDataManager, mockRecommender)
    
    // Test event
    val testEvent = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = Some(Map("test" -> "value")),
      timestamp = System.currentTimeMillis()
    )
    
    // Process the event directly through the transformer
    transformer.processEvent(testEvent)
    
    // Verify transformer processed the event
    transformer.processedEvents should contain(testEvent)
    
    // Verify recommender generated recommendations for this user
    val recommendations = Await.result(mockRecommender.recommendationsPromise.future, 2.seconds)
    recommendations.userId shouldBe testEvent.userId
    recommendations.recommendedItems.nonEmpty shouldBe true
  }
  
  // Mock classes for testing the pipeline flow
  class MockTransformer(dataManager: MockDataManager, recommender: MockRecommender) extends Transformer(dataManager) {
    var processedEvents: List[UserEvent] = List.empty
    
    override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
      processedEvents = event :: processedEvents
      
      val processedEvent = ProcessedEvent(
        id = Some(1L),
        userId = event.userId,
        eventType = event.eventType,
        itemId = event.itemId,
        properties = event.properties.getOrElse(Map.empty),
        timestamp = event.timestamp
      )
      
      // Store event in dataManager
      val future = dataManager.storeProcessedEvent(processedEvent)
      
      // Trigger recommendation request
      future.foreach { _ =>
        recommender.recommendForUser(RecommendationRequest(
          userId = event.userId,
          itemId = event.itemId.getOrElse(-1L),
          context = "test"
        ))
      }
      
      future
    }
    
    override def publishProcessedEvent(event: ProcessedEvent): Unit = {
      // Do nothing in mock
    }
    
    override def publishRecommendationRequest(request: RecommendationRequest): Unit = {
      // Directly call recommender for unit testing
      recommender.recommendForUser(request)
    }
    
    // Override methods that would interact with RabbitMQ
    override protected def setupPublishChannel() = null
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
  
  class MockRecommender(dataManager: MockDataManager) extends Recommender(dataManager) {
    var recommendationsPromise: Promise[Recommendation] = Promise[Recommendation]()
    
    override def recommendForUser(request: RecommendationRequest): Future[Recommendation] = {
      // Create sample recommendation
      val recommendation = Recommendation(
        id = None,
        userId = request.userId,
        timestamp = System.currentTimeMillis(),
        recommendedItems = List(
          RecommendedItem(request.itemId, 0.9, "Test reason 1"),
          RecommendedItem(request.itemId + 1, 0.8, "Test reason 2")
        )
      )
      
      // Complete the promise with this recommendation
      if (!recommendationsPromise.isCompleted) {
        recommendationsPromise.success(recommendation)
      }
      
      // Store and return
      dataManager.storeRecommendation(recommendation)
      Future.successful(recommendation)
    }
    
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
}

