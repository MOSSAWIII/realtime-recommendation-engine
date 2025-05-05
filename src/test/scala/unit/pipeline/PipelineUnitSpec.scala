package unit.pipeline

import core.{StreamingConsumer, Transformer, Recommender}
import models._
import repositories.Repository
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PipelineUnitSpec extends AnyFlatSpec with Matchers {

  "The recommendation pipeline" should "flow events from consumer through transformer to recommender" in {
    // Create the test components
    val mockRepository = new MockRepository()
    val mockRecommender = new MockRecommender(mockRepository)
    val transformer = new MockTransformer(mockRepository, mockRecommender)
    val consumer = new StreamingConsumer(transformer)
    
    // Test event
    val testEvent = UserEvent(
      userId = 123L,
      eventType = "view",
      itemId = Some(456L),
      properties = Some(Map("test" -> "value"))
    )
    
    // Process the event through the consumer
    consumer.processEvent(testEvent)
    
    // Verify transformer processed the event
    transformer.processedEvents should contain(testEvent)
    
    // Verify recommender generated recommendations for this user
    val recommendations = Await.result(mockRecommender.recommendationsPromise.future, 2.seconds)
    recommendations.userId shouldBe testEvent.userId
    recommendations.recommendedItems.nonEmpty shouldBe true
  }
  
  // Mock classes for testing the pipeline flow
  class MockTransformer(repository: Repository, recommender: MockRecommender) extends Transformer(repository) {
    var processedEvents: List[UserEvent] = List.empty
    
    override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
      processedEvents = event :: processedEvents
      
      val processedEvent = ProcessedEvent(
        id = Some(1L),
        userId = event.userId,
        eventType = event.eventType,
        itemId = event.itemId,
        properties = event.properties.getOrElse(Map.empty),
        timestamp = System.currentTimeMillis()
      )
      
      // Store event in repository
      val future = repository.storeProcessedEvent(processedEvent)
      
      // Trigger recommendation request
      future.foreach { _ =>
        if (event.itemId.isDefined) {
          val request = RecommendationRequest(
            userId = event.userId,
            timestamp = System.currentTimeMillis()
          )
          
          // Generate recommendation directly instead of publishing message
          recommender.generateRecommendations(request)
        }
      }
      
      future
    }
    
    override def publishRecommendationRequest(request: RecommendationRequest): Unit = {}
    override def close(): Unit = {}
  }
  
  class MockRecommender(repository: Repository) extends Recommender(repository) {
    val recommendationsPromise = Promise[Recommendation]()
    
    override def generateRecommendations(request: RecommendationRequest): Future[Recommendation] = {
      val recommendation = Recommendation(
        userId = request.userId,
        recommendedItems = List(
          RecommendedItem(itemId = 101, score = 0.9, reason = "Test recommendation"),
          RecommendedItem(itemId = 102, score = 0.8, reason = "Test recommendation"),
          RecommendedItem(itemId = 103, score = 0.7, reason = "Test recommendation")
        ),
        score = 0.8,
        timestamp = System.currentTimeMillis()
      )
      
      // Complete the promise with this recommendation
      if (!recommendationsPromise.isCompleted) {
        recommendationsPromise.success(recommendation)
      }
      
      // Store and return
      repository.storeRecommendation(recommendation)
      Future.successful(recommendation)
    }
    
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
  
  class MockRepository extends Repository {
    var events: List[ProcessedEvent] = List.empty
    var recommendations: List[Recommendation] = List.empty
    
    override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
      events = event :: events
      Future.successful(event.copy(id = Some(1L)))
    }
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
      Future.successful(events.filter(_.userId == userId).take(limit))
    }
    
    override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
      recommendations = recommendation :: recommendations
      Future.successful(recommendation)
    }
    
    override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
      Future.successful(recommendations.find(_.userId == userId))
    }
    
    override def initTables(): Future[Unit] = Future.successful(())
  }
}

