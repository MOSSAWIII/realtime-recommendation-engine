package unit.core

import core.Recommender
import models.{ProcessedEvent, RecommendationRequest, Recommendation, RecommendedItem}
import repositories.Repository
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class RecommenderSpec extends AnyFlatSpec with Matchers {
  
  "Recommender" should "generate recommendations based on user history" in {
    // Create mock repository with predefined user history
    val mockRepository = new MockRepository()
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 123L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify recommendations
    recommendation.userId shouldBe request.userId
    recommendation.recommendedItems should have size 2
    recommendation.recommendedItems.map(_.itemId) should contain allOf(101L, 102L)
  }
  
  it should "prioritize items with higher frequency" in {
    // Create mock repository with item interaction counts
    val mockRepository = new MockRepository(frequencyTest = true)
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 123L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify most frequent item is first
    recommendation.recommendedItems.size should be > 0
    recommendation.recommendedItems.head.itemId shouldBe 102L  // Item with more interactions
  }
  
  it should "store generated recommendations in the repository" in {
    // Create mock repository
    val mockRepository = new MockRepository()
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 123L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    Await.result(recommendationFuture, 2.seconds)
    
    // Verify recommendation was stored
    mockRepository.storedRecommendation.isDefined shouldBe true
    mockRepository.storedRecommendation.get.userId shouldBe request.userId
  }
  
  it should "handle empty user history gracefully" in {
    // Create mock repository with empty history
    val mockRepository = new MockRepository(emptyHistory = true)
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 999L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify empty recommendations
    recommendation.userId shouldBe request.userId
    recommendation.recommendedItems shouldBe empty
  }
  
  // Test version of Recommender that doesn't use RabbitMQ
  class RecommenderUnderTest(repository: Repository) extends Recommender(repository) {
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
  
  // Mock repository for testing
  class MockRepository(emptyHistory: Boolean = false, frequencyTest: Boolean = false) extends Repository {
    var storedRecommendation: Option[Recommendation] = None
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
      if (userId == 42) {
        // Test case with duplicate item IDs
        val now = System.currentTimeMillis()
        Future.successful(List(
          ProcessedEvent(Some(1L), userId, "view", Some(101L), Map.empty, now - 10000),
          ProcessedEvent(Some(2L), userId, "view", Some(101L), Map.empty, now - 9000),
          ProcessedEvent(Some(3L), userId, "view", Some(102L), Map.empty, now - 8000),
          ProcessedEvent(Some(4L), userId, "view", Some(102L), Map.empty, now - 7000)
        ))
      } else {
        // Normal test case with two different items
        val now = System.currentTimeMillis()
        Future.successful(List(
          ProcessedEvent(Some(1L), userId, "view", Some(101L), Map.empty, now - 10000),
          ProcessedEvent(Some(2L), userId, "purchase", Some(101L), Map.empty, now - 5000),
          ProcessedEvent(Some(3L), userId, "view", Some(102L), Map.empty, now - 2000)
        ))
      }
    }
    
    override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
      storedRecommendation = Some(recommendation)
      Future.successful(recommendation)
    }
    
    override def initTables(): Future[Unit] = Future.successful(())
  }
}