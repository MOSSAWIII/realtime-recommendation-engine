package unit.application.core

import core.{Recommender, DataManager}
import models._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

class RecommenderSpec extends AnyFlatSpec with Matchers {
  
  "Optimized Recommender" should "generate recommendations based on user history" in {
    // Create mock DataManager
    val mockDataManager = mock(classOf[DataManager])
    
    // Setup mock responses
    val userId = 123L
    val now = System.currentTimeMillis()
    
    // Mock user events
    when(mockDataManager.getProcessedEventsForUser(anyLong(), anyInt())).thenReturn(
      Future.successful(List(
        ProcessedEvent(Some(1L), userId, "view", Some(101L), Map("category" -> "tech"), now - 10000),
        ProcessedEvent(Some(2L), userId, "purchase", Some(101L), Map("category" -> "tech"), now - 5000),
        ProcessedEvent(Some(3L), userId, "view", Some(102L), Map("category" -> "music"), now - 2000)
      ))
    )
    
    // Mock user profile
    when(mockDataManager.getUserProfile(anyLong())).thenReturn(
      Future.successful(Some(User(
        userId = userId,
        name = Some("Test User"),
        emailAddress = Some("test@example.com"),
        location = Some("New York, USA")
      )))
    )
    
    // Mock user friends
    when(mockDataManager.getUserFriends(anyLong())).thenReturn(
      Future.successful(List(
        UserFriend(userId, 456L, 1.0),
        UserFriend(userId, 789L, 0.8)
      ))
    )
    
    // Mock active events
    when(mockDataManager.getAllActiveEvents()).thenReturn(
      Future.successful(List(
        Event(
          id = Some(101L),
          name = "Tech Conference",
          description = Some("A conference about technology"),
          startTime = now + 86400000,
          location = Some("New York, USA"),
          createdBy = 1L,
          status = "active",
          participants = Set(456L) // Friend is participating
        ),
        Event(
          id = Some(102L),
          name = "Music Festival",
          description = Some("A music festival"),
          startTime = now + 172800000,
          location = Some("Boston, USA"),
          createdBy = 2L,
          status = "active",
          participants = Set.empty
        )
      ))
    )
    
    // Mock recommendation storage
    when(mockDataManager.storeRecommendation(any[Recommendation])).thenAnswer { invocation =>
      Future.successful(invocation.getArgument(0).asInstanceOf[Recommendation])
    }
    
    // Create recommender with mock data manager
    val recommender = new RecommenderUnderTest(mockDataManager)
    
    // Test request
    val request = RecommendationRequest(
      userId = userId,
      timestamp = now
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify recommendations
    recommendation.userId shouldBe userId
    recommendation.recommendedItems.size should be >= 1
    
    // Event 101 should be recommended with a high score (has friend participating and location match)
    val event101 = recommendation.recommendedItems.find(_.itemId == 101L)
    event101 shouldBe defined
    event101.get.score should be > 0.0
    
    // The reason should contain social connections
    event101.get.reason should include("social connections")
  }
  
  // Test class to avoid RabbitMQ operations
  class RecommenderUnderTest(dataManager: DataManager) extends Recommender(dataManager) {
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
}