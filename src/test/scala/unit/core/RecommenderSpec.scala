package unit.core

import core.Recommender
import models._
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
  
  it should "incorporate social context in recommendations" in {
    // Create mock repository with social context
    val mockRepository = new MockRepository(socialContextTest = true)
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 123L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify social-influenced recommendations
    recommendation.recommendedItems.size should be > 0
    // Event 103 should be in top results because friends are participating
    recommendation.recommendedItems.map(_.itemId) should contain(103L)
  }
  
  it should "incorporate geographic proximity in recommendations" in {
    // Create mock repository with location context
    val mockRepository = new MockRepository(locationContextTest = true)
    val recommender = new RecommenderUnderTest(mockRepository)
    
    // Test request
    val request = RecommendationRequest(
      userId = 123L,
      timestamp = System.currentTimeMillis()
    )
    
    // Generate recommendations
    val recommendationFuture = recommender.generateRecommendations(request)
    val recommendation = Await.result(recommendationFuture, 2.seconds)
    
    // Verify location-influenced recommendations
    recommendation.recommendedItems.size should be > 0
    // Event 104 should be in top results because it's nearby
    recommendation.recommendedItems.map(_.itemId) should contain(104L)
  }
  
  // Test version of Recommender that doesn't use RabbitMQ
  class RecommenderUnderTest(repository: Repository) extends Recommender(repository) {
    override def start(): Unit = {}
    override def close(): Unit = {}
  }
  
  // Mock repository for testing
  class MockRepository(
    emptyHistory: Boolean = false, 
    frequencyTest: Boolean = false,
    socialContextTest: Boolean = false,
    locationContextTest: Boolean = false
  ) extends Repository {
    var storedRecommendation: Option[Recommendation] = None
    private val now = System.currentTimeMillis()
    
    override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
      if (emptyHistory) {
        Future.successful(List.empty)
      } else if (frequencyTest) {
        // More interactions with item 102 than 101
        Future.successful(List(
          ProcessedEvent(Some(1L), userId, "view", Some(101L), Map("category" -> "tech"), now - 10000),
          ProcessedEvent(Some(2L), userId, "click", Some(102L), Map("category" -> "music"), now - 9000),
          ProcessedEvent(Some(3L), userId, "view", Some(102L), Map("category" -> "music"), now - 8000),
          ProcessedEvent(Some(4L), userId, "view", Some(102L), Map("category" -> "music"), now - 7000)
        ))
      } else {
        // Normal test case with two different items
        Future.successful(List(
          ProcessedEvent(Some(1L), userId, "view", Some(101L), Map("category" -> "tech"), now - 10000),
          ProcessedEvent(Some(2L), userId, "purchase", Some(101L), Map("category" -> "tech"), now - 5000),
          ProcessedEvent(Some(3L), userId, "view", Some(102L), Map("category" -> "music"), now - 2000)
        ))
      }
    }
    
    override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
      storedRecommendation = Some(recommendation)
      Future.successful(recommendation)
    }
    
    override def getUserProfile(userId: Long): Future[Option[User]] = {
      if (locationContextTest) {
        Future.successful(Some(User(
          userId = userId,
          name = Some("Test User"),
          emailAddress = Some("test@example.com"),
          locale = "en_US",
          birthYear = Some(1990),
          gender = Some("M"),
          location = Some("New York"),
          timezone = Some(0),
          isGenerated = false
        )))
      } else {
        Future.successful(None)
      }
    }
    
    override def getUserFriends(userId: Long): Future[List[UserFriend]] = {
      if (socialContextTest) {
        Future.successful(List(
          UserFriend(userId, 456L, 1.0),
          UserFriend(userId, 789L, 0.8)
        ))
      } else {
        Future.successful(List.empty)
      }
    }
    
    override def getAllActiveEvents(): Future[List[Event]] = {
      val events = List(
        Event(
          id = Some(1L),
          name = "Tech Conference",
          description = Some("A conference about technology"),
          startTime = System.currentTimeMillis() + 86400000,
          endTime = Some(System.currentTimeMillis() + (2 * 86400000)),
          location = Some("Convention Center"),
          createdBy = 1L,
          status = "active",
          keywords = List("tech", "conference"),
          keywordFrequencies = Map("tech" -> 5, "conference" -> 3),
          participants = Set(1L, 2L, 3L)
        ),
        Event(
          id = Some(102L),
          name = "Music Festival",
          description = Some("A large music festival with multiple stages"),
          startTime = now + 86400000L,
          endTime = Some(now + (2 * 86400000L)),
          location = Some("Central Park"),
          createdBy = 2L,
          status = "active",
          keywords = List("music", "festival"),
          keywordFrequencies = Map("music" -> 8, "festival" -> 5),
          participants = Set(2L, 3L, 4L)
        ),
        Event(
          id = Some(103L),
          name = "Social Gathering",
          description = Some("A networking event for professionals"),
          startTime = now + 259200000L,
          endTime = Some(now + (3 * 86400000L)),
          location = Some("Nice, France"),
          createdBy = 3L,
          status = "active",
          keywords = List("social", "networking"),
          keywordFrequencies = Map("social" -> 4, "networking" -> 3),
          participants = Set(456L, 789L)
        ),
        Event(
          id = Some(104L),
          name = "Local Exhibition",
          description = Some("An exhibition showcasing local art"),
          startTime = now + 86400000L,
          endTime = Some(now + (2 * 86400000L)),
          location = Some("Paris, France"),
          createdBy = 4L,
          status = "active",
          keywords = List("art", "exhibition"),
          keywordFrequencies = Map("art" -> 6, "exhibition" -> 4),
          participants = Set(555L, 666L)
        )
      )
      Future.successful(events)
    }
    
    override def initTables(): Future[Unit] = Future.successful(())
    override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = Future.successful(None)
    override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = Future.successful(None)
    override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = Future.successful(event)
  }
}