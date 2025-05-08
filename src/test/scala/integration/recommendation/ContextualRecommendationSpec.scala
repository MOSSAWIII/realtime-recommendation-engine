package integration.recommendation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import models.{ProcessedEvent, User, UserFriend, Event, Recommendation, RecommendationRequest}
import core.Recommender
import repositories.Repository
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.slf4j.LoggerFactory

class ContextualRecommendationSpec extends AnyFlatSpec with Matchers {
  private val logger = LoggerFactory.getLogger(getClass)

  "Contextual recommender" should "recommend events with participating friends" in {
    // Setup
    val userId = 1000L
    val now = System.currentTimeMillis()
    val friendId1 = 2000L
    val friendId2 = 3000L
    
    // Create test repository with mock data
    val testRepository = new TestRepository
    
    // Setup processed events data
    testRepository.mockProcessedEvents = List(
      ProcessedEvent(Some(1), userId, "view", Some(5000L), Map(), now - 1000),
      ProcessedEvent(Some(2), userId, "view", Some(6000L), Map(), now - 2000)
    )
    
    // Setup user profile
    testRepository.mockUserProfile = Some(User(
      userId = userId,
      name = Some("Test User"),
      emailAddress = Some("test@example.com"),
      locale = "en_US",
      birthYear = Some(1990),
      gender = Some("M"),
      location = Some("Paris, France"),
      timezone = Some(1),
      isGenerated = false
    ))
    
    // Setup user friends - using strength not affinity
    testRepository.mockUserFriends = List(
      UserFriend(userId, friendId1, 0.9),
      UserFriend(userId, friendId2, 0.8)
    )

    // Setup test events - one with friends participating, one without
    testRepository.mockEvents = List(
      Event(
        id = Some(5000L),
        name = "Event with Friends",
        description = Some("An event where friends participate"),
        startTime = now + 86400000,
        endTime = Some(now + (2 * 86400000)),
        location = Some("Paris, France"),
        createdBy = 999L,
        status = "active",
        keywords = List("social", "friends"),
        keywordFrequencies = Map("social" -> 3, "friends" -> 5),
        participants = Set(friendId1, 456L) // Include one friend
      ),
      Event(
        id = Some(6000L),
        name = "Event without Friends",
        description = Some("An event where no friends participate"),
        startTime = now + 86400000,
        endTime = Some(now + (2 * 86400000)),
        location = Some("Paris, France"),
        createdBy = 888L,
        status = "active",
        keywords = List("tech", "workshop"),
        keywordFrequencies = Map("tech" -> 4, "workshop" -> 3),
        participants = Set(777L, 888L) // No friends participating
      )
    )

    // Create recommender and get recommendations
    val recommender = new Recommender(testRepository)
    val request = RecommendationRequest(userId)
    val recommendations = Await.result(recommender.generateRecommendations(request), 5.seconds)
    
    // Log the details for debugging
    logger.info(s"Event with friends (5000): ${recommendations.recommendedItems.find(_.itemId == 5000L).map(_.score).getOrElse("Not found")}")
    logger.info(s"Event without friends (6000): ${recommendations.recommendedItems.find(_.itemId == 6000L).map(_.score).getOrElse("Not found")}")
    
    // The event with friends should be scored higher
    val withFriendsScore = recommendations.recommendedItems.find(_.itemId == 5000L).map(_.score).getOrElse(0.0)
    val withoutFriendsScore = recommendations.recommendedItems.find(_.itemId == 6000L).map(_.score).getOrElse(0.0)
    
    withFriendsScore should be > withoutFriendsScore
    
    // Check the reason mentions social connections
    val withFriendsReason = recommendations.recommendedItems.find(_.itemId == 5000L).map(_.reason).getOrElse("")
    withFriendsReason should include("social connections")
  }
}

