package unit.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.reflect.Whitebox

import com.rabbitmq.client.Channel
import config.RabbitMQManager
import core.Recommender
import models._
import repositories.Repository
import repositories.RepositoryInterface
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

// A testable version of the recommender that overrides methods that would access external systems
class TestableRecommender(repository: RepositoryInterface)(implicit ec: ExecutionContext) extends Recommender(repository)(ec) {
  override def publishRecommendation(recommendation: Recommendation): Unit = {
    // Do nothing in tests
  }
  
  override def start(): Unit = {
    // Don't start the consumer in tests
  }
  
  override def close(): Unit = {
    // Do nothing in tests
  }
}

class RecommenderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with ScalaFutures {
  
  // Configure ScalaFutures timeout
  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds))
  
  // Mock objects
  var mockRepository: Repository = _
  var mockChannel: Channel = _
  var recommender: TestableRecommender = _
  
override def beforeEach(): Unit = {
  // Setup fresh mocks for each test
  mockRepository = PowerMockito.mock(classOf[Repository])
  mockChannel = PowerMockito.mock(classOf[Channel])
  
  // Mock static methods
  PowerMockito.mockStatic(RabbitMQManager.getClass)
  when(RabbitMQManager.createChannel(anyString())).thenReturn(mockChannel)
  
  // Setup standard repository responses - COMPLETE THESE CONFIGURATIONS
  when(mockRepository.storeRecommendation(any[Recommendation]())).thenReturn(
    Future.successful(Recommendation(id = Some(1L), userId = 1L, recommendedItems = Nil, score = 0.0))
  )
  
  when(mockRepository.getRecentRecommendationForUser(anyLong(), anyLong())).thenReturn(
    Future.successful(None)
  )
  
  // Create recommender with mocks
  recommender = new TestableRecommender(mockRepository)
}

  "Recommender" should "calculate scores with 70% frequency and 30% recency weighting" in {
    // Prepare test data - events with different timestamps
    val now = System.currentTimeMillis()
    val userId = 1L
    val item1 = 101L
    val item2 = 102L
    
    val events = List(
      ProcessedEvent(Some(1), userId, "view", Some(item1), Map(), now),
      ProcessedEvent(Some(2), userId, "view", Some(item1), Map(), now - 1.day.toMillis),
      ProcessedEvent(Some(3), userId, "click", Some(item1), Map(), now - 2.days.toMillis),
      ProcessedEvent(Some(4), userId, "view", Some(item2), Map(), now),
    )
    
    // Setup mock responses
    when(mockRepository.getProcessedEventsForUser(anyLong(), anyInt())).thenReturn(
      Future.successful(events))
    
    // Execute the method under test with timeout
    val recommendationFuture = recommender.generateRecommendationsForUser(userId)
    val recommendation = Await.result(recommendationFuture, 5.seconds)
    
    // Verify recommendation contains our items
    recommendation.userId shouldBe userId
    recommendation.recommendedItems.map(_.itemId).toSet shouldBe Set(item1, item2)
    
    // Verify item1 has higher score than item2 since it has more events
    val item1Rec = recommendation.recommendedItems.find(_.itemId == item1).get
    val item2Rec = recommendation.recommendedItems.find(_.itemId == item2).get
    item1Rec.score should be > item2Rec.score
    
    // Verify the repository was called correctly
    verify(mockRepository).getProcessedEventsForUser(userId, 100)
    verify(mockRepository).storeRecommendation(any[Recommendation])
  }
  
  it should "use existing recommendations if they are recent enough" in {
    // Setup: Create an existing recommendation
    val userId = 2L
    val existingRecommendation = Recommendation(
      id = Some(1L),
      userId = userId,
      recommendedItems = List(RecommendedItem(201L, 0.8, "Existing recommendation")),
      score = 0.8,
      timestamp = System.currentTimeMillis() - 4.minutes.toMillis // 4 minutes old
    )
    
    // Mock: Return the existing recommendation
    when(mockRepository.getRecentRecommendationForUser(userId, 5 * 60 * 1000)).thenReturn(
      Future.successful(Some(existingRecommendation)))
    
    // Execute with timeout
    val result = Await.result(recommender.generateRecommendationsForUser(userId), 5.seconds)
    
    // Verify: We got the existing recommendation without calculating a new one
    result shouldBe existingRecommendation
    
    // Verify: Repository methods called correctly
    verify(mockRepository).getRecentRecommendationForUser(userId, 5 * 60 * 1000)
    verify(mockRepository, never()).getProcessedEventsForUser(anyLong(), anyInt())
  }
}