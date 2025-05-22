package integration.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import common.utils.MockDataManager
import models._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class DataManagerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  
  implicit val ec: ExecutionContext = ExecutionContext.global
  var dataManager: MockDataManager = _
  
  override def beforeEach(): Unit = {
    // Initialiser un nouveau MockDataManager pour chaque test
    dataManager = new MockDataManager()
    
    // Configurer des données de test
    dataManager.setupMockData(
      users = List(
        User(userId = 1L, name = Some("User 1"), emailAddress = Some("user1@example.com"))
      ),
      events = List(
        Event(id = Some(101L), name = "Event 1", startTime = System.currentTimeMillis(), createdBy = 1L, status = "active")
      ),
      processedEvents = List(
        ProcessedEvent(id = Some(1L), userId = 1L, eventType = "view", itemId = Some(101L), 
                       properties = Map("test" -> "true"), timestamp = System.currentTimeMillis())
      ),
      friendships = List(
        UserFriend(userId = 1L, friendId = 2L, similarity = 0.8)
      )
    )
  }
  
  "DataManager" should "retrieve stored events for a user" in {
    val events = Await.result(dataManager.getProcessedEventsForUser(1L), 5.seconds)
    events should have size 1
    events.head.userId shouldBe 1L
    events.head.itemId shouldBe Some(101L)
  }
  
  it should "store and retrieve recommendations" in {
    val recommendation = Recommendation(
      id = None,
      userId = 1L,
      timestamp = System.currentTimeMillis(),
      recommendedItems = List(
        RecommendedItem(101L, 0.9, "Based on your recent activity")
      )
    )
    
    val storedRecommendation = Await.result(dataManager.storeRecommendation(recommendation), 5.seconds)
    storedRecommendation.id shouldBe defined
    
    val retrievedRecommendation = Await.result(dataManager.getRecommendationForUser(1L), 5.seconds)
    retrievedRecommendation shouldBe defined
    retrievedRecommendation.get.userId shouldBe 1L
    retrievedRecommendation.get.recommendedItems should have size 1
  }
  
  override def afterEach(): Unit = {
    if (dataManager != null) {
      dataManager.close()
    }
  }
}

