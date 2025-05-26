package integration.db

import domain.models._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import common.base.IntegrationTestBase
import common.mocks.TestMockManager.MockDataManager
import common.fixtures.TestDataFactory

class DataManagerIntegrationSpec extends IntegrationTestBase {
  
  var dataManager: MockDataManager = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    // Initialize a new MockDataManager for each test
    dataManager = new MockDataManager()
    
    // Configure test data using TestDataFactory
    val testUser = TestDataFactory.createUser(userId = 1L)
    val testEvent = TestDataFactory.createEvent(id = Some(101L), createdBy = 1L)
    val testProcessedEvent = TestDataFactory.createProcessedEvent(
      id = Some(1L), userId = 1L, itemId = Some(101L)
    )
    val testFriendship = TestDataFactory.createUserFriend(userId = 1L, friendId = 2L)
    
    // Setup test data
    dataManager.setupMockData(
      users = List(testUser),
      events = List(testEvent),
      processedEvents = List(testProcessedEvent),
      friendships = List(testFriendship)
    )
  }
  
  "DataManager" should "retrieve stored events for a user" in {
    val events = Await.result(dataManager.getProcessedEventsForUser(1L), 5.seconds)
    events should have size 1
    events.head.userId shouldBe 1L
    events.head.itemId shouldBe Some(101L)
  }
  
  it should "store and retrieve recommendations" in {
    val recommendation = TestDataFactory.createRecommendation(userId = 1L)
    
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
    super.afterEach()
  }
}

