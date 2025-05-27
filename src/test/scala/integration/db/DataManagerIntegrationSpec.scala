package integration.db

import domain.models._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import common.base.DatabaseTestBase
import common.mocks.MockDataManager
import common.fixtures.TestDataFactory
import common.utils.TestResourceManager

class DataManagerIntegrationSpec extends DatabaseTestBase {
  
  override protected def dbProvider: infrastructure.config.DatabaseConfigProvider = 
    new common.mocks.TestDatabaseProvider()
  
  var dataManager: MockDataManager = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    // Initialize a new MockDataManager for each test with unique IDs
    dataManager = new MockDataManager()
    
    // Configure test data using TestDataFactory with unique IDs
    val testUser = TestDataFactory.createUser(id = uniqueId())
    val testEvent = TestDataFactory.createEvent(id = Some(uniqueId()))
    val testProcessedEvent = TestDataFactory.createProcessedEvent(
      id = Some(uniqueId()), userId = testUser.userId, itemId = testEvent.id
    )
    val testFriendship = TestDataFactory.createUserFriend(userId = testUser.userId, friendId = uniqueId())
    
    // Setup test data
    dataManager.setupMockData(
      users = List(testUser),
      events = List(testEvent),
      processedEvents = List(testProcessedEvent),
      friendships = List(testFriendship)
    )
    
    // Vérification de sécurité - Utiliser la méthode publique getUsersList
    require(dataManager.getUsersList.nonEmpty, "Test setup failed: No users created")
  }
  
  "DataManager" should "retrieve stored events for a user" in {
    // Utiliser la méthode publique getUsersList
    val userId = dataManager.getUsersList.headOption.map(_.userId).getOrElse(
      throw new IllegalStateException("No test users created")
    )
    val events = Await.result(dataManager.getProcessedEventsForUser(userId), 5.seconds)
    events should have size 1
    events.head.userId shouldBe userId
  }
  
  override def afterEach(): Unit = {
    if (dataManager != null) {
      dataManager.close()
    }
    super.afterEach()
  }
  
  // Helper pour générer des ID uniques pour ce test
  private def uniqueId(): Long = TestResourceManager.uniqueId()
}

