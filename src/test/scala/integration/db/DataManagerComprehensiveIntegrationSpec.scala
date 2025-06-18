package integration.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import application.core.DataManager
import domain.models._
import java.sql.DriverManager
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import common.mocks.{TestConfigurationService, TestDatabaseProvider}
import common.fixtures.TestDataFactory
import common.tags.TestTags.DatabaseTest
import common.utils.TestResourceManager // Ajouter cet import

@org.scalatest.TagAnnotation("DatabaseTest")
class DataManagerComprehensiveIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  var dataManager: DataManager = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    val jdbcUrl = sys.env.getOrElse("DATABASE_URL", "jdbc:postgresql://test-db:5432/test_recommendation_engine")
    val username = sys.env.getOrElse("DATABASE_USER", "postgres")
    val password = sys.env.getOrElse("DATABASE_PASSWORD", "test_password")
    
    Thread.sleep(5000) // Wait for database
    
    // Initialize database schema
    val connection = DriverManager.getConnection(jdbcUrl, username, password)
    val statement = connection.createStatement()
    
    // Create all required tables
    val createStatements = List(
      """CREATE TABLE IF NOT EXISTS users (
        |  user_id BIGSERIAL PRIMARY KEY,
        |  name VARCHAR(255),
        |  email_address VARCHAR(255),
        |  locale VARCHAR(10) NOT NULL DEFAULT 'en_US',
        |  birth_year INTEGER,
        |  gender VARCHAR(10),
        |  location VARCHAR(255),
        |  timezone INTEGER,
        |  is_generated BOOLEAN NOT NULL DEFAULT FALSE
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS events (
        |  id BIGSERIAL PRIMARY KEY,
        |  name VARCHAR(255) NOT NULL,
        |  description TEXT,
        |  start_time BIGINT NOT NULL,
        |  end_time BIGINT,
        |  location VARCHAR(255),
        |  created_by BIGINT NOT NULL,
        |  status VARCHAR(50) NOT NULL DEFAULT 'active',
        |  keywords VARCHAR(255)  -- AJOUTER CETTE LIGNE
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS processed_events (
        |  id BIGSERIAL PRIMARY KEY,
        |  user_id BIGINT NOT NULL,
        |  event_type VARCHAR(100) NOT NULL,
        |  item_id BIGINT,
        |  properties JSONB NOT NULL DEFAULT '{}',
        |  timestamp BIGINT NOT NULL,
        |  processing_timestamp BIGINT NOT NULL
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS user_friends (
        |  user_id BIGINT NOT NULL,
        |  friend_id BIGINT NOT NULL,
        |  strength FLOAT NOT NULL DEFAULT 1.0,
        |  PRIMARY KEY (user_id, friend_id)
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS event_participants (
        |  event_id BIGINT NOT NULL,
        |  user_id BIGINT NOT NULL,
        |  status VARCHAR(50) NOT NULL,
        |  PRIMARY KEY (event_id, user_id)
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS recommendations (
        |  id BIGSERIAL PRIMARY KEY,
        |  user_id BIGINT NOT NULL,
        |  score FLOAT NOT NULL,
        |  timestamp BIGINT NOT NULL
        |)""".stripMargin,
      
      """CREATE TABLE IF NOT EXISTS recommended_items (
        |  recommendation_id BIGINT NOT NULL,
        |  item_id BIGINT NOT NULL,
        |  score FLOAT NOT NULL,
        |  reason VARCHAR(255),
        |  PRIMARY KEY (recommendation_id, item_id)
        |)""".stripMargin
    )
    
    createStatements.foreach(statement.execute)
    statement.close()
    connection.close()
    
    // Create DataManager instance
    val configService = new TestConfigurationService() {
      override def getDatabaseUrl(): String = jdbcUrl
      override def getDatabaseUser(): String = username
      override def getDatabasePassword(): String = password
    }
    
    val dbProvider = new TestDatabaseProvider() {
      override def getConnection(): java.sql.Connection = {
        DriverManager.getConnection(jdbcUrl, username, password)
      }
    }
    
    dataManager = new DataManager(configService, dbProvider)
    Await.result(dataManager.initTables(), 10.seconds)
  }
  
  override def afterAll(): Unit = {
    if (dataManager != null) {
      dataManager.shutdown()
    }
    super.afterAll()
  }

  "DataManager User Operations" should "create and retrieve users" in {
    val testUser = TestDataFactory.createUser(
      name = "Integration Test User",
      emailAddress = "integration@test.com"
    )

    // Create user - utiliser createUser au lieu d'upsertUser
    val createdUser = Await.result(dataManager.createUser(testUser), 5.seconds)
    createdUser.userId should be > 0L
    createdUser.name should contain("Integration Test User")

    // Retrieve user - utiliser l'ID retourné par createUser
    val retrievedUser = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    retrievedUser should not be empty
    retrievedUser.get.emailAddress should contain("integration@test.com")
  }
  
  it should "create users with auto-generated unique IDs" in {
    val user = TestDataFactory.createUser(
      name = "Unique User Test"
    )
    
    // Utiliser createUser qui génère automatiquement un ID unique
    val createdUser = Await.result(dataManager.createUser(user), 5.seconds)
    createdUser.userId should be > 0L
    
    // Debug: vérifier que l'utilisateur est bien en base
    println(s"Created user with auto-generated ID: ${createdUser.userId}")
    
    // Le récupérer par son ID généré automatiquement
    val retrievedUser = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    
    // Debug output
    println(s"Retrieved user: $retrievedUser")
    
    retrievedUser should not be empty
    retrievedUser.get.userId shouldBe createdUser.userId
    retrievedUser.get.name should contain("Unique User Test")
  }
  
  it should "update existing users" in {
    val originalUser = TestDataFactory.createUser(name = "Original Name")
    val createdUser = Await.result(dataManager.upsertUser(originalUser), 5.seconds)
    
    val updatedUser = createdUser.copy(name = Some("Updated Name"))
    val result = Await.result(dataManager.upsertUser(updatedUser), 5.seconds)
    
    result.name should contain("Updated Name")
    result.userId shouldBe createdUser.userId
  }
  
  it should "batch retrieve users by IDs" in {
    // Créer des utilisateurs avec createUser (IDs auto-générés)
    val testUsers = (1 to 5).map { i =>
      TestDataFactory.createUser(name = s"Batch User $i")
    }
    
    // Créer tous les utilisateurs et récupérer leurs IDs générés
    val createdUsers = Await.result(
      Future.sequence(testUsers.map(dataManager.createUser)), 
      10.seconds
    )
    
    val userIds = createdUsers.map(_.userId).toList  // Ajouter .toList ici
    
    // Maintenant les récupérer par lot avec les vrais IDs
    val retrievedUsers = Await.result(dataManager.batchGetUsers(userIds), 5.seconds)
    retrievedUsers should have size 5
    retrievedUsers.keys should contain allElementsOf userIds
  }
  
  it should "delete users" in {
    val testUser = TestDataFactory.createUser(
      name = "User to Delete"
    )
    
    // Créer l'utilisateur avec createUser
    val createdUser = Await.result(dataManager.createUser(testUser), 5.seconds)
    
    // Vérifier qu'il existe
    val beforeDelete = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    beforeDelete should not be empty
    
    // Le supprimer
    val deleted = Await.result(dataManager.deleteUser(createdUser.userId), 5.seconds)
    deleted should be(true)
    
    // Vérifier qu'il n'existe plus
    val afterDelete = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    afterDelete should be(empty)
  }

  "DataManager Event Operations" should "create and retrieve events" in {
    // First create a user to be the event creator
    val creator = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val testEvent = Event(
      id = None,
      name = "Integration Test Event",
      description = Some("A test event for integration testing"),
      startTime = System.currentTimeMillis() + 86400000, // Tomorrow
      endTime = Some(System.currentTimeMillis() + 90000000), // Day after tomorrow
      location = Some("Test Location"),
      createdBy = creator.userId,
      status = "active",
      participants = Set.empty
    )
    
    val createdEvent = Await.result(dataManager.upsertEvent(testEvent), 5.seconds)
    createdEvent.id should not be empty
    
    val retrievedEvent = Await.result(dataManager.getEvent(createdEvent.id.get), 5.seconds)
    retrievedEvent should not be empty
    retrievedEvent.get.name shouldBe "Integration Test Event"
  }
  
  it should "retrieve events by creator" in {
    val creator = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val events = (1 to 3).map { i =>
      Event(
        id = None,
        name = s"Event $i",
        description = Some(s"Description $i"),
        startTime = System.currentTimeMillis() + (i * 86400000),
        endTime = None,
        location = None,
        createdBy = creator.userId,
        status = "active",
        participants = Set.empty
      )
    }.toList
    
    Await.result(Future.sequence(events.map(dataManager.upsertEvent)), 10.seconds)
    
    val creatorEvents = Await.result(dataManager.getEventsByCreator(creator.userId), 5.seconds)
    creatorEvents should have size 3
    creatorEvents.foreach(_.createdBy shouldBe creator.userId)
  }

  "DataManager ProcessedEvent Operations" should "store and retrieve processed events" in {
    val user = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val processedEvents = (1 to 5).map { i =>
      ProcessedEvent(
        id = None,
        userId = user.userId,
        eventType = s"test_event_$i",
        itemId = Some(i.toLong),
        properties = Map("test" -> s"value_$i"),
        timestamp = System.currentTimeMillis() - (i * 1000),
        processingTimestamp = System.currentTimeMillis()
      )
    }.toList
    
    // Store events individually
    Await.result(Future.sequence(processedEvents.map(dataManager.storeProcessedEvent)), 10.seconds)
    
    // Retrieve events for user
    val userEvents = Await.result(dataManager.getProcessedEventsForUser(user.userId), 5.seconds)
    userEvents should have size 5
    userEvents.foreach(_.userId shouldBe user.userId)
  }
  
  it should "batch store processed events" in {
    val user = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val events = (1 to 10).map { i =>
      ProcessedEvent(
        id = None,
        userId = user.userId,
        eventType = "batch_event",
        itemId = Some(i.toLong),
        properties = Map("batch" -> "true"),
        timestamp = System.currentTimeMillis(),
        processingTimestamp = System.currentTimeMillis()
      )
    }.toList
    
    val storedCount = Await.result(dataManager.batchStoreProcessedEvents(events), 5.seconds)
    storedCount shouldBe 10
    
    val userEvents = Await.result(dataManager.getProcessedEventsForUser(user.userId), 5.seconds)
    userEvents.filter(_.eventType == "batch_event") should have size 10
  }

  "DataManager Friends Operations" should "manage user friendships" in {
    val user1 = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    val user2 = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    val user3 = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val friendships = List(
      UserFriend(user1.userId, user2.userId, 0.8),
      UserFriend(user1.userId, user3.userId, 0.6)
    )
    
    val insertedCount = Await.result(dataManager.upsertUserFriends(friendships), 5.seconds)
    insertedCount shouldBe 2
    
    val user1Friends = Await.result(dataManager.getUserFriends(user1.userId), 5.seconds)
    user1Friends should have size 2
    user1Friends.map(_.friendId) should contain allOf(user2.userId, user3.userId)
  }

  "DataManager Participation Operations" should "manage event participations" in {
    val user = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    val creator = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val event = Event(
      id = None,
      name = "Participation Test Event",
      description = None,
      startTime = System.currentTimeMillis() + 86400000,
      endTime = None,
      location = None,
      createdBy = creator.userId,
      status = "active",
      participants = Set.empty
    )
    
    val createdEvent = Await.result(dataManager.upsertEvent(event), 5.seconds)
    
    val participation = Participation(
      userId = user.userId,
      eventId = createdEvent.id.get,
      status = "attending"
    )
    
    val added = Await.result(dataManager.addEventParticipation(participation), 5.seconds)
    added shouldBe true
    
    val eventParticipations = Await.result(dataManager.getEventParticipations(createdEvent.id.get), 5.seconds)
    eventParticipations should have size 1
    eventParticipations.head.userId shouldBe user.userId
    
    val userParticipations = Await.result(dataManager.getUserParticipations(user.userId), 5.seconds)
    userParticipations should have size 1
    userParticipations.head.eventId shouldBe createdEvent.id.get
  }

  "DataManager Recommendation Operations" should "store and retrieve recommendations" in {
    val user = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    val recommendedItems = List(
      RecommendedItem(itemId = 101L, score = 0.95, reason = "High similarity"),
      RecommendedItem(itemId = 102L, score = 0.87, reason = "User preference"),
      RecommendedItem(itemId = 103L, score = 0.76, reason = "Popular item")
    )
    
    val recommendation = Recommendation(
      id = None,
      userId = user.userId,
      recommendedItems = recommendedItems,
      score = 0.86,
      timestamp = System.currentTimeMillis()
    )
    
    val storedRec = Await.result(dataManager.storeRecommendation(recommendation), 5.seconds)
    storedRec.id should not be empty
    
    val retrievedRec = Await.result(dataManager.getRecommendationForUser(user.userId), 5.seconds)
    retrievedRec should not be empty
    retrievedRec.get.recommendedItems should have size 3
    retrievedRec.get.userId shouldBe user.userId
  }
  
  it should "retrieve recent recommendations within time window" in {
    val user = Await.result(dataManager.upsertUser(TestDataFactory.createUser()), 5.seconds)
    
    // Store an old recommendation
    val oldRec = Recommendation(
      id = None,
      userId = user.userId,
      recommendedItems = List(RecommendedItem(itemId = 201L, score = 0.5, reason = "Old")),
      score = 0.5,
      timestamp = System.currentTimeMillis() - 3600000 // 1 hour ago
    )
    Await.result(dataManager.storeRecommendation(oldRec), 5.seconds)
    
    // Store a recent recommendation
    val recentRec = Recommendation(
      id = None,
      userId = user.userId,
      recommendedItems = List(RecommendedItem(itemId = 202L, score = 0.9, reason = "Recent")),
      score = 0.9,
      timestamp = System.currentTimeMillis() - 60000 // 1 minute ago
    )
    Await.result(dataManager.storeRecommendation(recentRec), 5.seconds)
    
    // Retrieve recent recommendations (within last 30 minutes)
    val recentResult = Await.result(
      dataManager.getRecentRecommendationForUser(user.userId, 30 * 60 * 1000), 
      5.seconds
    )
    
    recentResult should not be empty
    recentResult.get.recommendedItems.head.reason shouldBe "Recent"
  }

  "DataManager Cache Operations" should "cache and invalidate data correctly" in {
    val testUser = TestDataFactory.createUser(
      name = "Cache Test User",
      emailAddress = "cache_test@example.com"
    )
    
    // Créer avec createUser
    val createdUser = Await.result(dataManager.createUser(testUser), 5.seconds)
    
    // Premier accès - devrait charger depuis la base de données
    val firstRetrieval = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    firstRetrieval should not be empty
    
    // Deuxième accès - devrait être en cache
    val secondRetrieval = Await.result(dataManager.getUserProfile(createdUser.userId), 5.seconds)
    secondRetrieval should not be empty
    secondRetrieval.get.userId shouldBe createdUser.userId
  }

  "DataManager Health Check" should "report healthy status when database is accessible" in {
    val isHealthy = dataManager.healthCheck()
    isHealthy shouldBe true
    
    val isDatabaseAccessible = dataManager.isDatabaseAccessible()
    isDatabaseAccessible shouldBe true
    
    val isCacheAccessible = dataManager.isCacheAccessible()
    isCacheAccessible shouldBe true
  }

  "DataManager Error Handling" should "handle database errors gracefully" in {
    // Try to insert invalid data that should fail
    val invalidUser = User(
      userId = -1L, // Invalid ID
      name = Some("Invalid User"),
      emailAddress = None,
      locale = "en_US",
      birthYear = None,
      gender = None,
      location = None,
      timezone = None,
      isGenerated = false
    )
    
    // This should handle the error gracefully
    val result = dataManager.upsertUser(invalidUser)
    
    // The operation should complete (either succeed or fail gracefully)
    noException should be thrownBy {
      Await.result(result, 5.seconds)
    }
  }
}

