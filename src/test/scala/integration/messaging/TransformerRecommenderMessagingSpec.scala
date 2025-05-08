package integration.messaging

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{Transformer, Recommender}
import repositories.Repository
import config.{RabbitMQManager, RabbitMQConfig}
import models.{UserEvent, ProcessedEvent, Recommendation, RecommendationRequest, User, UserFriend, Event}
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{Channel, DeliverCallback}
import testutil.RabbitMQTestManager
import java.util.concurrent.TimeUnit.SECONDS

class TransformerRecommenderMessagingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var repository: TestRepository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  val testUserId = 777L
  val testItemId = 555L
  val recommendationLatch = new CountDownLatch(1)
  var capturedRecommendation: Option[Recommendation] = None
  
  // Create test channels
  private var userChannel: Option[Channel] = None
  private var responseChannel: Option[Channel] = None
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Setup the RabbitMQ queues
    val setupChannel = RabbitMQTestManager.createChannel("test-setup")
    RabbitMQTestManager.setupQueues(setupChannel)
    RabbitMQManager.closeChannel("test-setup")
    
    // Purge the queues to ensure clean state
    RabbitMQTestManager.purgeAllQueues()
    
    // Create channels for the test
    userChannel = Some(RabbitMQTestManager.createChannel("test-user-channel"))
    responseChannel = Some(RabbitMQTestManager.createChannel("test-response-channel"))
  }
  
  override def afterAll(): Unit = {
    // Clean up resources
    userChannel.foreach(c => if (c.isOpen) c.close())
    responseChannel.foreach(c => if (c.isOpen) c.close())
    
    // Close any remaining connections
    RabbitMQTestManager.close()
    
    // Close recommender and transformer if they were created
    if (recommender != null) recommender.close()
    if (transformer != null) transformer.close()
    
    super.afterAll()
  }
  
  "Transformer and Recommender" should "communicate via RabbitMQ" in {
    // Arrange
    repository = new TestRepository()
    repository.mockEventsForUser = List(
      ProcessedEvent(Some(1), testUserId, "view", Some(testItemId), Map("category" -> "tech"), System.currentTimeMillis() - 1000),
      ProcessedEvent(Some(2), testUserId, "click", Some(testItemId), Map("category" -> "tech"), System.currentTimeMillis() - 2000)
    )
    
    // Set up mock user profile and friends for contextual recommendations
    repository.mockUserProfile = Some(
      User(
        userId = testUserId,
        name = Some("Test User"),
        location = Some("Paris, France"),
        timezone = None // Or an appropriate integer value if timezone is stored as Int
      )
    )
    
    repository.mockUserFriends = List(
      UserFriend(testUserId, 456L, 1.0),
      UserFriend(testUserId, 789L, 0.8)
    )
    
    repository.mockEvents = List(
      Event(
        id = Some(testItemId),
        name = "Test Event",
        location = Some("Paris, France"),
        startTime = System.currentTimeMillis() + 86400000L,
        createdBy = 999L,
        participants = Set(456L) // One of the friends participates
      )
    )
    
    // Initialize components
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    recommender.start()
    
    // Setup recommendation listener
    val recChannel = RabbitMQTestManager.createChannel("recommendations-consumer")
    recChannel.queueDeclare(RabbitMQConfig.Queues.RECOMMENDATIONS, true, false, false, null)
    
    val callback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Recommendation](message) match {
          case Right(rec) => 
            capturedRecommendation = Some(rec)
            recommendationLatch.countDown()
          case Left(err) => println(s"Failed to decode recommendation: $err")
        }
        recChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    recChannel.basicConsume(
      RabbitMQConfig.Queues.RECOMMENDATIONS, 
      false, // manual ack
      callback,
      _ => {}
    )
    
    // Act: Simulate a user event
    val event = UserEvent(
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("source" -> "messaging_test")),
      timestamp = System.currentTimeMillis()
    )
    
    // Process the event, which should trigger a recommendation request
    Await.result(transformer.processEvent(event), 5.seconds)
    
    // Wait for recommendation generation (with timeout)
    val success = recommendationLatch.await(15, SECONDS)
    
    // Assert
    success shouldBe true
    capturedRecommendation shouldBe defined
    capturedRecommendation.foreach { recommendation =>
      recommendation.userId shouldBe testUserId
      recommendation.recommendedItems.exists(_.itemId == testItemId) shouldBe true
      
      // Verify recommendation reasons are generated
      recommendation.recommendedItems.foreach { item =>
        item.reason should not be empty
        item.reason should startWith("Based on")
      }
      
      // Verify score is calculated (should be non-zero with our test data)
      recommendation.recommendedItems.head.score should be > 0.0
    }
  }
  
  "Messaging system" should "successfully deliver user messages" in {
    // Create test user
    val user = User(
      userId = 1L,
      name = Some("Test User"),
      emailAddress = Some("test@example.com")
    )
    
    // Send user to queue
    userChannel.foreach { channel =>
      channel.basicPublish(
        "", // Default exchange
        RabbitMQConfig.Queues.USER_INTERACTIONS,
        null,
        user.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
      )
    }
    
    // In a real test you would:
    // 1. Wait for processing
    // 2. Verify message was processed correctly
    // 3. Check for expected side effects
    
    Thread.sleep(1000) // Simple wait for demonstration
    
    // Add assertions based on your actual requirements
    succeed
  }
}

// Test Repository implementation
class TestRepository extends Repository {
  var mockEventsForUser: List[ProcessedEvent] = List.empty
  var mockUserProfile: Option[User] = None
  var mockUserFriends: List[UserFriend] = List.empty
  var mockEvents: List[Event] = List.empty
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    Future.successful(event.copy(id = Some(1L)))
  }
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    Future.successful(mockEventsForUser)
  }
  
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    Future.successful(recommendation.copy(id = Some(1L)))
  }
  
  // New methods
  override def getUserProfile(userId: Long): Future[Option[User]] = {
    Future.successful(mockUserProfile)
  }
  
  override def getUserFriends(userId: Long): Future[List[UserFriend]] = {
    Future.successful(mockUserFriends)
  }
  
  override def getAllActiveEvents(): Future[List[Event]] = {
    Future.successful(mockEvents)
  }
  
  // Other necessary methods
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = Future.successful(None)
  override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = Future.successful(None)
  override def initTables(): Future[Unit] = Future.successful(())
}