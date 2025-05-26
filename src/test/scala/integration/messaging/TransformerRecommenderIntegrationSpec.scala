package integration.recommendation

import common.base.IntegrationTestBase
import application.core.{Transformer, Recommender}
import domain.models.{UserEvent, ProcessedEvent, Recommendation, RecommendedItem}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import common.mocks.TestRepository
import org.slf4j.LoggerFactory

class TransformerRecommenderIntegrationSpec extends IntegrationTestBase {
  
  private val logger = LoggerFactory.getLogger(getClass)
  var repository: TestRepository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Use a TestRepository instead of the real Repository
    repository = new TestRepository()
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    
    // Initialize the repository
    Await.result(repository.initTables(), 5.seconds)
    
    // Start the recommender service
    recommender.start()
  }
  
  override def afterAll(): Unit = {
    if (recommender != null) {
      recommender.close()
    }
    super.afterAll()
  }
  
  "Transformer and Recommender" should "work together to process events and generate recommendations" in {
    // Arrange: Test user
    val userId = 100L
    
    // Create multiple events for the same item
    val events = (1 to 5).map { i =>
      UserEvent(
        userId = userId,
        eventType = if (i % 2 == 0) "click" else "view",
        itemId = Some(200L),
        timestamp = System.currentTimeMillis() - (i * 10000)
      )
    }.toList
    
    // Act: Process all events
    val futures = events.map(transformer.processEvent)
    Await.result(Future.sequence(futures), 10.seconds)
    
    // Allow time for the Recommender to generate recommendations
    Thread.sleep(5000)
    
    // Assert: Verify a recommendation was generated
    val recommendation = Await.result(repository.getRecommendationForUser(userId), 5.seconds)
    
    // Log recommendation details for debugging
    logger.info(s"Retrieved recommendation: $recommendation")
    logger.info(s"Available recommendations: ${repository.mockRecommendations}")
    
    recommendation shouldBe defined
    recommendation.get.userId shouldBe userId
    recommendation.get.recommendedItems.find(_.itemId == 200L) shouldBe defined
  }
}