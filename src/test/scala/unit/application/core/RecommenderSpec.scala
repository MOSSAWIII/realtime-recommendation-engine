package unit.application.core

import application.core.{Recommender, DataManager}
import common.base.UnitSpec
import common.mocks.{MockDataManager, TestRepository, TestConfigurationService, TestDatabaseProvider}
import domain.models._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

class RecommenderSpec extends UnitSpec with BeforeAndAfterEach {
  
  class RecommenderUnderTest(dataManager: DataManager)(implicit ec: ExecutionContext) 
    extends Recommender(dataManager)(ec) {
    
    // Exposer des méthodes ou états internes pour les tests si nécessaire
  }
  
  "Recommender" should "generate recommendations based on user history" in {
    // Arrange
    val mockRepo = new TestRepository()
    
    // Corriger la création du DataManager mock - utiliser 'ec' depuis BaseSpec
    val mockDataManager = new MockDataManager(
      new TestConfigurationService(),
      new TestDatabaseProvider()
    )(ec) // Utiliser l'ec hérité
    
    // Ajouter des données de test
    val userId = 123L
    val testEvents = List(
      ProcessedEvent(
        id = Some(1),
        userId = userId,
        eventType = "view",
        itemId = Some(456),
        properties = Map("category" -> "movie"),
        timestamp = System.currentTimeMillis() - 1000,
        processingTimestamp = System.currentTimeMillis()
      ),
      ProcessedEvent(
        id = Some(2),
        userId = userId,
        eventType = "like",
        itemId = Some(789),
        properties = Map("category" -> "movie"),
        timestamp = System.currentTimeMillis() - 2000,
        processingTimestamp = System.currentTimeMillis()
      )
    )
    
    // Créer des événements qui peuvent être recommandés
    val event1 = Event(
      id = Some(456),
      name = "Test Event 1",
      description = Some("Description 1"),
      startTime = System.currentTimeMillis() + 10000,
      endTime = Some(System.currentTimeMillis() + 20000),
      location = Some("Test Location 1"),
      createdBy = 1,
      status = "active"
    )
    
    val event2 = Event(
      id = Some(789),
      name = "Test Event 2",
      description = Some("Description 2"),
      startTime = System.currentTimeMillis() + 15000,
      endTime = Some(System.currentTimeMillis() + 25000),
      location = Some("Test Location 2"),
      createdBy = 2,
      status = "active"
    )
    
    // Configurer le mock repository
    mockRepo.mockEvents = event1 :: mockRepo.mockEvents
    mockRepo.mockEvents = event2 :: mockRepo.mockEvents
    testEvents.foreach(mockRepo.storeProcessedEvent)
    
    // Act
    import domain.models.RecommendationRequest
    val recommender = new RecommenderUnderTest(mockRepo)
    val futureRecommendation = recommender.generateRecommendations(RecommendationRequest(userId))
    val recommendation = Await.result(futureRecommendation, 5.seconds)
    
    // Assert
    recommendation.recommendedItems.size should be >= 1
    recommendation.userId shouldBe userId
  }
}