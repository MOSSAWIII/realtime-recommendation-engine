package integration.recommendation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{Transformer, Recommender}
import repositories.Repository
import models.{UserEvent, ProcessedEvent}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class TransformerRecommenderIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var repository: Repository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  
  override def beforeAll(): Unit = {
    repository = new Repository()
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    
    // Initialiser la base de données
    Await.result(repository.initTables(), 5.seconds)
    
    // Démarrer le consumer de recommandations
    recommender.start()
  }
  
  override def afterAll(): Unit = {
    recommender.close()
  }
  
  "Transformer and Recommender" should "work together to process events and generate recommendations" in {
    // Arrange: Utilisateur de test
    val userId = 100L
    
    // Créer plusieurs événements pour le même item
    val events = (1 to 5).map { i =>
      UserEvent(
        userId = userId,
        eventType = if (i % 2 == 0) "click" else "view",
        itemId = Some(200L),
        timestamp = System.currentTimeMillis() - (i * 10000)
      )
    }.toList
    
    // Act: Traiter tous les événements
    val futures = events.map(transformer.processEvent)
    Await.result(Future.sequence(futures), 10.seconds)
    
    // Laisser le temps au Recommender de générer des recommandations
    Thread.sleep(5000)
    
    // Assert: Vérifier qu'une recommandation a été générée
    val recommendation = Await.result(repository.getRecommendationForUser(userId), 5.seconds)
    recommendation shouldBe defined
    recommendation.get.userId shouldBe userId
    recommendation.get.recommendedItems.find(_.itemId == 200L) shouldBe defined
  }
}