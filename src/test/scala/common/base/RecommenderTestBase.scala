package common.base

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.ExecutionContext
import common.mocks.TestRepository
import application.core.Recommender
import scala.concurrent.Future

abstract class RecommenderTestBase extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  // Utiliser un ExecutionContext spécifique pour les tests de recommandation
  implicit override def executionContext: ExecutionContext = 
    ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(4))
  
  // Créer un repository et un recommender partagés
  lazy val repository = new TestRepository()
  lazy val recommender = new Recommender(repository)
  
  // Helper pour tester les recommandations avec moins de code répétitif
  def testRecommendation(userId: Long, expectedItems: Seq[Long]): Future[Any] = {
    import domain.models.RecommendationRequest
    
    recommender.generateRecommendations(RecommendationRequest(userId)).map { result =>
      result.recommendedItems.map(_.itemId) should contain allElementsOf expectedItems
    }
  }
}

