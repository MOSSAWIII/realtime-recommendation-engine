package unit.application.core

import application.core.{Transformer, Recommender, DataManager}
import common.base.UnitSpec
import common.mocks.MockDataManager
import common.fixtures.TestDataFactory  // Ajout de l'import correct
import domain.models.{UserEvent, Recommendation, RecommendationRequest, RecommendedItem}
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.concurrent.ScalaFutures

class PipelineUnitSpec extends UnitSpec with ScalaFutures {
  
  "The recommendation pipeline" should "flow events from transformer to recommender" in {
    // Arrange
    val mockDataManager = spy(new MockDataManager())
    val transformer = new Transformer(mockDataManager)
    val recommender = new Recommender(mockDataManager)
    
    // Créer une variable mutable pour capturer la recommandation
    val recommenderReceived = new AtomicReference[Recommendation]()
    
    // Utiliser Mockito pour intercepter l'appel à storeRecommendation
    doAnswer { invocation =>
      val rec = invocation.getArgument[Recommendation](0)
      recommenderReceived.set(rec)
      Future.successful(rec)
    }.when(mockDataManager).storeRecommendation(any[Recommendation])
    
    // Act
    // Créer et traiter un événement utilisateur
    val userEvent = TestDataFactory.createUserEvent(userId = 123L)
    val processedEventFuture = transformer.processEvent(userEvent)
    
    // Utiliser Await.result au lieu de futureValue
    val processedEvent = Await.result(processedEventFuture, 5.seconds)
    
    // Attendre que la recommandation soit générée
    Thread.sleep(500) // Attendre un peu pour le traitement asynchrone
    
    // Assert
    // Vérifier la recommandation
    val recommendationResult = recommenderReceived.get()
    recommendationResult should not be null
    
    // Pour un test plus spécifique
    if (recommendationResult != null) {
      recommendationResult.userId shouldBe 123L
    }
  }
}

