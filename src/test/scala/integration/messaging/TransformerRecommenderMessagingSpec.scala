package integration.messaging

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{Transformer, Recommender}
import repositories.Repository
import config.RabbitMQManager
import models.{UserEvent, ProcessedEvent, Recommendation, RecommendationRequest}
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.DeliverCallback

class TransformerRecommenderMessagingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var repository: TestRepository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  val testUserId = 777L
  val testItemId = 555L
  val recommendationLatch = new CountDownLatch(1)
  var capturedRecommendation: Option[Recommendation] = None
  
  override def beforeAll(): Unit = {
    // Initialiser les composants
    repository = new TestRepository()
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    
    // S'assurer que les queues existent
    val channel = RabbitMQManager.createChannel("test-setup")
    channel.queueDeclare(RabbitMQManager.Queues.INTERNAL_RECOMMENDATION_REQUESTS, true, false, false, null)
    channel.queueDeclare(RabbitMQManager.Queues.RECOMMENDATIONS, true, false, false, null)
    
    // Configurer un consumer pour capturer les recommandations publiées
    val recommendationChannel = RabbitMQManager.createChannel("test-recommendation-consumer")
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Recommendation](message) match {
          case Right(recommendation) => 
            capturedRecommendation = Some(recommendation)
            recommendationLatch.countDown()
          case Left(error) => println(s"Erreur de décodage: $error")
        }
        recommendationChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    recommendationChannel.basicConsume(RabbitMQManager.Queues.RECOMMENDATIONS, false, deliverCallback, _ => ())
    
    // Démarrer le recommender
    recommender.start()
    
    // Attendre l'initialisation
    Thread.sleep(1000)
  }
  
  override def afterAll(): Unit = {
    recommender.close()
    transformer.close()
    RabbitMQManager.closeAllChannels()
  }
  
  "Transformer and Recommender" should "communicate via RabbitMQ" in {
    // Arrange
    repository.mockEventsForUser = List(
      ProcessedEvent(Some(1), testUserId, "view", Some(testItemId), Map(), System.currentTimeMillis() - 1000),
      ProcessedEvent(Some(2), testUserId, "click", Some(testItemId), Map(), System.currentTimeMillis() - 2000)
    )
    
    // Act: Simuler un événement utilisateur
    val event = UserEvent(
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("source" -> "messaging_test")),
      timestamp = System.currentTimeMillis()
    )
    
    // Traiter l'événement, ce qui devrait déclencher une demande de recommandation
    Await.result(transformer.processEvent(event), 5.seconds)
    
    // Attendre la génération de recommandation (avec timeout)
    val success = recommendationLatch.await(15, SECONDS)
    
    // Assert
    success shouldBe true
    capturedRecommendation shouldBe defined
    capturedRecommendation.foreach { recommendation =>
      recommendation.userId shouldBe testUserId
      recommendation.recommendedItems.exists(_.itemId == testItemId) shouldBe true
    }
  }
}

// Repository de test
class TestRepository extends Repository()(global) {
  var mockEventsForUser: List[ProcessedEvent] = List.empty
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    Future.successful(event.copy(id = Some(1L)))
  }
  
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    Future.successful(mockEventsForUser)
  }
  
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    Future.successful(recommendation.copy(id = Some(1L)))
  }
}