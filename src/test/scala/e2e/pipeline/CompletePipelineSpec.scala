package e2e.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{Main, StreamingConsumer, Transformer, Recommender}
import repositories.Repository
import config.{DatabaseConfig, RabbitMQManager}
import models.{UserEvent, Recommendation}
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import com.rabbitmq.client.{Channel, DeliverCallback}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.charset.StandardCharsets
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class CompletePipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var publishChannel: Channel = _
  var consumeChannel: Channel = _
  var receivedRecommendation: Option[Recommendation] = None
  var latch: CountDownLatch = _
  val testUserId: Long = 42
  val testItemId: Long = 24
  
  // Composants du système
  var repository: Repository = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  var consumer: StreamingConsumer = _
  
  override def beforeAll(): Unit = {
    // Initialiser la base de données de test
    repository = new Repository()
    Await.result(repository.initTables(), 10.seconds)
    
    // Initialiser les composants du système
    transformer = new Transformer(repository)
    recommender = new Recommender(repository)
    consumer = new StreamingConsumer(transformer)
    
    // Démarrer les composants
    recommender.start()
    consumer.start()
    
    // Initialiser les canaux RabbitMQ
    publishChannel = RabbitMQManager.createChannel("e2e-publisher")
    consumeChannel = RabbitMQManager.createChannel("e2e-consumer")
    
    // Configurer le consumer pour capturer les recommandations
    val callback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Recommendation](message) match {
          case Right(recommendation) =>
            if (recommendation.userId == testUserId) {
              receivedRecommendation = Some(recommendation)
              latch.countDown()
            }
          case Left(error) =>
            println(s"Error decoding recommendation: $error")
        }
        consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    consumeChannel.basicConsume(RabbitMQManager.Queues.RECOMMENDATIONS, false, callback, _ => ())
    
    // Attendre que tout soit initialisé
    Thread.sleep(2000)
  }
  
  override def afterAll(): Unit = {
    // Arrêter les composants
    consumer.close()
    recommender.close()
    transformer.close()
    
    // Nettoyer RabbitMQ
    RabbitMQManager.closeAllChannels()
  }
  
  "Complete recommendation pipeline" should "process an event and generate recommendations" in {
    // Arrange
    latch = new CountDownLatch(1)
    receivedRecommendation = None
    
    // Créer un événement test
    val event = UserEvent(
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("test" -> "true", "source" -> "e2e_test")),
      timestamp = System.currentTimeMillis()
    )
    
    // Act: Publier l'événement dans la queue d'entrée
    publishChannel.basicPublish(
      "",
      RabbitMQManager.Queues.USER_EVENTS,
      null,
      event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
    )
    
    // Attendre la recommandation (avec timeout de 30 secondes)
    val success = latch.await(30, TimeUnit.SECONDS)
    
    // Assert
    success shouldBe true
    receivedRecommendation shouldBe defined
    val recommendation = receivedRecommendation.get
    recommendation.userId shouldBe testUserId
    
    // Vérifier que l'item est recommandé (devrait être le cas puisque c'est le seul vu)
    recommendation.recommendedItems.exists(_.itemId == testItemId) shouldBe true
  }
}