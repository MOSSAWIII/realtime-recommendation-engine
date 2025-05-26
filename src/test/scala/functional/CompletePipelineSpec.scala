package e2e.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.nio.charset.StandardCharsets
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import models._
import application.core._
import common.utils.RabbitMQManager
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import common.utils.MockDataManager

class CompletePipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Composants du système
  var dataManager: MockDataManager = _
  var transformer: Transformer = _
  var recommender: Recommender = _
  
  // Journalisation de débogage
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Données de test
  private val testUserId = 42L
  private val testItemId = 24L
  private var publishChannel: Channel = _
  private var consumeChannel: Channel = _
  private var receivedRecommendation: Option[Recommendation] = None
  private val latch = new CountDownLatch(1)
  
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  
  override def beforeAll(): Unit = {
    // Initialiser le DataManager mock
    dataManager = new MockDataManager()
    
    // Configuration des données de test
    setupTestData()
    
    // Initialiser les composants du système
    transformer = new Transformer(dataManager)
    recommender = new Recommender(dataManager)
    
    // Démarrer les composants - le transformer gère maintenant directement la consommation
    recommender.start()
    transformer.start()
    
    // Initialiser les canaux RabbitMQ
    publishChannel = RabbitMQManager.createChannel("e2e-publisher")
    consumeChannel = RabbitMQManager.createChannel("e2e-consumer")
    
    // Purger les queues avant le test
    consumeChannel.queuePurge(RabbitMQManager.Queues.RECOMMENDATIONS)
    
    // Configurer le consumer pour capturer les recommandations
    setupRecommendationListener()
  }
  
  private def setupTestData(): Unit = {
    // Créer un événement de test pré-traité
    val testEvent = ProcessedEvent(
      id = None,
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Map("category" -> "test", "importance" -> "high"),
      timestamp = System.currentTimeMillis() - 5000
    )
    
    // Configurer l'utilisateur test
    val testUser = User(
      userId = testUserId,
      name = Some("Test User"),
      location = Some("Paris, France"),
      emailAddress = Some("test@example.com")
    )
    
    // Configurer l'événement test
    val testEventObj = Event(
      id = Some(testItemId),
      name = "Test Event",
      description = Some("A test event"),
      location = Some("Paris, France"),
      startTime = System.currentTimeMillis() + 86400000L,
      createdBy = 999L,
      status = "active"
    )
    
    // Configurer les données mock
    dataManager.setupMockData(
      users = List(testUser),
      events = List(testEventObj),
      processedEvents = List(testEvent)
    )
    
    logger.info(s"Test data configured: User ${testUser.userId}, Event ${testEventObj.id.get}, ProcessedEvent for user ${testEvent.userId}")
  }
  
  private def setupRecommendationListener(): Unit = {
    // Configurer un consumer pour écouter les recommandations
    val callback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Received message: $message")
        
        decode[Recommendation](message) match {
          case Right(recommendation) =>
            logger.info(s"Decoded recommendation for user: ${recommendation.userId}")
            if (recommendation.userId == testUserId) {
              logger.info(s"Received matching recommendation for test user $testUserId")
              receivedRecommendation = Some(recommendation)
              latch.countDown()
            } else {
              logger.debug(s"Ignoring recommendation for user: ${recommendation.userId}")
            }
          case Left(error) =>
            logger.error(s"Error decoding recommendation: $error")
        }
        consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Consumer cancelled: $consumerTag")
      }
    }
    
    // Démarrer la consommation avec ack manuel
    consumeChannel.basicConsume(
      RabbitMQManager.Queues.RECOMMENDATIONS,
      false, // ack manuel
      callback,
      cancelCallback
    )
  }
  
  override def afterAll(): Unit = {
    // Fermer proprement les ressources
    if (recommender != null) recommender.close()
    if (transformer != null) transformer.close()
    if (dataManager != null) dataManager.close()
    
    // Fermer les canaux RabbitMQ
    if (publishChannel != null && publishChannel.isOpen) publishChannel.close()
    if (consumeChannel != null && consumeChannel.isOpen) consumeChannel.close()
    
    // Fermer toutes les connexions RabbitMQ
    RabbitMQManager.closeAllChannels()
  }
  
  "Complete recommendation pipeline" should "process an event and generate recommendations" in {
    // Publier un événement de test
    val event = UserEvent(
      userId = testUserId,
      eventType = "view",
      itemId = Some(testItemId),
      properties = Some(Map("test" -> "true", "source" -> "e2e_test")),
      timestamp = System.currentTimeMillis()
    )
    
    // Envoyer l'événement au pipeline
    val message = event.asJson.noSpaces
    publishChannel.basicPublish(
      RabbitMQManager.Exchanges.USER_ACTIONS,  // Utiliser l'exchange correct
      "user.action.event",  // Routing key appropriée
      null,
      message.getBytes(StandardCharsets.UTF_8)
    )
    
    // Attendre la recommandation (avec timeout)
    val success = latch.await(15, java.util.concurrent.TimeUnit.SECONDS)
    
    // Vérifier que nous avons reçu une recommandation
    success shouldBe true
    receivedRecommendation.isDefined shouldBe true
    
    // Si la recommandation existe mais sans items, journaliser des informations utiles
    if (receivedRecommendation.exists(_.recommendedItems.isEmpty)) {
      logger.warn(s"Received empty recommendation: ${receivedRecommendation.get}")
      
      // Vérifier les événements traités pour cet utilisateur
      val events = Await.result(dataManager.getProcessedEventsForUser(testUserId), 5.seconds)
      logger.info(s"Found ${events.size} processed events for user $testUserId")
      events.foreach(e => logger.info(s"  Event: $e"))
    }
    
    // Assertion plus spécifique maintenant que nous avons un meilleur contrôle des données de test
    receivedRecommendation.isDefined shouldBe true
    receivedRecommendation.foreach { recommendation =>
      recommendation.userId shouldBe testUserId
      recommendation.recommendedItems.nonEmpty shouldBe true
    }
  }
}