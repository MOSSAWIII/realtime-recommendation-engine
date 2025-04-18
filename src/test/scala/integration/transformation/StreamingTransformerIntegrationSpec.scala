package integration.transformation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{StreamingConsumer, Transformer}
import repositories.Repository
import config.{RabbitMQManager, DatabaseConfig}
import models.UserEvent
import io.circe.syntax._
import io.circe.generic.auto._
import com.rabbitmq.client.AMQP
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

class StreamingTransformerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  // Configuration de test
  val testQueueName = s"test_events_${UUID.randomUUID().toString}"
  var repository: Repository = _
  var transformer: Transformer = _
  var consumer: StreamingConsumer = _
  
  override def beforeAll(): Unit = {
    // Initialiser les composants réels
    repository = new Repository()
    transformer = new Transformer(repository)
    
    // Créer une queue de test
    val channel = RabbitMQManager.createChannel("test")
    channel.queueDeclare(testQueueName, false, false, true, null)
    
    // Initialiser la base de données de test
    Await.result(repository.initTables(), 5.seconds)
  }
  
  override def afterAll(): Unit = {
    // Nettoyer les ressources
    val channel = RabbitMQManager.createChannel("cleanup")
    channel.queueDelete(testQueueName)
    RabbitMQManager.closeAllChannels()
  }
  
  "StreamingConsumer and Transformer" should "process events and store them in the database" in {
    // Arrange: Préparer un événement test
    val testEvent = UserEvent(userId = 99, eventType = "test_view", itemId = Some(999))
    val eventJson = testEvent.asJson.noSpaces
    
    // Act: Publier l'événement dans la queue
    val channel = RabbitMQManager.createChannel("publisher")
    channel.basicPublish("", testQueueName, new AMQP.BasicProperties.Builder().build(), eventJson.getBytes)
    
    // Wait for processing
    Thread.sleep(2000)
    
    // Assert: Vérifier que l'événement a été stocké
    val storedEvents = Await.result(repository.getProcessedEventsForUser(99), 5.seconds)
    storedEvents.size should be >= 1
    storedEvents.exists(e => e.userId == 99 && e.eventType == "test_view" && e.itemId.contains(999)) shouldBe true
  }
}