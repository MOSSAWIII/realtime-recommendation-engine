package integration.streaming

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import core.{StreamingConsumer, Transformer}
import repositories.Repository
import config.RabbitMQManager
import models.{UserEvent, ProcessedEvent}
import io.circe.syntax._
import io.circe.generic.auto._
import java.util.UUID
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext  // Add this import
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import java.sql.Connection

class StreamingTransformerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  // Queues de test uniques pour éviter les conflits
  val testQueueName = s"test_events_${UUID.randomUUID().toString}"
  
  // Composants à tester
  var repository: TestRepository = _
  var transformer: Transformer = _
  var consumer: StreamingConsumer = _
  
  override def beforeAll(): Unit = {
    // Initialiser les composants
    repository = new TestRepository()
    transformer = new Transformer(repository)
    
    // Configurer le canal RabbitMQ
    val channel = RabbitMQManager.createChannel("test-publisher")
    channel.queueDeclare(testQueueName, false, false, true, null)
    
    // Créer StreamingConsumer qui utilise notre transformer
    consumer = new TestStreamingConsumer(transformer, testQueueName)
    consumer.start()
    
    // Attendre que tout soit initialisé
    Thread.sleep(1000)
  }
  
  override def afterAll(): Unit = {
    // Nettoyer
    consumer.close()
    transformer.close()
    
    // Supprimer la queue de test
    val channel = RabbitMQManager.createChannel("test-cleanup")
    channel.queueDelete(testQueueName)
    RabbitMQManager.closeChannel("test-cleanup")
    RabbitMQManager.closeChannel("test-publisher")
  }
  
  "StreamingConsumer and Transformer integration" should "process events end-to-end" in {
    // Arrange
    val eventPromise = Promise[ProcessedEvent]()
    repository.nextEventPromise = eventPromise
    
    val testEvent = UserEvent(
      userId = 999,
      eventType = "test_view",
      itemId = Some(888),
      properties = Map("source" -> "integration_test"),
      timestamp = System.currentTimeMillis()
    )
    
    // Act: Publier l'événement dans la queue
    val channel = RabbitMQManager.createChannel("test-publisher")
    channel.basicPublish(
      "", 
      testQueueName, 
      null, 
      testEvent.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
    )
    
    // Attendre que l'événement soit traité (avec timeout)
    val result = Await.result(eventPromise.future, 10.seconds)
    
    // Assert
    result.userId shouldBe 999
    result.eventType shouldBe "test_view"
    result.itemId shouldBe Some(888)
    result.properties should contain key "source"
    result.properties("source") shouldBe "integration_test"
    result.properties should contain key "processed_at"
  }
}

// Classes d'aide pour le test
class TestRepository extends Repository()(global) {
  var nextEventPromise: Promise[ProcessedEvent] = _
  
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    // Compléter la promesse pour signaler que l'événement a été traité
    if (nextEventPromise != null && !nextEventPromise.isCompleted) {
      nextEventPromise.success(event)
    }
    
    Future.successful(event.copy(id = Some(999L)))
  }
}

class TestStreamingConsumer(transformer: Transformer, queueName: String)(implicit ec: ExecutionContext) 
  extends StreamingConsumer(transformer)(ec) {
  
  // Define a new method instead of overriding
  protected def setupQueue(): Unit = {
    val channel = RabbitMQManager.createChannel("test-consumer")
    channel.queueDeclare(queueName, false, false, true, null)
    
    // Rest of your setup code...
  }
}

class TestableRepository(mockConnection: Connection)(override implicit val ec: ExecutionContext) extends Repository()(ec) {
  
  // Change from override to a new method
  protected def withConnection[T](operation: Connection => T): T = {
    operation(mockConnection)
  }
}