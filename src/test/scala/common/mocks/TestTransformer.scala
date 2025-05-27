package common.mocks

import application.core.{DataManager, Transformer}
import domain.models._
import scala.concurrent.{Promise, Future, ExecutionContext}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.scalatest.matchers.should.Matchers
import com.rabbitmq.client.Channel

class TestTransformer(
  dataManager: DataManager,
  queueName: String = "test_queue",
  processPromise: Promise[ProcessedEvent] = Promise[ProcessedEvent](),
  channelMock: Option[Channel] = None
)(implicit ec: ExecutionContext) extends Transformer(dataManager) with Matchers {
  
  private val latch = new CountDownLatch(1)
  
  override def processEvent(event: UserEvent): Future[ProcessedEvent] = {
    val processed = ProcessedEvent(
      id = None,
      userId = event.userId,
      eventType = event.eventType,
      itemId = event.itemId,
      properties = event.properties.getOrElse(Map.empty),
      timestamp = event.timestamp,
      processingTimestamp = System.currentTimeMillis()
    )
    
    processPromise.success(processed)
    latch.countDown()
    Future.successful(processed)
  }
  
  def waitForProcessing(timeoutSeconds: Int = 5): Boolean = {
    latch.await(timeoutSeconds, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    // Code pour arrêter le transformer
    // Si aucune logique spécifique n'est requise, laissez simplement vide
  }
}

object TestTransformer extends Matchers {
  def test(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val mockDataManager = new MockDataManager()
    val processPromise = Promise[ProcessedEvent]()
    val latch = new CountDownLatch(1)
    val testQueueName = "test_transformer_queue"
    
    val transformer = new TestTransformer(mockDataManager, testQueueName, processPromise)
    transformer.start()
    
    // Assert results
    latch.await(5, TimeUnit.SECONDS) should be (true)
    transformer.stop()
  }
}

