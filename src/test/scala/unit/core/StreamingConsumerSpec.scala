package unit.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import core.{StreamingConsumer, Transformer}
import models.{UserEvent, ProcessedEvent}
import repositories.MockRepository
import com.rabbitmq.client.Channel
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class StreamingConsumerSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  
  "StreamingConsumer" should "correctly pass events to the transformer" in {
    // Create mocks
    val mockChannel = mock[Channel]
    val mockRepository = new MockRepository()
    val mockTransformer = mock[Transformer]
    
    // Configure mock behavior
    val testEvent = UserEvent(userId = 1, eventType = "view", itemId = Some(123))
    val processedEvent = ProcessedEvent(None, 1, "view", Some(123), Map(), System.currentTimeMillis())
    when(mockTransformer.processEvent(any[UserEvent])).thenReturn(Future.successful(processedEvent))
    
    // Create consumer with mocked transformer
    val consumer = new StreamingConsumer(mockTransformer)
    
    // Test handleMessage method directly
    val result = consumer.handleMessage(testEvent)
    
    // Verify
    verify(mockTransformer).processEvent(testEvent)
    result shouldBe defined
  }
}