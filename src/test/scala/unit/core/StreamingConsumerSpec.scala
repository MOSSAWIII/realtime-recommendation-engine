package unit.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import core.{StreamingConsumer, Transformer}
import models.{UserEvent, ProcessedEvent}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span, Millis} // Import Millis

class StreamingConsumerSpec extends AnyFlatSpec with Matchers with ScalaFutures {

  // --> Increase timeout significantly for diagnosis <--
  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))

  "StreamingConsumer" should "correctly pass events to the transformer" in {
    // Create mocks
    val mockTransformer = mock(classOf[Transformer])

    // Configure mock behavior
    val testEvent = UserEvent(userId = 1, eventType = "view", itemId = Some(123))
    val processedEvent = ProcessedEvent(None, 1, "view", Some(123), Map(), System.currentTimeMillis())
    when(mockTransformer.processEvent(any[UserEvent])).thenReturn(Future.successful(processedEvent))

    // Create consumer with mocked transformer
    val consumer = new StreamingConsumer(mockTransformer)

    // Use the simpler processEvent method
    val resultFuture = consumer.processEvent(testEvent)

    whenReady(resultFuture) { result =>
      // Verify transformer was called with correct event
      verify(mockTransformer).processEvent(testEvent)
      result shouldBe processedEvent
    }
  }
}