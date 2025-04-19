package unit.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.powermock.api.mockito.PowerMockito
import core.Transformer
import models._
import repositories.MockRepository
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rabbitmq.client.Channel

class TransformerSpec extends AnyFlatSpec with Matchers {

  "Transformer" should "process events correctly" in {
    // Utiliser MockRepository
    val mockRepo = new MockRepository()
    val mockChannel = PowerMockito.mock(classOf[Channel])
    val transformer = new Transformer(mockRepo, mockChannel)
    
    val event = UserEvent(
      userId = 42,
      eventType = "view",
      itemId = Some(101),
      properties = Map("sourceapp" -> "mobile", "  " -> null),
      timestamp = System.currentTimeMillis()
    )
    
    val resultF = transformer.processEvent(event)
    val result = Await.result(resultF, 5.seconds)
    
    result.userId shouldBe 42
    result.eventType shouldBe "view"
    result.itemId shouldBe Some(101)
    result.properties should contain key "sourceapp"
    result.properties("sourceapp") shouldBe "mobile"
    result.properties should not contain key("  ")
    result.properties should contain key "processed_at"
  }
}