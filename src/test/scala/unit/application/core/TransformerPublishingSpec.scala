package unit.application.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.verification.VerificationWithTimeout
import org.mockito.verification.VerificationMode
import scala.concurrent.ExecutionContext
import infrastructure.messaging.rabbitmq.RabbitMQManager
import application.core.{Transformer, DataManager}
import com.rabbitmq.client.{Channel, ConfirmCallback}
import common.base.UnitSpec

class TransformerPublishingSpec extends UnitSpec {
  
  // Mocks pour RabbitMQ
  private var channelMock: Channel = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    
    // Initialiser les mocks
    channelMock = mock(classOf[Channel])
    reset(channelMock)
    
    // Configurer le comportement du mock RabbitMQManager
    when(RabbitMQManager.createChannel(anyString())).thenReturn(channelMock)
  }
  
  "Transformer" should "publish messages to RabbitMQ" in {
    // Setup mock pour la publication
    doAnswer(invocation => {
      // Logique de simulation de publication réussie
      val callback = invocation.getArgument[ConfirmCallback](0)
      callback.handle(1L, false)
      null
    }).when(channelMock).basicPublish(anyString(), anyString(), any(), any())
    
    // Créer et configurer le transformer avec l'ExecutionContext explicite
    val mockDataManager = createMockDataManager()
    val transformer = new Transformer(mockDataManager)(ec)
    
    // Utiliser timeout() de Mockito au lieu de after()
    verify(channelMock, timeout(5000).times(1)).basicPublish(
      anyString(),
      anyString(),
      any(),
      any()
    )
  }
  
  // Helper method pour le timeout Mockito
  private def timeout(millis: Long): VerificationWithTimeout = {
    org.mockito.Mockito.timeout(millis)
  }
  
  // Helper pour créer un DataManager mocké
  private def createMockDataManager(): DataManager = {
    mock(classOf[DataManager])
  }
}

