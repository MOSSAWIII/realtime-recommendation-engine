package common.mocks

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, anyString}
import com.rabbitmq.client.{Channel, Connection, ConfirmCallback, AMQP}
import infrastructure.messaging.rabbitmq.RabbitMQManager
import org.scalatest.BeforeAndAfterEach

/**
 * Support pour les tests utilisant RabbitMQ avec des mocks
 */
trait RabbitMQTestSupport extends BeforeAndAfterEach { self: org.scalatest.Suite =>
  
  // Mocks partagés
  protected var channelMock: Channel = _
  protected var connectionMock: Connection = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    
    // Initialiser les mocks
    channelMock = mock(classOf[Channel])
    connectionMock = mock(classOf[Connection])
    
    // Configuration standard
    when(connectionMock.createChannel()).thenReturn(channelMock)
    when(RabbitMQManager.createChannel(anyString())).thenReturn(channelMock)
  }
  
  /**
   * Configure un mock pour simuler une publication avec accusé de réception
   */
  protected def setupPublishConfirm(deliveryTag: Long = 1L, multiple: Boolean = false): Unit = {
    doAnswer(invocation => {
      val callback = invocation.getArgument[ConfirmCallback](0)
      callback.handle(deliveryTag, multiple)
      null
    }).when(channelMock).addConfirmListener(any(), any())
  }
}

