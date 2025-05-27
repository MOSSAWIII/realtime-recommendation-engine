package common.base

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, times, verify}
import org.mockito.ArgumentMatchers.{any, anyString, eq => argEq}

trait RabbitMQTestSupport {
  // Préparation automatique des mocks RabbitMQ
  lazy val channelMock: Channel = {
    val mockChannel = Mockito.mock(classOf[Channel])
    doNothing().when(mockChannel).basicPublish(anyString(), anyString(), any(), any())
    mockChannel
  }
  
  lazy val connectionMock: Connection = Mockito.mock(classOf[Connection])
  
  // Helper methods pour vérifier les publications
  def verifyMessagePublished(exchange: String, routingKey: String): Unit = {
    verify(channelMock, times(1)).basicPublish(
      argEq(exchange),
      argEq(routingKey),
      any(),
      any()
    )
  }
}

