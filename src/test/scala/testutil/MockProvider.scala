package testutil

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}
import com.rabbitmq.client.Channel

object MockProvider {
  def mockDatabaseConfig(): Connection = {
    val mockConnection = PowerMockito.mock(classOf[Connection])
    val mockStatement = PowerMockito.mock(classOf[PreparedStatement])
    val mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    mockConnection
  }
  
  def mockRabbitMQConfig(): Channel = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    PowerMockito.mockStatic(classOf[config.RabbitMQConfig.type])
    when(config.RabbitMQConfig.createChannel()).thenReturn(mockChannel)
    mockChannel
  }
}

