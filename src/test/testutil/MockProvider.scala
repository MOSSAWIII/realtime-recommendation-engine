package utils.mocks

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}
import com.rabbitmq.client.Channel

object MockProvider {
  // Database mocking utilities
  val DATABASE_CONFIG_CLASS = "config.DatabaseConfig$"
  
  def mockDatabaseConfig(): Connection = {
    val mockConnection = mock(classOf[Connection])
    PowerMockito.mockStatic(classOf[config.DatabaseConfig])
    when(config.DatabaseConfig.getConnection).thenReturn(mockConnection)
    mockConnection
  }
  
  def createMockDbConnection(): Connection = {
    val conn = mock(classOf[Connection])
    val stmt = mock(classOf[PreparedStatement])
    val rs = mock(classOf[ResultSet])
    
    when(conn.prepareStatement(any())).thenReturn(stmt)
    when(stmt.executeQuery()).thenReturn(rs)
    
    conn
  }

  // RabbitMQ mocking utilities  
  def mockRabbitMQConfig(): Channel = {
    val mockChannel = PowerMockito.mock(classOf[Channel])
    PowerMockito.mockStatic(classOf[config.RabbitMQConfig])
    when(config.RabbitMQConfig.createChannel()).thenReturn(mockChannel)
    mockChannel
  }
}

