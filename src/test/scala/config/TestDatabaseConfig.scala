package config

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import java.sql.{Connection, PreparedStatement, ResultSet}

object TestDatabaseConfig {
  // For unit tests, provide a mock connection
  def getMockConnection(): Connection = {
    val mockConnection = PowerMockito.mock(classOf[Connection])
    val mockStatement = PowerMockito.mock(classOf[PreparedStatement])
    val mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    mockConnection
  }
}

